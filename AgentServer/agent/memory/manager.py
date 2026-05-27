"""Short-term and long-term memory persistence."""

from __future__ import annotations

import uuid
import json
from datetime import datetime, timedelta
from typing import Any, Optional

from core.managers import llm_manager, milvus_manager, mongo_manager

from .compressor import ContextCompressor
from ..state import MemoryItem


class MemoryManager:
    """Loads, writes, and summarizes agent memory.

    MongoDB is the source of truth. Milvus is used opportunistically for semantic
    lookup when it is initialized and available.
    """

    def __init__(self) -> None:
        self.compressor = ContextCompressor()

    async def get_thread_history(self, thread_id: str, user_id: str, limit: int = 30) -> list[dict]:
        docs = await mongo_manager.find_many(
            "agent_messages",
            {"thread_id": thread_id, "user_id": user_id},
            projection={"_id": 0},
            sort=[("created_at", -1)],
            limit=limit,
        )
        return list(reversed(docs))

    async def add_message(
        self,
        thread_id: str,
        user_id: str,
        role: str,
        content: str,
        run_id: Optional[str] = None,
    ) -> None:
        await mongo_manager.insert_one(
            "agent_messages",
            {
                "message_id": uuid.uuid4().hex,
                "thread_id": thread_id,
                "user_id": user_id,
                "run_id": run_id,
                "role": role,
                "content": content,
            },
        )

    async def search_memories(
        self,
        user_id: str,
        query: str,
        limit: int = 8,
    ) -> list[MemoryItem]:
        await self.forget_expired_memories(user_id)
        now = datetime.utcnow()
        active_filter = {
            "user_id": user_id,
            "status": {"$ne": "archived"},
            "$or": [{"expires_at": {"$exists": False}}, {"expires_at": None}, {"expires_at": {"$gt": now}}],
        }
        semantic = await self._search_semantic_memories(query, limit)
        if semantic:
            memory_ids = [item["memory_id"] for item in semantic if item.get("memory_id")]
            docs = await mongo_manager.find_many(
                "agent_memories",
                {**active_filter, "memory_id": {"$in": memory_ids}},
                projection={"_id": 0},
                limit=limit,
            )
            if docs:
                await self.record_memory_hits(user_id, [doc["memory_id"] for doc in docs], query)
                return [MemoryItem(**doc) for doc in docs]

        docs = await mongo_manager.find_many(
            "agent_memories",
            active_filter,
            projection={"_id": 0},
            sort=[("pinned", -1), ("importance", -1), ("hit_count", -1), ("updated_at", -1)],
            limit=limit,
        )
        if docs:
            await self.record_memory_hits(user_id, [doc["memory_id"] for doc in docs], query)
        return [MemoryItem(**doc) for doc in docs]

    async def maybe_write_memory(
        self,
        user_id: str,
        content: str,
        source_run_id: str,
        memory_type: str = "episode",
        importance: float = 0.45,
        confidence: float = 0.8,
        expires_at: Optional[datetime] = None,
        pinned: bool = False,
    ) -> str:
        memory_id = uuid.uuid4().hex
        normalized = self._normalize(content)
        doc = {
            "memory_id": memory_id,
            "user_id": user_id,
            "scope": "user",
            "type": memory_type,
            "content": content[:2000],
            "normalized": normalized,
            "confidence": confidence,
            "importance": importance,
            "hit_count": 0,
            "last_accessed_at": None,
            "expires_at": expires_at,
            "pinned": pinned,
            "status": "active",
            "conflicts_with": [],
            "source_run_id": source_run_id,
            "updated_at": datetime.utcnow(),
        }
        await mongo_manager.insert_one("agent_memories", doc)
        await self.resolve_conflicts(user_id, doc)
        await self._insert_semantic_memory(memory_id, content)
        return memory_id

    async def maintain_after_run(
        self,
        user_id: str,
        thread_id: str,
        run_id: str,
        user_message: str,
        assistant_output: str,
    ) -> None:
        """Run advanced memory maintenance after a completed interaction."""
        await self.forget_expired_memories(user_id)
        await self.decay_importance(user_id)
        summary = await self.summarize_thread(thread_id, user_id)
        if summary:
            memory_id = await self.upsert_memory_deduped(
                user_id=user_id,
                content=summary,
                source_run_id=run_id,
                memory_type="episode",
                importance=0.62,
                confidence=0.78,
            )
            await self.promote_hot_episodes(user_id)
            await self.consolidate_memories(user_id)
            await self.merge_user_profile(
                user_id=user_id,
                user_message=user_message,
                assistant_output=assistant_output,
                summary=summary,
                source_memory_id=memory_id,
            )

    async def summarize_thread(self, thread_id: str, user_id: str, limit: int = 12) -> str:
        history = await self.get_thread_history(thread_id, user_id, limit=limit)
        if not history:
            return ""
        transcript = "\n".join(
            f"{item.get('role')}: {item.get('content', '')[:1200]}" for item in history
        )
        if not llm_manager.is_initialized:
            return transcript[-1200:]
        try:
            return await llm_manager.chat(
                [
                    {
                        "role": "system",
                        "content": "你是记忆整理器。请把对话压缩为可复用长期记忆，保留用户偏好、关注标的、任务目标、结论和风险点，120字以内。",
                    },
                    {"role": "user", "content": transcript},
                ],
                temperature=0.1,
                max_tokens=300,
            )
        except Exception:
            return transcript[-1200:]

    async def upsert_memory_deduped(
        self,
        user_id: str,
        content: str,
        source_run_id: str,
        memory_type: str,
        importance: float,
        confidence: float = 0.8,
        expires_at: Optional[datetime] = None,
        pinned: bool = False,
    ) -> str:
        normalized = self._normalize(content)
        existing = await mongo_manager.find_one(
            "agent_memories",
            {"user_id": user_id, "type": memory_type, "normalized": normalized, "status": {"$ne": "archived"}},
            projection={"_id": 0},
        )
        if existing:
            await mongo_manager.update_one(
                "agent_memories",
                {"memory_id": existing["memory_id"]},
                {
                    "$set": {
                        "content": content[:2000],
                        "source_run_id": source_run_id,
                        "last_accessed_at": datetime.utcnow(),
                        "status": "active",
                    },
                    "$max": {"importance": importance},
                    "$inc": {"reinforcement": 1, "hit_count": 1},
                },
            )
            return existing["memory_id"]

        memory_id = uuid.uuid4().hex
        await mongo_manager.insert_one(
            "agent_memories",
            {
                "memory_id": memory_id,
                "user_id": user_id,
                "scope": "user",
                "type": memory_type,
                "content": content[:2000],
                "normalized": normalized,
                "confidence": confidence,
                "importance": importance,
                "hit_count": 1,
                "last_accessed_at": datetime.utcnow(),
                "expires_at": expires_at,
                "pinned": pinned,
                "status": "active",
                "conflicts_with": [],
                "reinforcement": 1,
                "source_run_id": source_run_id,
                "updated_at": datetime.utcnow(),
            },
        )
        await self.resolve_conflicts(
            user_id,
            {
                "memory_id": memory_id,
                "type": memory_type,
                "content": content,
                "confidence": confidence,
            },
        )
        await self._insert_semantic_memory(memory_id, content)
        return memory_id

    async def decay_importance(self, user_id: str, older_than_days: int = 14) -> None:
        cutoff = datetime.utcnow() - timedelta(days=older_than_days)
        memories = await mongo_manager.find_many(
            "agent_memories",
            {
                "user_id": user_id,
                "updated_at": {"$lt": cutoff},
                "importance": {"$gt": 0.2},
                "pinned": {"$ne": True},
                "status": {"$ne": "archived"},
            },
            projection={"_id": 0, "memory_id": 1, "importance": 1, "type": 1, "expires_at": 1},
            limit=100,
        )
        for memory in memories:
            decay = 0.88 if memory.get("expires_at") else 0.96
            await mongo_manager.update_one(
                "agent_memories",
                {"memory_id": memory["memory_id"]},
                {"$set": {"importance": max(0.2, float(memory.get("importance", 0.5)) * decay)}},
            )

    async def record_memory_hits(self, user_id: str, memory_ids: list[str], query: str = "") -> None:
        if not memory_ids:
            return
        now = datetime.utcnow()
        for memory_id in memory_ids:
            await mongo_manager.update_one(
                "agent_memories",
                {"memory_id": memory_id, "user_id": user_id},
                {
                    "$set": {"last_accessed_at": now},
                    "$inc": {"hit_count": 1},
                    "$max": {"importance": 0.55},
                },
            )
        await self.promote_hot_episodes(user_id)

    async def promote_hot_episodes(self, user_id: str, min_hits: int = 3) -> None:
        """Promote repeatedly retrieved episode summaries to durable memories."""
        episodes = await mongo_manager.find_many(
            "agent_memories",
            {
                "user_id": user_id,
                "type": "episode",
                "hit_count": {"$gte": min_hits},
                "status": "active",
                "promoted_to": {"$exists": False},
            },
            projection={"_id": 0},
            sort=[("hit_count", -1), ("importance", -1)],
            limit=10,
        )
        for episode in episodes:
            memory_id = await self.upsert_memory_deduped(
                user_id=user_id,
                content=episode.get("content", ""),
                source_run_id=episode.get("source_run_id", ""),
                memory_type=self._promoted_type(episode.get("content", "")),
                importance=max(0.75, float(episode.get("importance", 0.5))),
                confidence=max(0.82, float(episode.get("confidence", 0.75))),
            )
            await mongo_manager.update_one(
                "agent_memories",
                {"memory_id": episode["memory_id"]},
                {"$set": {"promoted_to": memory_id, "status": "promoted"}},
            )

    async def consolidate_memories(self, user_id: str, min_hits: int = 2) -> Optional[str]:
        """Merge several strong memories into one compact procedure/fact memory."""
        candidates = await mongo_manager.find_many(
            "agent_memories",
            {
                "user_id": user_id,
                "status": {"$in": ["active", "conflict"]},
                "pinned": {"$ne": True},
                "hit_count": {"$gte": min_hits},
                "type": {"$in": ["episode", "fact", "preference", "procedure"]},
            },
            projection={"_id": 0},
            sort=[("hit_count", -1), ("importance", -1)],
            limit=8,
        )
        if len(candidates) < 3:
            return None
        content = await self._consolidated_content(candidates)
        memory_id = await self.upsert_memory_deduped(
            user_id=user_id,
            content=content,
            source_run_id="memory_consolidation",
            memory_type="procedure" if "流程" in content or "偏好" in content else "fact",
            importance=0.82,
            confidence=0.86,
        )
        for item in candidates:
            await mongo_manager.update_one(
                "agent_memories",
                {"memory_id": item["memory_id"]},
                {"$set": {"consolidated_to": memory_id}},
            )
        return memory_id

    async def resolve_conflicts(self, user_id: str, memory: dict[str, Any]) -> list[str]:
        """Mark potentially contradictory memories instead of deleting either side."""
        if memory.get("type") not in {"preference", "fact", "procedure"}:
            return []
        peers = await mongo_manager.find_many(
            "agent_memories",
            {
                "user_id": user_id,
                "memory_id": {"$ne": memory.get("memory_id")},
                "type": memory.get("type"),
                "status": {"$ne": "archived"},
            },
            projection={"_id": 0, "memory_id": 1, "content": 1, "confidence": 1},
            sort=[("updated_at", -1)],
            limit=30,
        )
        conflicts = [
            peer["memory_id"]
            for peer in peers
            if await self._is_conflicting(memory.get("content", ""), peer.get("content", ""))
        ]
        if not conflicts:
            return []
        await mongo_manager.update_one(
            "agent_memories",
            {"memory_id": memory["memory_id"]},
            {
                "$set": {"status": "conflict", "conflicts_with": conflicts},
                "$max": {"importance": 0.7},
            },
        )
        for peer_id in conflicts:
            await mongo_manager.update_one(
                "agent_memories",
                {"memory_id": peer_id},
                {
                    "$set": {"status": "conflict"},
                    "$addToSet": {"conflicts_with": memory["memory_id"]},
                },
            )
        return conflicts

    async def forget_expired_memories(self, user_id: str) -> int:
        """Archive expired or very weak memories while preserving pinned items."""
        now = datetime.utcnow()
        expired = await mongo_manager.update_many(
            "agent_memories",
            {
                "user_id": user_id,
                "pinned": {"$ne": True},
                "status": {"$ne": "archived"},
                "expires_at": {"$lte": now},
            },
            {"$set": {"status": "archived", "archived_reason": "expired_market_info"}},
        )
        stale_cutoff = now - timedelta(days=45)
        stale = await mongo_manager.update_many(
            "agent_memories",
            {
                "user_id": user_id,
                "pinned": {"$ne": True},
                "status": {"$ne": "archived"},
                "importance": {"$lt": 0.25},
                "hit_count": {"$lt": 2},
                "updated_at": {"$lt": stale_cutoff},
            },
            {"$set": {"status": "archived", "archived_reason": "low_value_stale"}},
        )
        return int(expired or 0) + int(stale or 0)

    async def pin_memory(self, user_id: str, memory_id: str, pinned: bool = True) -> bool:
        updated = await mongo_manager.update_one(
            "agent_memories",
            {"memory_id": memory_id, "user_id": user_id},
            {
                "$set": {"pinned": pinned, "status": "active"},
                "$max": {"importance": 0.9},
            },
        )
        return bool(updated)

    async def archive_memory(self, user_id: str, memory_id: str, reason: str = "manual") -> bool:
        updated = await mongo_manager.update_one(
            "agent_memories",
            {"memory_id": memory_id, "user_id": user_id, "pinned": {"$ne": True}},
            {"$set": {"status": "archived", "archived_reason": reason}},
        )
        return bool(updated)

    async def merge_user_profile(
        self,
        user_id: str,
        user_message: str,
        assistant_output: str,
        summary: str,
        source_memory_id: str,
    ) -> None:
        profile = await mongo_manager.find_one(
            "agent_user_profiles",
            {"user_id": user_id},
            projection={"_id": 0},
        )
        current = profile.get("profile", "") if profile else ""
        if llm_manager.is_initialized:
            try:
                merged = await llm_manager.chat(
                    [
                        {
                            "role": "system",
                            "content": "你是用户画像合并器。请合并旧画像和新观察，输出简洁中文 JSON，字段包含 risk_preference, watched_symbols, strategy_preferences, notes。",
                        },
                        {
                            "role": "user",
                            "content": (
                                f"旧画像:\n{current}\n\n新观察:\n用户: {user_message}\n"
                                f"助手: {assistant_output[:1200]}\n摘要: {summary}"
                            ),
                        },
                    ],
                    temperature=0.1,
                    max_tokens=600,
                )
            except Exception:
                merged = f"{current}\n{summary}".strip()
        else:
            merged = f"{current}\n{summary}".strip()

        await mongo_manager.update_one(
            "agent_user_profiles",
            {"user_id": user_id},
            {
                "$set": {
                    "user_id": user_id,
                    "profile": merged[:4000],
                    "source_memory_id": source_memory_id,
                    "updated_at": datetime.utcnow(),
                }
            },
            upsert=True,
        )

    async def _consolidated_content(self, candidates: list[dict[str, Any]]) -> str:
        joined = "\n".join(
            f"- [{item.get('type')}] {item.get('content', '')[:600]}" for item in candidates
        )
        if llm_manager.is_initialized:
            try:
                return await llm_manager.chat(
                    [
                        {
                            "role": "system",
                            "content": (
                                "你是长期记忆整合器。把多条记忆合并为一条稳定、可执行、"
                                "无重复的长期记忆。保留用户偏好、事实、流程和约束，120字以内。"
                            ),
                        },
                        {"role": "user", "content": joined},
                    ],
                    temperature=0.1,
                    max_tokens=300,
                )
            except Exception:
                pass
        return "；".join(item.get("content", "")[:120] for item in candidates[:4])

    async def _is_conflicting(self, left: str, right: str) -> bool:
        if not left or not right:
            return False
        if self._simple_conflict(left, right):
            return True
        if not llm_manager.is_initialized:
            return False
        try:
            judged = await llm_manager.chat(
                [
                    {
                        "role": "system",
                        "content": "判断两条记忆是否互相矛盾。只输出 JSON: {\"conflict\": true|false}。",
                    },
                    {
                        "role": "user",
                        "content": f"记忆A: {left[:800]}\n记忆B: {right[:800]}",
                    },
                ],
                temperature=0.0,
                max_tokens=80,
            )
            payload = judged.strip().strip("`").removeprefix("json").strip()
            return bool(json.loads(payload).get("conflict"))
        except Exception:
            return False

    @staticmethod
    def _simple_conflict(left: str, right: str) -> bool:
        negations = ["不", "不要", "不再", "避免", "拒绝", "不喜欢", "不关注"]
        affirmations = ["要", "喜欢", "关注", "偏好", "需要", "使用"]
        shared_terms = set(left.split()) & set(right.split())
        if len(shared_terms) < 2 and not any(term in left and term in right for term in ("风险", "短线", "长线", "回测", "A股")):
            return False
        left_negative = any(word in left for word in negations)
        right_negative = any(word in right for word in negations)
        left_positive = any(word in left for word in affirmations)
        right_positive = any(word in right for word in affirmations)
        return (left_negative and right_positive) or (right_negative and left_positive)

    @staticmethod
    def _promoted_type(content: str) -> str:
        if any(word in content for word in ("偏好", "喜欢", "不喜欢", "关注", "风险承受")):
            return "preference"
        if any(word in content for word in ("步骤", "流程", "先", "再", "策略")):
            return "procedure"
        return "fact"

    @staticmethod
    def _normalize(content: str) -> str:
        return "".join(content.lower().split())[:500]

    async def _search_semantic_memories(self, query: str, limit: int) -> list[dict]:
        if not llm_manager.is_initialized or not milvus_manager.is_initialized:
            return []
        try:
            if getattr(milvus_manager, "is_disabled")():
                return []
            vectors = await llm_manager.embedding([query])
            results = await milvus_manager.search_market_snippets(vectors[0], top_k=limit)
            return [
                {**item, "memory_id": str(item.get("market_sentiment", "")).removeprefix("agent_memory:")}
                for item in results
                if str(item.get("market_sentiment", "")).startswith("agent_memory:")
            ]
        except Exception:
            return []

    async def _insert_semantic_memory(self, memory_id: str, content: str) -> None:
        if not llm_manager.is_initialized or not milvus_manager.is_initialized:
            return
        try:
            if getattr(milvus_manager, "is_disabled")():
                return
            vectors = await llm_manager.embedding([content])
            await milvus_manager.add_market_snippet(
                vector=vectors[0],
                content=content[:4000],
                analysis_date=datetime.utcnow().strftime("%Y%m%d"),
                market_sentiment=f"agent_memory:{memory_id}",
            )
        except Exception:
            return
