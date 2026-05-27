"""Agent evaluation for dashboards, regression checks and training filters."""

from __future__ import annotations

import json
import statistics
import uuid
from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel, Field

from core.managers import llm_manager, mongo_manager


class EvalDimensionScores(BaseModel):
    task_completion: float = 0.0
    tool_call_accuracy: float = 0.0
    factual_accuracy: float = 0.0
    context_utilization: float = 0.0
    risk_disclosure: float = 0.0
    hallucination_rate: float = 1.0
    approval_hit_rate: float = 0.0
    avg_tool_rounds: float = 0.0
    avg_latency_ms: float = 0.0


class EvalResult(BaseModel):
    eval_id: Optional[str] = None
    result_id: Optional[str] = None
    run_id: Optional[str] = None
    case_id: Optional[str] = None
    scores: EvalDimensionScores = Field(default_factory=EvalDimensionScores)
    overall: float = 0.0
    passed: bool = False
    issues: list[str] = Field(default_factory=list)
    sample_type: str = "normal"
    question: str = ""
    answer: str = ""
    expected_tools: list[str] = Field(default_factory=list)
    called_tools: list[str] = Field(default_factory=list)
    latency_ms: float = 0.0


class EvalRunRequest(BaseModel):
    run_ids: list[str] = Field(default_factory=list)
    case_ids: list[str] = Field(default_factory=list)
    limit: int = Field(default=20, ge=1, le=100)
    min_pass_score: float = Field(default=0.7, ge=0, le=1)
    use_llm_judge: bool = True


class EvalRunSummary(BaseModel):
    eval_id: str
    total: int = 0
    passed: int = 0
    pass_rate: float = 0.0
    average_score: float = 0.0
    metrics: EvalDimensionScores = Field(default_factory=EvalDimensionScores)
    failed_samples: list[dict[str, Any]] = Field(default_factory=list)
    tool_misuse_samples: list[dict[str, Any]] = Field(default_factory=list)
    low_score_answers: list[dict[str, Any]] = Field(default_factory=list)


class AgentEvaluator:
    """Evaluates completed agent runs with heuristics plus optional LLM judge."""

    async def evaluate_run(self, run_id: str, user_id: str) -> EvalResult:
        result = await self._evaluate_single_run(
            eval_id=None,
            run_id=run_id,
            user_id=user_id,
            case=None,
            min_pass_score=0.7,
            use_llm_judge=True,
        )
        if result.result_id:
            await mongo_manager.update_one(
                "agent_runs",
                {"run_id": run_id},
                {"$set": {"eval": result.model_dump(mode="json")}},
            )
        return result

    async def run_eval(self, user_id: str, request: EvalRunRequest) -> EvalRunSummary:
        eval_id = uuid.uuid4().hex
        await mongo_manager.insert_one(
            "agent_eval_runs",
            {
                "eval_id": eval_id,
                "user_id": user_id,
                "status": "running",
                "request": request.model_dump(mode="json"),
                "started_at": datetime.utcnow(),
            },
        )

        cases = await self._load_cases(user_id, request.case_ids)
        run_ids = request.run_ids or [run["run_id"] for run in await self._load_recent_runs(user_id, request.limit)]
        results: list[EvalResult] = []

        for run_id in run_ids:
            case = self._match_case(run_id, cases)
            results.append(
                await self._evaluate_single_run(
                    eval_id=eval_id,
                    run_id=run_id,
                    user_id=user_id,
                    case=case,
                    min_pass_score=request.min_pass_score,
                    use_llm_judge=request.use_llm_judge,
                )
            )

        summary = self._summarize(eval_id, results)
        await mongo_manager.update_one(
            "agent_eval_runs",
            {"eval_id": eval_id, "user_id": user_id},
            {
                "$set": {
                    "status": "completed",
                    "completed_at": datetime.utcnow(),
                    "summary": summary.model_dump(mode="json"),
                }
            },
        )
        return summary

    async def get_eval(self, eval_id: str, user_id: str) -> dict[str, Any]:
        eval_run = await mongo_manager.find_one(
            "agent_eval_runs",
            {"eval_id": eval_id, "user_id": user_id},
            projection={"_id": 0},
        )
        if not eval_run:
            return {"error": "eval not found"}
        results = await mongo_manager.find_many(
            "agent_eval_results",
            {"eval_id": eval_id, "user_id": user_id},
            projection={"_id": 0},
            sort=[("overall", 1)],
            limit=200,
        )
        eval_run["results"] = results
        return eval_run

    async def get_summary(self, user_id: str, limit: int = 5) -> dict[str, Any]:
        recent = await mongo_manager.find_many(
            "agent_eval_runs",
            {"user_id": user_id},
            projection={"_id": 0},
            sort=[("started_at", -1)],
            limit=limit,
        )
        latest = recent[0] if recent else None
        return {
            "latest": latest,
            "recent": recent,
            "totals": await self._global_totals(user_id),
        }

    async def _evaluate_single_run(
        self,
        eval_id: Optional[str],
        run_id: str,
        user_id: str,
        case: Optional[dict[str, Any]],
        min_pass_score: float,
        use_llm_judge: bool,
    ) -> EvalResult:
        run = await mongo_manager.find_one(
            "agent_runs",
            {"run_id": run_id, "user_id": user_id},
            projection={"_id": 0},
        )
        events = await mongo_manager.find_many(
            "agent_events",
            {"run_id": run_id},
            projection={"_id": 0},
            sort=[("sequence", 1)],
        )
        if not run:
            return EvalResult(eval_id=eval_id, run_id=run_id, issues=["run not found"])

        question = run.get("request", {}).get("message", "")
        answer = run.get("output", "") or ""
        tool_events = [event for event in events if event.get("event") == "tool_call_completed"]
        approval_events = [event for event in events if event.get("event") == "tool_approval_required"]
        memory_events = [event for event in events if event.get("event") == "memory_loaded"]
        context_events = [event for event in events if event.get("event") == "context_compressed"]
        expected_tools = self._expected_tools(events, case)
        called_tools = [str(event.get("data", {}).get("name", "")) for event in tool_events if event.get("data")]
        latency_ms = self._latency_ms(run, events)

        scores = EvalDimensionScores(
            task_completion=0.85 if answer else 0.05,
            tool_call_accuracy=self._tool_accuracy(expected_tools, tool_events),
            factual_accuracy=0.78 if tool_events else 0.55,
            context_utilization=self._context_utilization(memory_events, context_events, answer),
            risk_disclosure=0.9 if self._has_risk_disclosure(answer) else 0.35,
            hallucination_rate=self._hallucination_rate(answer, tool_events),
            approval_hit_rate=min(1.0, len(approval_events) / max(1, len(tool_events))),
            avg_tool_rounds=float(len(tool_events)),
            avg_latency_ms=latency_ms,
        )

        issues = self._heuristic_issues(scores, expected_tools, called_tools, answer)
        if use_llm_judge and llm_manager.is_initialized and answer:
            scores, issues = await self._llm_judge(question, answer, tool_events, scores, issues)

        overall = self._overall(scores)
        sample_type = self._sample_type(overall, scores, expected_tools, called_tools, min_pass_score)
        result = EvalResult(
            eval_id=eval_id,
            result_id=uuid.uuid4().hex,
            run_id=run_id,
            case_id=case.get("case_id") if case else None,
            scores=scores,
            overall=overall,
            passed=overall >= min_pass_score,
            issues=issues,
            sample_type=sample_type,
            question=question,
            answer=answer[:1600],
            expected_tools=expected_tools,
            called_tools=called_tools,
            latency_ms=latency_ms,
        )
        await mongo_manager.insert_one(
            "agent_eval_results",
            {"user_id": user_id, **result.model_dump(mode="json")},
        )
        return result

    async def _load_recent_runs(self, user_id: str, limit: int) -> list[dict[str, Any]]:
        return await mongo_manager.find_many(
            "agent_runs",
            {"user_id": user_id, "status": "completed"},
            projection={"_id": 0, "run_id": 1},
            sort=[("started_at", -1)],
            limit=limit,
        )

    async def _load_cases(self, user_id: str, case_ids: list[str]) -> list[dict[str, Any]]:
        filter_query: dict[str, Any] = {"$or": [{"user_id": user_id}, {"scope": "global"}]}
        if case_ids:
            filter_query["case_id"] = {"$in": case_ids}
        return await mongo_manager.find_many(
            "agent_eval_cases",
            filter_query,
            projection={"_id": 0},
            sort=[("created_at", -1)],
            limit=200,
        )

    @staticmethod
    def _match_case(run_id: str, cases: list[dict[str, Any]]) -> Optional[dict[str, Any]]:
        for case in cases:
            if case.get("run_id") == run_id:
                return case
        return None

    @staticmethod
    def _expected_tools(events: list[dict[str, Any]], case: Optional[dict[str, Any]]) -> list[str]:
        if case and case.get("expected_tools"):
            return list(case["expected_tools"])
        for event in events:
            if event.get("event") == "plan_created":
                expected = event.get("data", {}).get("expected_tools", [])
                if isinstance(expected, list):
                    return [str(item) for item in expected]
        return []

    @staticmethod
    def _tool_accuracy(expected_tools: list[str], tool_events: list[dict[str, Any]]) -> float:
        if not tool_events and not expected_tools:
            return 1.0
        called = [str(event.get("data", {}).get("name", "")) for event in tool_events]
        successful = [event for event in tool_events if event.get("data", {}).get("success", False)]
        success_rate = len(successful) / max(1, len(tool_events))
        if not expected_tools:
            return round(success_rate, 3)
        expected_hit = len(set(expected_tools) & set(called)) / max(1, len(set(expected_tools)))
        extra_penalty = max(0, len(set(called) - set(expected_tools))) * 0.12
        return round(max(0.0, min(1.0, (success_rate + expected_hit) / 2 - extra_penalty)), 3)

    @staticmethod
    def _context_utilization(
        memory_events: list[dict[str, Any]],
        context_events: list[dict[str, Any]],
        answer: str,
    ) -> float:
        memory_count = sum(int(event.get("data", {}).get("count", 0) or 0) for event in memory_events)
        context_messages = sum(int(event.get("data", {}).get("messages", 0) or 0) for event in context_events)
        if memory_count == 0 and context_messages <= 2:
            return 0.65
        signal = 0.45 + min(0.35, memory_count * 0.08) + min(0.2, context_messages * 0.025)
        if any(word in answer for word in ("根据", "结合", "历史", "记忆", "上下文")):
            signal += 0.1
        return round(min(1.0, signal), 3)

    @staticmethod
    def _hallucination_rate(answer: str, tool_events: list[dict[str, Any]]) -> float:
        if not answer:
            return 1.0
        risky_phrases = ["保证", "一定", "必然", "内幕", "稳赚", "无风险"]
        risk = sum(1 for phrase in risky_phrases if phrase in answer) * 0.18
        if tool_events:
            risk -= 0.2
        if "无法确认" in answer or "需核验" in answer:
            risk -= 0.15
        return round(max(0.0, min(1.0, 0.35 + risk)), 3)

    @staticmethod
    def _has_risk_disclosure(answer: str) -> bool:
        return any(phrase in answer for phrase in ("风险", "不构成投资建议", "仅供参考", "需谨慎", "回撤"))

    @staticmethod
    def _latency_ms(run: dict[str, Any], events: list[dict[str, Any]]) -> float:
        start = run.get("started_at") or (events[0].get("created_at") if events else None)
        end = run.get("completed_at") or (events[-1].get("created_at") if events else None)
        if isinstance(start, datetime) and isinstance(end, datetime):
            return round((end - start).total_seconds() * 1000, 2)
        return 0.0

    @staticmethod
    def _heuristic_issues(
        scores: EvalDimensionScores,
        expected_tools: list[str],
        called_tools: list[str],
        answer: str,
    ) -> list[str]:
        issues = []
        if scores.task_completion < 0.6:
            issues.append("任务未充分完成")
        if expected_tools and not set(expected_tools).issubset(set(called_tools)):
            issues.append("缺少预期工具调用")
        if set(called_tools) - set(expected_tools) and expected_tools:
            issues.append("存在非预期工具调用")
        if scores.risk_disclosure < 0.6:
            issues.append("风险提示不足")
        if scores.hallucination_rate > 0.55:
            issues.append("幻觉风险偏高")
        if not answer:
            issues.append("回答为空")
        return issues

    async def _llm_judge(
        self,
        question: str,
        answer: str,
        tool_events: list[dict[str, Any]],
        scores: EvalDimensionScores,
        issues: list[str],
    ) -> tuple[EvalDimensionScores, list[str]]:
        try:
            judged = await llm_manager.chat(
                [
                    {
                        "role": "system",
                        "content": (
                            "你是 Agent Evaluation Judge。只输出 JSON。"
                            "字段: task_completion, tool_call_accuracy, factual_accuracy, "
                            "context_utilization, risk_disclosure, hallucination_rate, issues。"
                            "所有分数为 0-1，hallucination_rate 越低越好。"
                        ),
                    },
                    {
                        "role": "user",
                        "content": json.dumps(
                            {"question": question, "answer": answer, "tool_events": tool_events[:8]},
                            ensure_ascii=False,
                        ),
                    },
                ],
                max_tokens=700,
                temperature=0.1,
            )
            payload = json.loads(self._strip_json(judged))
            merged = scores.model_dump()
            for key in (
                "task_completion",
                "tool_call_accuracy",
                "factual_accuracy",
                "context_utilization",
                "risk_disclosure",
                "hallucination_rate",
            ):
                if key in payload:
                    merged[key] = self._clamp(payload[key])
            next_issues = payload.get("issues", issues)
            if not isinstance(next_issues, list):
                next_issues = issues
            return EvalDimensionScores(**merged), [str(item) for item in next_issues]
        except Exception:
            return scores, issues

    @staticmethod
    def _overall(scores: EvalDimensionScores) -> float:
        positive = [
            scores.task_completion,
            scores.tool_call_accuracy,
            scores.factual_accuracy,
            scores.context_utilization,
            scores.risk_disclosure,
            1 - scores.hallucination_rate,
        ]
        return round(sum(positive) / len(positive), 3)

    @staticmethod
    def _sample_type(
        overall: float,
        scores: EvalDimensionScores,
        expected_tools: list[str],
        called_tools: list[str],
        min_pass_score: float,
    ) -> str:
        if expected_tools and set(called_tools) - set(expected_tools):
            return "tool_misuse"
        if scores.tool_call_accuracy < 0.55:
            return "tool_misuse"
        if overall < min_pass_score:
            return "low_score"
        return "normal"

    def _summarize(self, eval_id: str, results: list[EvalResult]) -> EvalRunSummary:
        total = len(results)
        passed = len([item for item in results if item.passed])
        metrics = EvalDimensionScores()
        if total:
            metric_names = metrics.model_fields.keys()
            averaged = {
                name: round(statistics.mean(getattr(item.scores, name) for item in results), 3)
                for name in metric_names
            }
            metrics = EvalDimensionScores(**averaged)
        low = sorted(results, key=lambda item: item.overall)[:5]
        tool_misuse = [item for item in results if item.sample_type == "tool_misuse"][:5]
        failed = [item for item in results if not item.passed][:5]
        return EvalRunSummary(
            eval_id=eval_id,
            total=total,
            passed=passed,
            pass_rate=round(passed / max(1, total), 3),
            average_score=round(statistics.mean([item.overall for item in results]) if results else 0.0, 3),
            metrics=metrics,
            failed_samples=[self._sample(item) for item in failed],
            tool_misuse_samples=[self._sample(item) for item in tool_misuse],
            low_score_answers=[self._sample(item) for item in low],
        )

    @staticmethod
    def _sample(item: EvalResult) -> dict[str, Any]:
        return {
            "run_id": item.run_id,
            "overall": item.overall,
            "sample_type": item.sample_type,
            "question": item.question[:160],
            "answer": item.answer[:240],
            "issues": item.issues,
            "expected_tools": item.expected_tools,
            "called_tools": item.called_tools,
        }

    async def _global_totals(self, user_id: str) -> dict[str, Any]:
        runs = await mongo_manager.count("agent_eval_runs", {"user_id": user_id})
        results = await mongo_manager.count("agent_eval_results", {"user_id": user_id})
        return {"eval_runs": runs, "eval_results": results}

    @staticmethod
    def _strip_json(text: str) -> str:
        text = text.strip()
        if text.startswith("```"):
            text = text.strip("`")
            text = text.removeprefix("json").strip()
        return text

    @staticmethod
    def _clamp(value: Any) -> float:
        try:
            return round(max(0.0, min(1.0, float(value))), 3)
        except Exception:
            return 0.0
