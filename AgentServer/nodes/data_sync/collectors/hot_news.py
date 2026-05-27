"""
热点新闻采集器 (金融 + 娱乐版)

参考 newsnow 项目实现，专注于金融和娱乐类新闻源:

金融类:
- cls: 财联社 (电报)
- xueqiu: 雪球 (热门股票)
- wallstreetcn: 华尔街见闻 (快讯)
- gelonghui: 格隆汇 (事件)
- jin10: 金十数据 (快讯)

娱乐类:
- douyin: 抖音 (热搜)
- bilibili: 哔哩哔哩 (热搜)

科技类:
- github: Github Trending

采集策略:
- 异步并发抓取多个来源
- 统一数据格式存入 Redis (TTL=2小时)
- 支持手动触发刷新

调度时间: 每小时执行一次
"""

import asyncio
import re
import json
from typing import Dict, Any, List, Optional
from abc import ABC, abstractmethod

import httpx

from core.base import BaseCollector
from core.settings import settings
from core.managers import redis_manager


# ==================== 数据源基类 ====================


class HotNewsSource(ABC):
    """热点新闻源基类"""
    
    name: str  # 来源标识 (如 cls, xueqiu)
    display_name: str  # 显示名称 (如 财联社)
    color: str = "blue"  # 主题色
    column: str = "finance"  # 分类: finance / entertainment / tech
    
    def __init__(self):
        self.client = httpx.AsyncClient(
            timeout=15.0,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            },
            follow_redirects=True,
        )
    
    @abstractmethod
    async def fetch(self) -> List[Dict[str, Any]]:
        """
        抓取热点新闻
        
        Returns:
            新闻列表，每条新闻包含:
            - title: 标题
            - url: 链接
            - hot: 热度值
            - extra: 额外信息 (可选)
        """
        raise NotImplementedError
    
    async def close(self):
        """关闭 HTTP 客户端"""
        await self.client.aclose()


# ==================== 财联社 ====================


class CLSSource(HotNewsSource):
    """
    财联社电报
    
    API: https://www.cls.cn/nodeapi/updateTelegraphList
    """
    
    name = "cls"
    display_name = "财联社"
    color = "red"
    column = "finance"
    
    async def fetch(self) -> List[Dict[str, Any]]:
        """抓取财联社电报"""
        try:
            url = "https://www.cls.cn/nodeapi/updateTelegraphList"
            params = {
                "app": "CailianpressWeb",
                "os": "web",
                "sv": "8.4.6",
            }
            resp = await self.client.get(url, params=params)
            resp.raise_for_status()
            
            data = resp.json()
            items = data.get("data", {}).get("roll_data", [])
            
            result = []
            for idx, item in enumerate(items[:30]):
                if item.get("is_ad"):
                    continue
                
                title = item.get("title") or item.get("brief", "")
                if not title:
                    continue
                    
                result.append({
                    "title": title,
                    "url": f"https://www.cls.cn/detail/{item.get('id', '')}",
                    "hot": 30 - len(result),
                    "rank": len(result) + 1,
                    "extra": {
                        "time": item.get("ctime", 0) * 1000,
                    },
                })
            
            return result
            
        except Exception as e:
            print(f"[CLSSource] Fetch error: {e}")
            return []


# ==================== 雪球热股 ====================


class XueqiuSource(HotNewsSource):
    """
    雪球热门股票
    
    API: https://stock.xueqiu.com/v5/stock/hot_stock/list.json
    """
    
    name = "xueqiu"
    display_name = "雪球"
    color = "blue"
    column = "finance"
    
    async def fetch(self) -> List[Dict[str, Any]]:
        """抓取雪球热股"""
        try:
            # 先获取 cookie
            cookie_resp = await self.client.get("https://xueqiu.com/hq")
            cookies = cookie_resp.cookies
            
            # 获取热股数据
            url = "https://stock.xueqiu.com/v5/stock/hot_stock/list.json?size=30&_type=10&type=10"
            resp = await self.client.get(url, cookies=cookies)
            resp.raise_for_status()
            
            data = resp.json()
            items = data.get("data", {}).get("items", [])
            
            result = []
            for idx, item in enumerate(items[:30]):
                if item.get("ad"):
                    continue
                
                code = item.get("code", "")
                name = item.get("name", "")
                percent = item.get("percent", 0)
                exchange = item.get("exchange", "")
                
                result.append({
                    "title": name,
                    "url": f"https://xueqiu.com/s/{code}",
                    "hot": 30 - len(result),
                    "rank": len(result) + 1,
                    "extra": {
                        "percent": f"{percent:+.2f}%",
                        "exchange": exchange,
                        "code": code,
                    },
                })
            
            return result
            
        except Exception as e:
            print(f"[XueqiuSource] Fetch error: {e}")
            return []


# ==================== 华尔街见闻 ====================


class WallstreetcnSource(HotNewsSource):
    """
    华尔街见闻快讯
    
    API: https://api-one.wallstcn.com/apiv1/content/lives
    """
    
    name = "wallstreetcn"
    display_name = "华尔街见闻"
    color = "blue"
    column = "finance"
    
    async def fetch(self) -> List[Dict[str, Any]]:
        """抓取华尔街见闻快讯"""
        try:
            url = "https://api-one.wallstcn.com/apiv1/content/lives?channel=global-channel&limit=30"
            resp = await self.client.get(url)
            resp.raise_for_status()
            
            data = resp.json()
            items = data.get("data", {}).get("items", [])
            
            result = []
            for idx, item in enumerate(items[:30]):
                title = item.get("title") or item.get("content_text", "")
                if not title:
                    continue
                    
                result.append({
                    "title": title,
                    "url": item.get("uri", ""),
                    "hot": 30 - len(result),
                    "rank": len(result) + 1,
                    "extra": {
                        "time": item.get("display_time", 0) * 1000,
                    },
                })
            
            return result
            
        except Exception as e:
            print(f"[WallstreetcnSource] Fetch error: {e}")
            return []


# ==================== 格隆汇 ====================


class GelonghuiSource(HotNewsSource):
    """
    格隆汇事件
    
    通过快讯 API 获取新闻列表
    URL: https://www.gelonghui.com/live
    """
    
    name = "gelonghui"
    display_name = "格隆汇"
    color = "blue"
    column = "finance"
    
    async def fetch(self) -> List[Dict[str, Any]]:
        """抓取格隆汇快讯"""
        try:
            # 尝试快讯 API
            url = "https://www.gelonghui.com/api/v3/live/list?limit=30"
            resp = await self.client.get(url)
            
            if resp.status_code == 200:
                data = resp.json()
                items = data.get("result", [])
                
                result = []
                for idx, item in enumerate(items[:30]):
                    title = item.get("title") or item.get("content", "")
                    if not title:
                        continue
                    
                    # 去除 HTML 标签
                    title = re.sub(r'<[^>]+>', '', title).strip()
                    if len(title) > 100:
                        title = title[:100] + "..."
                    
                    live_id = item.get("id", "")
                    result.append({
                        "title": title,
                        "url": f"https://www.gelonghui.com/live/{live_id}" if live_id else "",
                        "hot": 30 - len(result),
                        "rank": len(result) + 1,
                        "extra": {},
                    })
                
                if result:
                    return result
            
            # 备用: 解析新闻页面 HTML
            html_url = "https://www.gelonghui.com/news"
            html_resp = await self.client.get(html_url)
            html_resp.raise_for_status()
            
            html = html_resp.text
            result = []
            
            # 匹配多种可能的文章链接格式
            patterns = [
                # 格式1: /p/数字
                r'href="(/p/\d+)"[^>]*>.*?class="[^"]*title[^"]*"[^>]*>([^<]+)',
                # 格式2: article-content 中的标题
                r'class="article-content"[^>]*>.*?href="([^"]+)"[^>]*>.*?<h2[^>]*>([^<]+)</h2>',
                # 格式3: 直接匹配带标题的链接
                r'<a[^>]*href="(/(?:p|news)/\d+)"[^>]*title="([^"]+)"',
            ]
            
            for pattern in patterns:
                matches = re.findall(pattern, html, re.DOTALL | re.IGNORECASE)
                for url_path, title in matches[:30]:
                    title = title.strip()
                    if not title or len(title) < 5:
                        continue
                    
                    full_url = url_path if url_path.startswith("http") else f"https://www.gelonghui.com{url_path}"
                    
                    # 避免重复
                    if any(item["url"] == full_url for item in result):
                        continue
                    
                    result.append({
                        "title": title,
                        "url": full_url,
                        "hot": 30 - len(result),
                        "rank": len(result) + 1,
                        "extra": {},
                    })
                    
                    if len(result) >= 30:
                        break
                
                if result:
                    break
            
            return result
            
        except Exception as e:
            print(f"[GelonghuiSource] Fetch error: {e}")
            return []


# ==================== 金十数据 ====================


class Jin10Source(HotNewsSource):
    """
    金十数据快讯
    
    API: https://www.jin10.com/flash_newest.js
    """
    
    name = "jin10"
    display_name = "金十数据"
    color = "blue"
    column = "finance"
    
    async def fetch(self) -> List[Dict[str, Any]]:
        """抓取金十数据快讯"""
        try:
            import time
            timestamp = int(time.time() * 1000)
            url = f"https://www.jin10.com/flash_newest.js?t={timestamp}"
            resp = await self.client.get(url)
            resp.raise_for_status()
            
            # 解析 JS 格式的数据
            raw_data = resp.text
            json_str = raw_data.replace("var newest = ", "").rstrip(";").strip()
            items = json.loads(json_str)
            
            result = []
            for idx, item in enumerate(items[:30]):
                data = item.get("data", {})
                title = data.get("title") or data.get("content", "")
                
                if not title:
                    continue
                
                # 去除 HTML 标签
                title = re.sub(r"</?b>", "", title)
                
                # 提取【】中的标题
                match = re.match(r"^【([^】]*)】(.*)$", title)
                if match:
                    title = match.group(1)
                    desc = match.group(2)
                else:
                    desc = ""
                
                flash_id = item.get("id", "")
                result.append({
                    "title": title,
                    "url": f"https://flash.jin10.com/detail/{flash_id}",
                    "hot": 30 - len(result),
                    "rank": len(result) + 1,
                    "extra": {
                        "desc": desc,
                        "important": bool(item.get("important")),
                    },
                })
            
            return result
            
        except Exception as e:
            print(f"[Jin10Source] Fetch error: {e}")
            return []


# ==================== 抖音热榜 ====================


class DouyinSource(HotNewsSource):
    """
    抖音热榜
    
    API: https://www.douyin.com/aweme/v1/web/hot/search/list/
    需要从 login.douyin.com 获取 cookie
    """
    
    name = "douyin"
    display_name = "抖音"
    color = "gray"
    column = "entertainment"
    
    async def fetch(self) -> List[Dict[str, Any]]:
        """抓取抖音热榜"""
        try:
            # 从 login.douyin.com 获取 cookie (关键！)
            cookie_resp = await self.client.get("https://login.douyin.com/")
            cookies = cookie_resp.cookies
            
            url = "https://www.douyin.com/aweme/v1/web/hot/search/list/?device_platform=webapp&aid=6383&channel=channel_pc_web&detail_list=1"
            resp = await self.client.get(url, cookies=cookies)
            resp.raise_for_status()
            
            data = resp.json()
            items = data.get("data", {}).get("word_list", [])
            
            result = []
            for idx, item in enumerate(items[:30]):
                word = item.get("word", "")
                if not word:
                    continue
                
                sentence_id = item.get("sentence_id", "")
                hot_value = item.get("hot_value", 0)
                
                result.append({
                    "title": word,
                    "url": f"https://www.douyin.com/hot/{sentence_id}",
                    "hot": int(hot_value) if hot_value else 30 - idx,
                    "rank": len(result) + 1,
                    "extra": {},
                })
            
            return result
            
        except Exception as e:
            print(f"[DouyinSource] Fetch error: {e}")
            return []


# ==================== 哔哩哔哩热搜 ====================


class BilibiliSource(HotNewsSource):
    """
    哔哩哔哩热搜
    
    API: https://s.search.bilibili.com/main/hotword
    """
    
    name = "bilibili"
    display_name = "哔哩哔哩"
    color = "blue"
    column = "entertainment"
    
    async def fetch(self) -> List[Dict[str, Any]]:
        """抓取 B 站热搜"""
        try:
            url = "https://s.search.bilibili.com/main/hotword?limit=30"
            resp = await self.client.get(url)
            resp.raise_for_status()
            
            data = resp.json()
            items = data.get("list", [])
            
            result = []
            for idx, item in enumerate(items[:30]):
                keyword = item.get("keyword", "")
                show_name = item.get("show_name", keyword)
                
                if not keyword:
                    continue
                
                result.append({
                    "title": show_name,
                    "url": f"https://search.bilibili.com/all?keyword={keyword}",
                    "hot": item.get("heat_score", 0) or (30 - idx),
                    "rank": len(result) + 1,
                    "extra": {
                        "icon": item.get("icon", ""),
                    },
                })
            
            return result
            
        except Exception as e:
            print(f"[BilibiliSource] Fetch error: {e}")
            return []


# ==================== Github Trending ====================


class GithubSource(HotNewsSource):
    """
    Github Trending
    
    URL: https://github.com/trending
    """
    
    name = "github"
    display_name = "Github"
    color = "gray"
    column = "tech"
    
    async def fetch(self) -> List[Dict[str, Any]]:
        """抓取 Github Trending"""
        try:
            url = "https://github.com/trending?spoken_language_code="
            resp = await self.client.get(url)
            resp.raise_for_status()
            
            html = resp.text
            result = []
            
            # 新版 Github Trending 页面结构
            # 匹配 article 中的仓库链接
            # 格式: <article ...><h2 ...><a href="/owner/repo">...</a></h2>
            pattern = r'<article[^>]*>.*?<h2[^>]*>\s*<a[^>]*href="(/[^/]+/[^"]+)"[^>]*>([^<]*(?:<span[^>]*>[^<]*</span>[^<]*)*)</a>'
            matches = re.findall(pattern, html, re.DOTALL)
            
            for idx, (url_path, raw_title) in enumerate(matches[:30]):
                # 清理标题中的空白和换行
                title = re.sub(r'\s+', ' ', raw_title).strip()
                title = re.sub(r'<[^>]+>', '', title).strip()  # 去除 HTML 标签
                title = title.replace(' / ', '/')
                
                if not title or url_path.count('/') < 2:
                    continue
                
                result.append({
                    "title": title,
                    "url": f"https://github.com{url_path.strip()}",
                    "hot": 30 - len(result),
                    "rank": len(result) + 1,
                    "extra": {},
                })
            
            # 如果新模式匹配失败，尝试备用模式
            if not result:
                # 备用: 匹配 <a> 标签直接包含仓库路径
                pattern2 = r'href="(/[a-zA-Z0-9_-]+/[a-zA-Z0-9_.-]+)"[^>]*class="[^"]*Link[^"]*"'
                matches2 = re.findall(pattern2, html)
                seen = set()
                for url_path in matches2[:50]:
                    if url_path in seen or url_path.count('/') != 2:
                        continue
                    seen.add(url_path)
                    title = url_path.lstrip('/')
                    result.append({
                        "title": title,
                        "url": f"https://github.com{url_path}",
                        "hot": 30 - len(result),
                        "rank": len(result) + 1,
                        "extra": {},
                    })
                    if len(result) >= 30:
                        break
            
            return result
            
        except Exception as e:
            print(f"[GithubSource] Fetch error: {e}")
            return []


# ==================== 稀土掘金 ====================


class JuejinSource(HotNewsSource):
    """
    稀土掘金热榜
    
    API: https://api.juejin.cn/content_api/v1/content/article_rank
    """
    
    name = "juejin"
    display_name = "稀土掘金"
    color = "blue"
    column = "tech"
    
    async def fetch(self) -> List[Dict[str, Any]]:
        """抓取稀土掘金热榜"""
        try:
            url = "https://api.juejin.cn/content_api/v1/content/article_rank?category_id=1&type=hot&spider=0"
            resp = await self.client.get(url)
            resp.raise_for_status()
            
            data = resp.json()
            items = data.get("data", [])
            
            result = []
            for idx, item in enumerate(items[:30]):
                content = item.get("content", {})
                title = content.get("title", "")
                content_id = content.get("content_id", "")
                
                if not title:
                    continue
                
                result.append({
                    "title": title,
                    "url": f"https://juejin.cn/post/{content_id}",
                    "hot": 30 - idx,
                    "rank": len(result) + 1,
                    "extra": {},
                })
            
            return result
            
        except Exception as e:
            print(f"[JuejinSource] Fetch error: {e}")
            return []


# ==================== 靠谱新闻 ====================


class KaopuSource(HotNewsSource):
    """
    靠谱新闻
    
    API: https://kaopustorage.blob.core.windows.net/news-prod/news_list_hans_0.json
    """
    
    name = "kaopu"
    display_name = "靠谱新闻"
    color = "gray"
    column = "world"
    
    async def fetch(self) -> List[Dict[str, Any]]:
        """抓取靠谱新闻"""
        try:
            url = "https://kaopustorage.blob.core.windows.net/news-prod/news_list_hans_0.json"
            resp = await self.client.get(url)
            resp.raise_for_status()
            
            items = resp.json()
            
            result = []
            for idx, item in enumerate(items[:30]):
                title = item.get("title", "")
                publisher = item.get("publisher", "")
                
                # 过滤特定来源
                if publisher in ["财新", "公视"]:
                    continue
                
                if not title:
                    continue
                
                result.append({
                    "title": title,
                    "url": item.get("link", ""),
                    "hot": 30 - len(result),
                    "rank": len(result) + 1,
                    "extra": {
                        "publisher": publisher,
                        "desc": item.get("description", ""),
                    },
                })
            
            return result
            
        except Exception as e:
            print(f"[KaopuSource] Fetch error: {e}")
            return []


# ==================== 澎湃新闻 ====================


class ThePaperSource(HotNewsSource):
    """
    澎湃新闻热榜
    
    API: https://cache.thepaper.cn/contentapi/wwwIndex/rightSidebar
    """
    
    name = "thepaper"
    display_name = "澎湃新闻"
    color = "gray"
    column = "china"
    
    async def fetch(self) -> List[Dict[str, Any]]:
        """抓取澎湃新闻热榜"""
        try:
            url = "https://cache.thepaper.cn/contentapi/wwwIndex/rightSidebar"
            resp = await self.client.get(url)
            resp.raise_for_status()
            
            data = resp.json()
            items = data.get("data", {}).get("hotNews", [])
            
            result = []
            for idx, item in enumerate(items[:30]):
                title = item.get("name", "")
                cont_id = item.get("contId", "")
                
                if not title:
                    continue
                
                result.append({
                    "title": title,
                    "url": f"https://www.thepaper.cn/newsDetail_forward_{cont_id}",
                    "hot": 30 - idx,
                    "rank": len(result) + 1,
                    "extra": {},
                })
            
            return result
            
        except Exception as e:
            print(f"[ThePaperSource] Fetch error: {e}")
            return []


# ==================== IT之家 ====================


class ITHomeSource(HotNewsSource):
    """
    IT之家
    
    URL: https://www.ithome.com/list/
    """
    
    name = "ithome"
    display_name = "IT之家"
    color = "red"
    column = "tech"
    
    async def fetch(self) -> List[Dict[str, Any]]:
        """抓取 IT之家"""
        try:
            url = "https://www.ithome.com/list/"
            resp = await self.client.get(url)
            resp.raise_for_status()
            
            html = resp.text
            
            # 解析 HTML
            result = []
            
            # 匹配新闻列表项
            pattern = r'<a class="t" href="([^"]+)"[^>]*>([^<]+)</a>'
            matches = re.findall(pattern, html)
            
            # 过滤广告
            ad_keywords = ["神券", "优惠", "补贴", "京东", "lapin"]
            
            for idx, (url, title) in enumerate(matches[:40]):
                title = title.strip()
                
                # 过滤广告
                if any(kw in title or kw in url for kw in ad_keywords):
                    continue
                
                if not title or not url:
                    continue
                
                result.append({
                    "title": title,
                    "url": url,
                    "hot": 30 - len(result),
                    "rank": len(result) + 1,
                    "extra": {},
                })
                
                if len(result) >= 30:
                    break
            
            return result
            
        except Exception as e:
            print(f"[ITHomeSource] Fetch error: {e}")
            return []


# ==================== 36氪 ====================


class Kr36Source(HotNewsSource):
    """
    36氪快讯
    
    URL: https://www.36kr.com/newsflashes
    """
    
    name = "36kr"
    display_name = "36氪"
    color = "blue"
    column = "tech"
    
    async def fetch(self) -> List[Dict[str, Any]]:
        """抓取 36氪快讯"""
        try:
            url = "https://www.36kr.com/newsflashes"
            resp = await self.client.get(url)
            resp.raise_for_status()
            
            html = resp.text
            
            # 解析 HTML
            result = []
            
            # 匹配快讯列表项
            pattern = r'<a class="item-title"[^>]*href="([^"]+)"[^>]*>([^<]+)</a>'
            matches = re.findall(pattern, html)
            
            for idx, (url_path, title) in enumerate(matches[:30]):
                title = title.strip()
                
                if not title:
                    continue
                
                full_url = f"https://www.36kr.com{url_path}" if url_path.startswith("/") else url_path
                
                result.append({
                    "title": title,
                    "url": full_url,
                    "hot": 30 - idx,
                    "rank": len(result) + 1,
                    "extra": {},
                })
            
            return result
            
        except Exception as e:
            print(f"[Kr36Source] Fetch error: {e}")
            return []


# ==================== 热点新闻采集器 ====================


class HotNewsCollector(BaseCollector):
    """
    热点新闻采集器
    
    采集多个来源的热点新闻，统一存入 Redis
    
    金融类:
    - cls: 财联社
    - xueqiu: 雪球
    - wallstreetcn: 华尔街见闻
    - gelonghui: 格隆汇
    - jin10: 金十数据
    
    科技类:
    - juejin: 稀土掘金
    - ithome: IT之家
    - 36kr: 36氪
    - github: Github Trending
    
    娱乐类:
    - douyin: 抖音
    - bilibili: 哔哩哔哩
    
    综合/世界:
    - kaopu: 靠谱新闻
    - thepaper: 澎湃新闻
    """
    
    name = "hot_news"
    description = "采集多来源热点新闻"
    default_schedule = "*/5 * * * *"  # 每5分钟执行一次
    
    # 注册的新闻源
    SOURCE_CLASSES = [
        # 金融类
        CLSSource,
        XueqiuSource,
        WallstreetcnSource,
        GelonghuiSource,
        Jin10Source,
        # 科技类
        JuejinSource,
        ITHomeSource,
        Kr36Source,
        GithubSource,
        # 娱乐类
        DouyinSource,
        BilibiliSource,
        # 综合/世界
        KaopuSource,
        ThePaperSource,
    ]
    
    @property
    def schedule(self) -> str:
        """从配置读取调度时间"""
        return getattr(settings.data_sync, "hot_news_schedule", None) or self.default_schedule
    
    async def collect(self) -> Dict[str, Any]:
        """
        执行采集
        
        并发抓取所有来源，统一存入 Redis
        """
        # 确保 Redis 已初始化
        if not redis_manager.is_initialized:
            await redis_manager.initialize()
        
        self.logger.info("Starting hot news collection...")
        
        # 创建所有来源实例
        sources = [cls() for cls in self.SOURCE_CLASSES]
        
        # 并发抓取
        tasks = [self._fetch_source(source) for source in sources]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # 统计结果
        success_count = 0
        fail_count = 0
        total_news = 0
        
        for source, result in zip(sources, results):
            if isinstance(result, Exception):
                self.logger.error(f"[{source.name}] Fetch failed: {result}")
                fail_count += 1
            elif isinstance(result, dict):
                success_count += 1
                total_news += result.get("count", 0)
        
        # 关闭所有 HTTP 客户端
        for source in sources:
            await source.close()
        
        self.logger.info(
            f"Hot news collection done: success={success_count}, "
            f"fail={fail_count}, total_news={total_news}"
        )
        
        return {
            "success_count": success_count,
            "fail_count": fail_count,
            "total_news": total_news,
        }
    
    async def _fetch_source(self, source: HotNewsSource) -> Dict[str, Any]:
        """
        抓取单个来源
        
        Args:
            source: 新闻源实例
            
        Returns:
            抓取结果
        """
        try:
            self.logger.debug(f"[{source.name}] Fetching...")
            items = await source.fetch()
            
            if not items:
                self.logger.warning(f"[{source.name}] No items fetched")
                return {"count": 0}
            
            # 保存到 Redis
            count = await self._save_items(source.name, source.display_name, source.color, source.column, items)
            self.logger.info(f"[{source.name}] Saved {count} items")
            
            return {"count": count}
            
        except Exception as e:
            self.logger.error(f"[{source.name}] Fetch error: {e}")
            raise
    
    async def _save_items(
        self,
        source_id: str,
        source_name: str,
        color: str,
        column: str,
        items: List[Dict[str, Any]],
    ) -> int:
        """
        保存新闻到 Redis
        
        Args:
            source_id: 来源标识
            source_name: 来源名称
            color: 主题色
            column: 分类
            items: 新闻列表
            
        Returns:
            保存的数量
        """
        from datetime import datetime
        
        # 构建文档
        documents = []
        for item in items:
            doc = {
                "source": source_id,
                "source_name": source_name,
                "color": color,
                "column": column,
                "title": item.get("title", ""),
                "url": item.get("url", ""),
                "hot": item.get("hot", 0),
                "rank": item.get("rank", 0),
                "extra": item.get("extra", {}),
                "updated_at": datetime.utcnow().isoformat(),
            }
            documents.append(doc)
        
        # 存储到 Redis
        await redis_manager.set_hot_news(source_id, documents)
        
        return len(documents)
    
    async def fetch_single_source(self, source_id: str) -> Dict[str, Any]:
        """
        抓取单个来源（用于手动刷新）
        
        Args:
            source_id: 来源标识
            
        Returns:
            抓取结果
        """
        # 查找对应的来源类
        source_class = None
        for cls in self.SOURCE_CLASSES:
            if cls.name == source_id:
                source_class = cls
                break
        
        if not source_class:
            return {"error": f"Unknown source: {source_id}"}
        
        # 确保 Redis 已初始化
        if not redis_manager.is_initialized:
            await redis_manager.initialize()
        
        # 抓取并保存
        source = source_class()
        try:
            result = await self._fetch_source(source)
            return result
        finally:
            await source.close()
    
    async def refresh(self, source_id: Optional[str] = None) -> Dict[str, Any]:
        """
        刷新热点新闻数据（API 调用入口）
        
        Args:
            source_id: 可选的来源ID，不传则刷新全部
            
        Returns:
            刷新结果
        """
        self.logger.info(f"[refresh] Starting refresh, source={source_id or 'ALL'}")
        
        if source_id:
            # 刷新单个来源
            result = await self.fetch_single_source(source_id)
        else:
            # 刷新全部来源
            result = await self.collect()
        
        self.logger.info(f"[refresh] Done: {result}")
        return result
    
    @classmethod
    def get_available_sources(cls) -> List[Dict[str, str]]:
        """
        获取所有可用的来源列表
        
        Returns:
            来源列表
        """
        return [
            {
                "id": source.name,
                "name": source.display_name,
                "color": source.color,
                "column": source.column,
            }
            for source in [c() for c in cls.SOURCE_CLASSES]
        ]
