"""
XP 画像构建模块
分析收藏Tag，构建用户XP权重
"""
import logging
import math
import itertools
import json
import asyncio
import re
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from typing import Optional

try:
    from openai import AsyncOpenAI
    HAS_OPENAI = True
except ImportError:
    HAS_OPENAI = False

from pixiv_client import Illust, PixivClient
import database as db
from utils import retry_async



logger = logging.getLogger(__name__)


# 常见别名映射表（可扩展）
TAG_ALIASES = {
    "白髪": "white_hair",
    "silver hair": "white_hair",
    "白髮": "white_hair",
    "猫耳": "cat_ears",
    "cat ears": "cat_ears",
    "nekomimi": "cat_ears",
    "ロリ": "loli",
    "巨乳": "large_breasts",
    "おっぱい": "breasts",
    "黒髪": "black_hair",
    "金髪": "blonde_hair",
    "ツインテール": "twintails",
    "twin tails": "twintails",
    "メイド": "maid",
    "水着": "swimsuit",
    "制服": "uniform",
    "ストッキング": "stockings",
    "ニーソ": "thighhighs",
    "眼鏡": "glasses",
}


class AITagProcessor:
    """AI Tag 处理器 - 过滤无意义tag和归类同义tag"""
    
    def __init__(self, config: dict):
        self.enabled = config.get("enabled", False) and HAS_OPENAI
        self.filter_meaningless = config.get("filter_meaningless", True)
        self.merge_synonyms = config.get("merge_synonyms", True)
        self.model = config.get("model", "gpt-4o-mini")
        self.batch_size = config.get("batch_size", 50)
        self.concurrency = config.get("concurrency", 3)  # 并发数
        self.pattern_users = re.compile(r"^(.*?)\d+users入り$") # 预编译正则
        
        if self.enabled:
            self.client = AsyncOpenAI(
                api_key=config.get("api_key", ""),
                base_url=config.get("base_url") or None
            )
        else:
            self.client = None
        
        # 缓存处理结果 (Tag -> CleanedTag/None); 仅成功结果写入
        self._cache: dict[str, str | None] = {}
        self._cache_initialized = False
        # 本轮任务新增的错误 ID (调用方负责在任务开始/告警后清空)
        self.occurred_errors: list[int] = []

    def clear_occurred_errors(self):
        """清空本轮错误记录"""
        self.occurred_errors.clear()
    
    def _preprocess_tags(self, tags: list[str]) -> list[str]:
        """正则预处理：去除 users入り 后缀等"""
        processed = []
        for tag in tags:
            # 1. 去除 users入り
            match = self.pattern_users.match(tag)
            if match:
                prefix = match.group(1)
                # 如果前缀非空，则使用前缀（例如 "鸣潮"）；否则保留原样
                processed.append(prefix if prefix else tag)
            else:
                processed.append(tag)
        return processed

    async def _ensure_cache_loaded(self):
        """首次运行时加载 DB 缓存"""
        if self._cache_initialized:
            return
        try:
            db_cache = await db.get_ai_cache_map()
            self._cache.update(db_cache)
            self._cache_initialized = True
            logger.info(f"已加载 {len(db_cache)} 条 AI 标签缓存")
        except Exception as e:
            logger.error(f"加载 AI 缓存失败: {e}")

    def _apply_cache_results(
        self, tags: list[str], effective_tags: list[str]
    ) -> tuple[list[str], dict[str, str]]:
        """根据缓存构造有效标签; 未成功处理的 tag 运行时降级为原值, 不污染持久缓存语义"""
        valid_tags = []
        synonym_map = {}

        for i, original_tag in enumerate(tags):
            effective_tag = effective_tags[i]

            if effective_tag not in self._cache:
                # 本批失败或尚未处理: 仅本轮降级, 不写 cache
                result = effective_tag
            else:
                result = self._cache[effective_tag]
                if result is None:
                    continue  # meaningless

            if result != original_tag:
                synonym_map[original_tag] = result

            valid_tags.append(result)

        valid_tags = list(dict.fromkeys(valid_tags))
        return valid_tags, synonym_map

    async def process_tags(self, tags: list[str]) -> tuple[list[str], dict[str, str]]:
        if not self.enabled or not tags:
            return tags, {}
            
        # 0. 正则预处理
        effective_tags = self._preprocess_tags(tags)
        
        # 1. 首次运行时加载 DB 缓存
        await self._ensure_cache_loaded()
        
        # 2. 检查缺失 (使用预处理后的标签)
        uncached = [t for t in effective_tags if t not in self._cache]
        
        if uncached:
            # 去重
            uncached = list(set(uncached))
            logger.info(f"发现 {len(uncached)} 个新 Tag，开始 AI 处理 ({self.batch_size}/批)...")
            await self._batch_process(uncached)
        
        # 3. 应用结果 (失败 tag 不在 cache 中, 运行时降级为原值)
        return self._apply_cache_results(tags, effective_tags)

    async def reprocess_tags(self, tags: list[str]) -> bool:
        """
        强制重新 AI 处理指定标签 (绕过内存/语义上的“已处理”).
        返回 True 表示全部成功并已写入 cache.
        """
        if not self.enabled:
            return False
        if not tags:
            return True

        effective_tags = self._preprocess_tags(tags)
        await self._ensure_cache_loaded()

        targets = list(dict.fromkeys(effective_tags))
        for t in targets:
            self._cache.pop(t, None)

        logger.info(f"强制重试 AI 处理 {len(targets)} 个标签...")
        ok = await self._batch_process(targets)
        # 全部目标均已成功写入 cache 才算成功
        return ok and all(t in self._cache for t in targets)

    @retry_async(max_retries=3, delay=2.0)
    async def _call_api(self, prompt: str) -> str:
        """调用AI API（流式，防止超时）"""
        response = await self.client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": "你是一个Pixiv插画标签数据处理专家，只输出标准JSON。"},
                {"role": "user", "content": prompt}
            ],
            temperature=0.1,
            response_format={"type": "json_object"},
            stream=True  # 启用流式
        )
        
        collected_content = []
        async for chunk in response:
            if chunk.choices and chunk.choices[0].delta.content:
                collected_content.append(chunk.choices[0].delta.content)

        content = "".join(collected_content)
        if not content.strip():
            raise ValueError("AI 返回空内容")
        return content

    async def _batch_process(self, tags: list[str]) -> bool:
        """并发批量处理Tag. 返回是否全部批次成功."""
        if not tags:
            return True
        
        batches = []
        for i in range(0, len(tags), self.batch_size):
            batches.append(tags[i:i + self.batch_size])
            
        logger.info(f"队列共 {len(batches)} 个批次，并发数: {self.concurrency}")
        
        semaphore = asyncio.Semaphore(self.concurrency)
        results: list[bool] = []
        
        async def _bounded_Process(batch):
            async with semaphore:
                ok = await self._process_single_batch(batch)
                results.append(ok)
                
        tasks = [_bounded_Process(b) for b in batches]
        await asyncio.gather(*tasks)
        return all(results) if results else True

    async def _process_single_batch(self, tags: list[str]) -> bool:
        """处理单批次并持久化. 成功返回 True; 失败不写入 cache."""
        prompt = self._build_prompt(tags)
        
        last_error = None
        max_logic_retries = 5
        
        for attempt in range(max_logic_retries):
            try:
                logger.info(f"🤖 正在请求 AI清洗 (尝试 {attempt+1}/{max_logic_retries})...")
                content = await self._call_api(prompt)
                
                # 清洗 Markdown
                content = content.strip()
                if content.startswith("```json"): content = content[7:]
                if content.startswith("```"): content = content[3:]
                if content.endswith("```"): content = content[:-3]
                
                # 直接解析，失败则触发外层重试
                result = json.loads(content)
    
                meaningless = set(result.get("meaningless", []))
                synonyms = result.get("synonyms", {})
                
                # 构造更新映射
                cache_update = {}
                mapping_update = {} 
                
                for tag in tags:
                    if tag in meaningless:
                        cache_update[tag] = None
                    elif tag in synonyms:
                        cleaned = synonyms[tag]
                        cache_update[tag] = cleaned
                        mapping_update[tag] = cleaned
                    else:
                        cache_update[tag] = tag
                
                # 更新内存缓存
                self._cache.update(cache_update)
                
                # 持久化到 DB
                await db.update_ai_cache(cache_update) 
                if mapping_update:
                    await db.update_tag_mapping_stats(mapping_update)
                    
                # 美化日志输出
                logger.info(f"✨ AI Batch 完成 (本批 {len(tags)} 个)")
                
                if meaningless:
                    logger.info(f"   🗑️ 过滤 {len(meaningless)} 个标签")
                if synonyms:
                    logger.info(f"   🔄 归类 {len(synonyms)} 个标签")
                
                return True

            except Exception as e:
                last_error = e
                # 简化报错日志
                error_msg = str(e)
                if "524" in error_msg:
                    logger.warning(f"AI API超时 (524)")
                else:
                    logger.warning(f"AI处理批次失败 (尝试 {attempt+1}): {e}")
                
                # 如果不是最后一次尝试，等待后重试
                if attempt < max_logic_retries - 1:
                    await asyncio.sleep(2)
                    continue

        # 如果所有重试都失败，执行最终的错误处理
        if last_error:
            logger.error(f"❌ AI Batch 最终失败: {last_error}")
            
            # 记录错误到数据库 (仅非超时错误)
            if "524" not in str(last_error):
                try:
                    err_id = await db.add_ai_error(tags, str(last_error))
                    self.occurred_errors.append(err_id)
                except Exception as db_e:
                    logger.error(f"记录错误日志失败: {db_e}")
            
            # 失败不写 cache, 以便后续/重试可再次请求 AI
        return False

from utils import TAG_TRANSLATIONS


def _build_ai_prompt(tags: list[str]) -> str:
    """构建优化后的 AI 提示词"""
    
    # 构建完整的标准词库（供 AI 参考）
    canonical_dict = {}
    for canonical, query_str in TAG_TRANSLATIONS.items():
        aliases = [p.strip().replace("(", "").replace(")", "") for p in query_str.split(" OR ")]
        for alias in aliases:
            if alias != canonical:
                canonical_dict[alias] = canonical
    
    # 格式化词库为紧凑字符串
    dict_entries = [f"{k} → {v}" for k, v in canonical_dict.items()]
    dict_text = " | ".join(dict_entries)

    return f"""# Pixiv Tag 清洗任务

## 目标
将原始 Pixiv 标签清洗为适合用户画像分析的标准化标签。

## 已有标准词库（必须优先使用）
以下是系统已定义的标准映射，如果输入包含这些标签，**必须**使用对应的标准形式：
{dict_text}

## 规则

### 1. 过滤（放入 meaningless）
删除以下类型的标签：
- **平台/统计类**: original, pixiv, users入り, bookmark, 収藏, 仕事絵
- **创作过程类**: 落書き, 練習, WIP, sketch, doodle
- **宣传/请求类**: お仕事募集中, commission, request, follow me
- **年份/日期类**: 2024, 2023, 新年, クリスマス
- **纯数字或无意义字符** (⚠️注意：如果数字代表角色名如"37"或作品名如"1999"等，请**保留**，不要过滤)

### 2. 归一化（放入 synonyms）
将同义词映射到 **Danbooru 风格** 标准标签：
- 格式: 全小写 + 下划线 (snake_case)
- **多语言合并**: 必须将中文/日文的作品名、角色名合并为统一的英文/罗马音标签
- **优先使用上方词库中的标准形式**，如果词库中没有，再自行判断

### 3. 保留（不出现在输出中）
仅保留无法归类或无需标准化的描述性标签（且不属于同义词）：
- 独特的风格描述
- 具体的场景细节
- **注意**：如果一个标签可以被归一化（如作品名），必须归一化，**不要**保留原样！

## 输入
```json
{json.dumps(tags, ensure_ascii=False)}
```

## 输出要求
严格 JSON，无 Markdown 包裹：
```json
{{
  "meaningless": ["tag1", "tag2"],
  "synonyms": {{"原tag": "标准tag"}}
}}
```
如果某类别为空，使用空数组/对象。"""


# 将 _build_prompt 方法绑定到 AITagProcessor 类
AITagProcessor._build_prompt = lambda self, tags: _build_ai_prompt(tags)


class XPProfiler:
    """XP画像构建器"""
    
    def __init__(
        self,
        client: PixivClient,
        stop_words: Optional[list[str]] = None,
        discovery_rate: float = 0.1,
        time_decay_days: int = 180,
        ai_config: Optional[dict] = None,
        saturation_threshold: float = 0.5
    ):
        self.client = client
        self.stop_words = set(stop_words or [])
        self.discovery_rate = discovery_rate
        self.time_decay_days = time_decay_days
        self.ai_processor = AITagProcessor(ai_config or {})
        self.saturation_threshold = saturation_threshold  # 高频 Tag 饱和度阈值
        self._blocked_artist_ids: set[int] = set()  # 初始化，由 load_blacklist 填充
        
        # 添加默认停用词（归一化为小写）
        # Pixiv 常见无意义标签
        default_stop_words = [
            # 通用描述
            "original", "オリジナル", "manga", "漫画", "pixiv",
            "illustration", "イラスト", "練習", "practice",
            "落書き", "doodle", "sketch", "スケッチ",
            "drawing", "art", "artwork", "fanart", "ファンアート",
            "digital", "デジタル", "アナログ", "analog",
            
            # 分级标签
            "R-18", "R-18G", "R18", "NSFW", "SFW", "safe",
            
            # 数字/编号类
            "1000users入り", "500users入り", "100users入り", "50users入り",
            "5000users入り", "10000users入り", "users入り",
            "1000bookmarks", "500bookmarks", "100bookmarks",
            
            # 活动/比赛标签
            "コンテスト", "contest", "企画", "project",
            "お題", "リクエスト", "request", "commission",
            "落書き集", "まとめ", "詰め合わせ", "log",
            
            # 平台标签
            "twitter", "fanbox", "patreon", "skeb",
            "pixivfanbox", "fantia",
            
            # 通用形容词
            "cute", "kawaii", "かわいい", "可愛い",
            "beautiful", "綺麗", "pretty", "sexy",
            "cool", "かっこいい", "カッコイイ",
            
            # 其他无意义
            "girls", "girl", "boy", "boys", "woman", "man",
            "female", "male", "solo", "1girl", "1boy",
            "2girls", "2boys", "multiple_girls", "multiple_boys",
            "背景", "background", "風景", "landscape",
            "創作", "オリキャラ", "original_character", "oc",
            "うちの子", "看板娘", "版権", "二次創作",
            "仕事絵", "お仕事", "work",
        ]
        for word in default_stop_words:
            self.stop_words.add(word.lower().replace(" ", "_"))
            
    async def load_blacklist(self):
        """从数据库加载黑名单 (仅包括用户手动屏蔽的)"""
        try:
            # 1. 仅加载手动屏蔽的标签
            # 用户明确要求：没确认就不屏蔽，因此不加载 high-dislike counts
            blocked_tags = await db.get_blocked_tags()
            for tag in blocked_tags:
                self.stop_words.add(self._normalize_tag(tag))
            
            # 2. 加载屏蔽的画师 ID
            blocked_artists = await db.get_blocked_artists()
            self._blocked_artist_ids = {artist_id for artist_id, _ in blocked_artists}
            
            logger.info(f"已加载黑名单: {len(blocked_tags)} 个手动屏蔽Tag + {len(blocked_artists)} 个屏蔽画师")
        except Exception as e:
            logger.error(f"加载黑名单失败: {e}")
            self._blocked_artist_ids = set()

            logger.error(f"加载黑名单失败: {e}")
            self._blocked_artist_ids = set()
    
    async def build_profile(
        self,
        user_id: int,
        scan_limit: int = 500,
        include_private: bool = False
    ) -> dict[str, float]:
        """
        扫描收藏，构建XP权重字典
        
        Args:
            user_id: 目标用户ID
            scan_limit: 扫描收藏数量
            include_private: 包含私密收藏
        
        Returns:
            {tag: weight} 权重字典
        """
        # 获取收藏
        await self.load_blacklist()  # 确保加载最新黑名单
        
        # 1. 加载本地缓存 ID
        cached_rows = await db.get_xp_bookmarks(user_id)
        cached_ids = {row['illust_id'] for row in cached_rows}
        
        # 2. 检查同步状态
        sync_key = f"sync_completed_{user_id}"
        is_completed = await db.get_state(sync_key) == "true"
        
        # 定义 Cursor Key 生成器
        def get_cursor_key(is_private):
            suffix = "private" if is_private else "public"
            return f"resume_cursor_{user_id}_{suffix}"

        # 通用获取逻辑封装
        async def fetch_segment(is_private):
            cursor_key = get_cursor_key(is_private)
            saved_cursor = await db.get_state(cursor_key)
            
            # 策略判断
            if is_completed:
                mode = "update" # 增量更新
                stop_ids = cached_ids
                skip_ids = None
                start_url = None
                do_tail_resume = False
            elif saved_cursor:
                mode = "jump"   # 高效跳转
                stop_ids = cached_ids
                skip_ids = None
                start_url = None
                do_tail_resume = True
            else:
                mode = "slow"   # 慢速扫描 (首次或丢失游标)
                stop_ids = None
                skip_ids = cached_ids
                start_url = None
                do_tail_resume = False
                
            desc = "私密" if is_private else "公开"
            logger.info(f"[{desc}] 模式: {mode}, 缓存: {len(cached_ids)}")

            # 回调工厂: 区分 Head 更新还是 Tail 更新
            def make_callback(update_cursor_key=None):
                async def _cb(items, next_url):
                    await db.save_xp_bookmarks(user_id, items)
                    if update_cursor_key and next_url:
                        await db.set_state(update_cursor_key, next_url)
                return _cb

            fetched = []
            
            # 1. 头部扫描 (填补最新的 Gap)
            # 即使在 update 模式，也是跑这个。
            # 如果是 jump 模式，这里负责只抓最新的，不要覆盖 saved_cursor
            logger.info(f"[{desc}] 正在扫描头部...")
            head_items = await self.client.get_bookmarks(
                user_id,
                limit=scan_limit,
                private=is_private,
                stop_ids=stop_ids,
                skip_ids=skip_ids,
                start_url=start_url,
                on_batch=make_callback(update_cursor_key=cursor_key if mode == "slow" else None)
            )
            fetched.extend(head_items)
            
            # 2. 尾部跳转 (仅 Jump 模式)
            if do_tail_resume:
                logger.info(f"[{desc}] ⚡ 触发高效断点续传，直接跳转到: {saved_cursor[:60]}...")
                # 这里必须从 saved_cursor 开始，并且要更新 cursor_key (推进进度)
                # 依然传入 stop_ids=cached_ids，万一 хво巴也接上了呢
                tail_items = await self.client.get_bookmarks(
                    user_id,
                    limit=scan_limit, # 剩余额度？暂不精确控制
                    private=is_private,
                    start_url=saved_cursor,
                    stop_ids=cached_ids,
                    on_batch=make_callback(update_cursor_key=cursor_key)
                )
                fetched.extend(tail_items)
                
            return fetched

        # 4. 执行获取
        bookmarks = await fetch_segment(False)
        
        if include_private and self.client._logged_in:
            private_bookmarks = await fetch_segment(True)
            bookmarks = bookmarks + private_bookmarks
        
        # 5. 标记同步完成
        if not is_completed:
            await db.set_state(sync_key, "true")
            logger.info("✅ 全量同步完成，标记为 [已完成]")
            # 清理游标？可选。留着也没事，下次 is_completed=True 会忽略它。
        
        # 6. 重新构建全量列表
        cached_rows = await db.get_xp_bookmarks(user_id)
        
        analyzed_illusts = []
        for row in cached_rows:
            # 数据库里存的时间可能是字符串，需转换
            cdate = row['illust_create_date']
            if isinstance(cdate, str):
                try:
                    cdate = datetime.fromisoformat(cdate)
                except:
                    cdate = datetime.now()
            
            # 预处理标签：去除 users入り 后缀等
            raw_tags = json.loads(row['tags'])
            cleaned_tags = [self._normalize_tag(t) for t in raw_tags]
            # 过滤空标签并去重（保持顺序）
            cleaned_tags = list(dict.fromkeys(t for t in cleaned_tags if t))
            
            analyzed_illusts.append(Illust(
                id=row['illust_id'],
                title="Cached",
                user_id=user_id,
                user_name="",
                tags=cleaned_tags,
                bookmark_count=0,
                view_count=0,
                page_count=1,
                image_urls=[],
                is_r18=False,
                ai_type=0,
                create_date=cdate
            ))
            
        bookmarks = analyzed_illusts
        logger.info(f"XP分析数据源: {len(bookmarks)} 个收藏作品 (含本地历史)")
        
        # 统计Tag出现次数和时间 (存储 illust_id 用于正确计算 DF)
        # 支持权重系数 (liked items = 0.5x，因为 apply_feedback 已给过 1.0x)
        tag_occurrences: dict[str, list[tuple[int, datetime, float]]] = defaultdict(list)
        
        # 获取已点赞的作品ID (避免双倍计分)
        liked_ids = await db.get_liked_illusts()
        
        for illust in bookmarks:
            # 已点赞的作品给 0.5x 权重 (与反馈的 1.0 合计 = 1.5x)
            weight_mult = 0.5 if illust.id in liked_ids else 1.0
            
            for tag in illust.tags:
                normalized = self._normalize_tag(tag)
                if normalized and normalized not in self.stop_words:
                    tag_occurrences[normalized].append((illust.id, illust.create_date, weight_mult))
        
        # AI 处理：过滤无意义tag和归类同义tag
        if self.ai_processor.enabled:
            all_tags = list(tag_occurrences.keys())
            valid_tags, synonym_map = await self.ai_processor.process_tags(all_tags)
            
            # 合并同义tag的统计
            new_occurrences = defaultdict(list)
            for tag, occs in tag_occurrences.items():
                if tag in synonym_map:
                    # 映射到规范化tag
                    new_occurrences[synonym_map[tag]].extend(occs)
                elif tag in valid_tags:
                    new_occurrences[tag].extend(occs)
                # else: 被过滤的无意义tag，丢弃
            
            tag_occurrences = new_occurrences
            logger.info(f"AI处理后剩余 {len(tag_occurrences)} 个有效Tag")
        
        # 计算权重
        total_docs = len(bookmarks)
        profile = {}
        tag_df = {}  # 用于 PMI 计算
        
        # 先计算所有 Tag 的 DF 并检测饱和度
        saturated_tags = []
        for tag, occurrences in tag_occurrences.items():
            unique_illusts = set(item[0] for item in occurrences)
            df = len(unique_illusts)
            tag_df[tag] = df
            
            # 饱和度检测：高频 Tag 自动加入停用词
            saturation = df / total_docs if total_docs > 0 else 0
            if saturation > self.saturation_threshold:
                saturated_tags.append((tag, saturation))
                self.stop_words.add(tag)
        
        if saturated_tags:
            logger.info(f"🎯 饱和度检测：{len(saturated_tags)} 个高频 Tag 自动加入停用词")
            for tag, sat in saturated_tags[:5]:  # 只显示前5个
                logger.info(f"   - {tag}: {sat:.1%}")
        
        for tag, occurrences in tag_occurrences.items():
            if tag in self.stop_words:
                continue  # 跳过饱和 Tag
                
            unique_illusts = set(item[0] for item in occurrences)
            dates = [item[1] for item in occurrences]
            weights = [item[2] for item in occurrences]  # 权重系数
            
            weight = self._calculate_weight(
                term_frequency=len(occurrences),
                document_frequency=len(unique_illusts),
                total_documents=total_docs,
                occurrence_dates=dates,
                weight_multipliers=weights  # 传入权重系数
            )
            profile[tag] = weight
        
        # 计算Tag组合权重 (Co-occurrence)
        pair_counts = Counter()
        for illust in bookmarks:
            # 获取该作品所有有效的 normalized tags
            valid_tags = []
            for tag in illust.tags:
                norm = self._normalize_tag(tag)
                if norm and norm not in self.stop_words:
                    valid_tags.append(norm)
            
            # 统计组合 (只统计高频Tag的组合以减少噪音)
            if len(valid_tags) >= 2:
                # 排序以保证 (A, B) 和 (B, A) 视为同一个组合
                valid_tags.sort()
                # 生成所有两两组合
                for t1, t2 in itertools.combinations(valid_tags, 2):
                    # 仅当两个Tag都在Profile中有一定权重时才统计（例如 Top 50）
                    # 这里简化为：所有组合都统计，但后续根据频率筛选
                    pair_counts[(t1, t2)] += 1
        
        # 保存热门组合 (使用 PMI 优化权重)
        pairs_to_save = []
        for (t1, t2), count in pair_counts.most_common(100):  # 扩大候选池
            # 计算 PMI = log(P(t1,t2) / (P(t1) * P(t2)))
            p_t1 = tag_df.get(t1, 1) / total_docs if total_docs > 0 else 0
            p_t2 = tag_df.get(t2, 1) / total_docs if total_docs > 0 else 0
            p_joint = count / total_docs if total_docs > 0 else 0
            
            # 防止除零，使用平滑
            pmi = math.log(p_joint / (p_t1 * p_t2 + 1e-10) + 1e-10)
            
            # 结合 PMI 和原权重，PMI 为负表示反相关，过滤掉
            if pmi > 0:
                weight = pmi * (profile.get(t1, 0) + profile.get(t2, 0))
                pairs_to_save.append((t1, t2, weight))
        
        # 只保留 Top 50
        pairs_to_save = sorted(pairs_to_save, key=lambda x: x[2], reverse=True)[:50]
            
        await db.update_xp_tag_pairs(pairs_to_save)
        
        # ============ 冷启动处理：收藏少时注入热门 Tag 弱先验 ============
        cold_start_threshold = 50  # 收藏少于此数时触发冷启动
        if len(bookmarks) < cold_start_threshold:
            logger.info(f"🧊 检测到冷启动场景 (收藏: {len(bookmarks)} < {cold_start_threshold})")
            try:
                popular_tags = await db.get_popular_tags(20)
                injected_count = 0
                for tag, freq in popular_tags:
                    normalized_tag = self._normalize_tag(tag)
                    if normalized_tag and normalized_tag not in profile and normalized_tag not in self.stop_words:
                        # 弱先验权重：频率 * 0.1（不会压过真实收藏）
                        prior_weight = freq * 0.1
                        profile[normalized_tag] = prior_weight
                        injected_count += 1
                if injected_count > 0:
                    logger.info(f"   注入 {injected_count} 个热门 Tag 作为弱先验")
            except Exception as e:
                logger.warning(f"冷启动注入失败: {e}")
        
        # 保存到数据库 (现有代码)
        await db.update_xp_profile(profile)
        
        logger.info(f"构建XP画像完成，共 {len(profile)} 个Tag，{len(pairs_to_save)} 个热门组合")
        return profile
    
    # 预编译正则：去除 users入り 后缀
    _pattern_users = re.compile(r"^(.*?)\d+users入り$", re.IGNORECASE)
    
    def _normalize_tag(self, tag: str) -> str:
        """
        Tag归一化
        1. 去除 xxxusers入り 后缀
        2. 统一转小写
        3. 去除空格
        4. 别名映射
        """
        tag = tag.strip()
        
        # 去除 users入り 后缀
        match = self._pattern_users.match(tag)
        if match:
            prefix = match.group(1)
            if prefix:  # 如果前缀非空，使用前缀
                tag = prefix
        
        tag = tag.lower()
        
        # 检查别名映射
        for alias, canonical in TAG_ALIASES.items():
            if tag == alias.lower():
                return canonical
        
        # 替换空格为下划线
        tag = tag.replace(" ", "_")
        
        return tag
    
    def _calculate_weight(
        self,
        term_frequency: int,
        document_frequency: int,
        total_documents: int,
        occurrence_dates: list[datetime],
        weight_multipliers: list[float] = None
    ) -> float:
        """
        权重计算（优化后的 TF-IDF + 时间衰减）
        
        weight = weighted_TF × IDF
        - weighted_TF = Σ(time_decay × weight_mult)
        - IDF = log(N / (df + 1)) + 1  (带平滑的标准IDF)
        """
        now = datetime.now(occurrence_dates[0].tzinfo if occurrence_dates else None)
        
        # 0. 高频饱和度过滤 (如果超过阈值如 50%，视为无意义停用词)
        df_ratio = document_frequency / total_documents if total_documents > 0 else 0
        if df_ratio > self.saturation_threshold:
            return 0.0

        # 计算带时间衰减的 TF (含权重系数)
        weighted_tf = 0
        for i, date in enumerate(occurrence_dates):
            days_ago = (now - date).days
            # 确保 days_ago 不为负
            days_ago = max(0, days_ago)
            decay = math.exp(-days_ago / self.time_decay_days)
            # 应用权重系数 (liked items = 0.5x)
            weighted_tf += decay * weight_multipliers[i]
        
        # 1. 对 TF 应用对数抑制 (防止数量堆积导致的线性无限增长)
        # log10(1 + 10) = 1.04
        # log10(1 + 100) = 2.0
        # log10(1 + 500) = 2.7
        # 即使有 1000 个收藏，权重也只比 100 个多 35%，而不是 1000%
        if weighted_tf > 0:
            weighted_tf = math.log10(1 + weighted_tf)
        
        # 2. 标准 IDF (带平滑防止除零)
        idf = math.log(total_documents / (document_frequency + 1)) + 1
        
        return weighted_tf * idf
    
    async def get_top_tags(self, n: int = 20) -> list[tuple[str, float]]:
        """获取权重最高的N个Tag"""
        profile = await db.get_xp_profile()
        sorted_tags = sorted(profile.items(), key=lambda x: x[1], reverse=True)
        return sorted_tags[:n]
    
    async def apply_feedback(self, illust: Illust, action: str, config: dict):
        """
        应用用户反馈调整权重
        
        Args:
            illust: 作品对象
            action: 'like' | 'dislike'
            config: 反馈配置
        """
        like_boost = config.get("like_boost", 0.5)
        dislike_penalty = config.get("dislike_penalty", 0.3)
        dislike_threshold = config.get("dislike_threshold", 3)
        
        # 获取当前画像用于分级惩罚
        profile = await db.get_xp_profile()
        max_weight = max(profile.values()) if profile else 1
        
        suggested_block_tag = None  # 记录建议屏蔽的 Tag
        
        for tag in illust.tags:
            normalized = self._normalize_tag(tag)
            if not normalized or normalized in self.stop_words:
                continue
            
            if action == "like":
                await db.adjust_tag_weight(normalized, like_boost)
                logger.debug(f"Tag '{normalized}' 权重 +{like_boost}")
            
            elif action == "dislike":
                # 分级惩罚：高权重 Tag 减少惩罚力度（可能是用户核心偏好）
                current_weight = profile.get(normalized, 0)
                weight_ratio = current_weight / max_weight if max_weight > 0 else 0
                # 高权重 Tag 最多减半惩罚
                adjusted_penalty = dislike_penalty * (1 - weight_ratio * 0.5)
                
                await db.adjust_tag_weight(normalized, -adjusted_penalty)
                
                # 同时更新负向画像（用于主动排斥相似作品）
                await db.adjust_negative_weight(normalized, adjusted_penalty)
                
                count = await db.increment_tag_dislike(normalized)
                
                # 用户要求：仅确认一次，没确认就算了
                if count == dislike_threshold:
                    logger.info(f"Tag '{normalized}' 累计否认 {count} 次，建议加入黑名单")
                    # 只记录第一个达到阈值的 Tag
                    if suggested_block_tag is None:
                        suggested_block_tag = normalized
                elif count > dislike_threshold:
                    logger.debug(f"Tag '{normalized}' 累计否认 {count} 次 (已提示过)")
                else:
                    logger.debug(f"Tag '{normalized}' 权重 -{adjusted_penalty:.2f} (分级惩罚)")
        
        # 画师权重关联 (Artist Weight) - 只执行一次
        if illust.user_id:
            try:
                artist_delta = 1.0 if action == "like" else -1.0
                await db.update_artist_score(illust.user_id, artist_delta)
                logger.debug(f"画师 {illust.user_id} ({illust.user_name}) 权重 {artist_delta:+.1f}")
            except Exception as e:
                logger.error(f"更新画师权重失败: {e}")

        # 记录反馈 - 只执行一次
        await db.record_feedback(illust.id, action)
        
        return suggested_block_tag
