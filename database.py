"""
SQLite 数据层
"""
import json
import aiosqlite
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional

DB_PATH = Path(__file__).parent / "data" / "pixiv_xp.db"


async def init_db():
    """初始化数据库表结构"""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    
    async with aiosqlite.connect(DB_PATH) as db:
        # ============ 简易迁移逻辑 ============
        # 检查 xp_bookmarks 表是否包含 user_id 列 (旧版没有)
        try:
             await db.execute("SELECT user_id FROM xp_bookmarks LIMIT 0")
        except Exception:
             await db.execute("DROP TABLE IF EXISTS xp_bookmarks")
             await db.commit()
             await db.execute("DROP TABLE IF EXISTS xp_profile")
             await db.execute("DROP TABLE IF EXISTS xp_tag_pairs")
             await db.commit()
        
        # 检查 illust_cache 表是否包含 user_id 列 (v2 新增)
        try:
             await db.execute("SELECT user_id FROM illust_cache LIMIT 0")
        except Exception:
             # 旧表只有 tags，删除重建
             await db.execute("DROP TABLE IF EXISTS illust_cache")
             await db.commit()
        
        await db.executescript("""
            -- 推送历史
            CREATE TABLE IF NOT EXISTS push_history (
                illust_id INTEGER PRIMARY KEY,
                pushed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                source TEXT  -- 'search' | 'subscription'
            );
            
            -- XP画像
            CREATE TABLE IF NOT EXISTS xp_profile (
                tag TEXT PRIMARY KEY,
                weight REAL DEFAULT 0,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            -- XP Tag组合 (新)
            CREATE TABLE IF NOT EXISTS xp_tag_pairs (
                tag1 TEXT,
                tag2 TEXT,
                weight REAL,
                PRIMARY KEY (tag1, tag2)
            );
            
            -- 用户反馈
            CREATE TABLE IF NOT EXISTS feedback (
                illust_id INTEGER PRIMARY KEY,
                action TEXT,  -- 'like' | 'dislike' | 'skip'
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            -- 收藏同步记录
            CREATE TABLE IF NOT EXISTS bookmarks (
                illust_id INTEGER PRIMARY KEY,
                scanned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            -- 临时黑名单(由反馈生成)
            CREATE TABLE IF NOT EXISTS tag_blacklist (
                tag TEXT PRIMARY KEY,
                dislike_count INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            -- 作品缓存(用于反馈处理) - v2: 增加画师信息
            CREATE TABLE IF NOT EXISTS illust_cache (
                illust_id INTEGER PRIMARY KEY,
                tags TEXT,  -- JSON数组
                user_id INTEGER,      -- 画师ID
                user_name TEXT,       -- 画师名
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            -- AI 处理错误日志
            CREATE TABLE IF NOT EXISTS ai_error_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tags_content TEXT,  -- JSON数组，原始Tags
                error_msg TEXT,
                status TEXT DEFAULT 'pending',  -- pending, resolved, ignored
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            -- 用户XP分析用的收藏数据缓存
            CREATE TABLE IF NOT EXISTS xp_bookmarks (
                illust_id INTEGER PRIMARY KEY,
                user_id INTEGER,       -- 收藏者的ID
                tags TEXT,             -- JSON encoded tags
                illust_create_date TIMESTAMP, -- 作品创建时间
                scanned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            -- 系统状态表 (用于记录同步状态等)
            CREATE TABLE IF NOT EXISTS system_state (
                key TEXT PRIMARY KEY,
                value TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            -- 标签映射统计表 (用于反查最佳搜索词)
            CREATE TABLE IF NOT EXISTS tag_mapping_stats (
                normalized_tag TEXT,
                original_tag TEXT,
                frequency INTEGER DEFAULT 0,
                PRIMARY KEY (normalized_tag, original_tag)
            );
            
            -- AI 处理结果缓存 (Tag -> CleanedTag/NULL)
            CREATE TABLE IF NOT EXISTS ai_tag_cache (
                original_tag TEXT PRIMARY KEY,
                cleaned_tag TEXT,  -- NULL 表示被过滤(meaningless)
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            -- MAB 策略统计表
            CREATE TABLE IF NOT EXISTS strategy_stats (
                strategy TEXT PRIMARY KEY,
                success_count INTEGER DEFAULT 0,
                total_count INTEGER DEFAULT 0,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            -- Bot 快速屏蔽标签 (持久化)
            CREATE TABLE IF NOT EXISTS blocked_tags (
                tag TEXT PRIMARY KEY,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            -- Bot 快速屏蔽画师 (持久化)
            CREATE TABLE IF NOT EXISTS blocked_artists (
                artist_id INTEGER PRIMARY KEY,
                artist_name TEXT,  -- 可选，用于显示
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            -- 画师权重档案 (用于 Related Works 策略)
            CREATE TABLE IF NOT EXISTS artist_profile (
                artist_id INTEGER PRIMARY KEY,
                score FLOAT DEFAULT 0,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            -- 负向画像 (用于记录负反馈，主动排斥相似作品)
            CREATE TABLE IF NOT EXISTS negative_profile (
                tag TEXT PRIMARY KEY,
                weight REAL DEFAULT 0,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            -- 批量消息与作品映射 (用于 Telegraph 批量模式)
            CREATE TABLE IF NOT EXISTS batch_message_map (
                message_id INTEGER,
                chat_id TEXT,
                illust_index INTEGER,  -- 作品在批次中的编号 (1-based)
                illust_id INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (message_id, chat_id, illust_index)
            );
            
            -- 作品 Embedding 缓存 (用于语义匹配)
            CREATE TABLE IF NOT EXISTS illust_embeddings (
                illust_id INTEGER PRIMARY KEY,
                embedding TEXT,  -- JSON 序列化的向量
                model TEXT,      -- 使用的模型名
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            -- 用户画像 Embedding (低频更新)
            CREATE TABLE IF NOT EXISTS user_embedding (
                user_id INTEGER PRIMARY KEY,
                embedding TEXT,  -- JSON 序列化的向量
                model TEXT,
                profile_hash TEXT,  -- XP Profile 的哈希，用于判断是否需要更新
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        await db.commit()
        
        # === 迁移：为 illust_cache 添加 source 和 chain 列 ===
        try:
            await db.execute("ALTER TABLE illust_cache ADD COLUMN source TEXT DEFAULT 'xp_search'")
            await db.commit()
            logger.info("迁移：illust_cache 添加 source 列")
        except:
            pass  # 列已存在
        
        try:
            await db.execute("ALTER TABLE illust_cache ADD COLUMN chain_depth INTEGER DEFAULT 0")
            await db.execute("ALTER TABLE illust_cache ADD COLUMN chain_parent_id INTEGER")
            await db.execute("ALTER TABLE illust_cache ADD COLUMN chain_msg_id INTEGER")
            await db.commit()
            logger.info("迁移：illust_cache 添加 chain 列")
        except:
            pass  # 列已存在


async def cleanup_old_records(days: int = 180):
    """
    清理过期数据，防止数据库无限增长
    
    Args:
        days: 保留最近多少天的记录 (默认 180 天)
    """
    import logging
    logger = logging.getLogger(__name__)
    
    cutoff_date = datetime.now() - timedelta(days=days)
    cutoff_str = cutoff_date.strftime("%Y-%m-%d %H:%M:%S")
    
    async with aiosqlite.connect(DB_PATH) as db:
        # 清理推送历史
        cursor = await db.execute(
            "DELETE FROM push_history WHERE pushed_at < ?", (cutoff_str,)
        )
        push_deleted = cursor.rowcount
        
        # 清理作品缓存
        cursor = await db.execute(
            "DELETE FROM illust_cache WHERE created_at < ?", (cutoff_str,)
        )
        cache_deleted = cursor.rowcount
        
        # 清理收藏同步记录
        cursor = await db.execute(
            "DELETE FROM xp_bookmarks WHERE scanned_at < ?", (cutoff_str,)
        )
        bookmarks_deleted = cursor.rowcount
        
        await db.commit()
        
        # Vacuum 数据库释放空间
        await db.execute("VACUUM")
        
        logger.info(
            f"🧹 数据库清理完成: 删除 {push_deleted} 条推送历史, "
            f"{cache_deleted} 条缓存, {bookmarks_deleted} 条收藏记录 "
            f"(保留最近 {days} 天)"
        )

async def get_ai_cache_map() -> dict[str, str | None]:
    """获取所有 AI 处理缓存"""
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT original_tag, cleaned_tag FROM ai_tag_cache")
        rows = await cursor.fetchall()
        return {row[0]: row[1] for row in rows}

async def update_ai_cache(cache_data: dict[str, str | None]):
    """批量更新 AI 处理缓存"""
    if not cache_data:
        return
        
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executemany(
            "INSERT OR REPLACE INTO ai_tag_cache (original_tag, cleaned_tag) VALUES (?, ?)",
            [(k, v) for k, v in cache_data.items()]
        )
        await db.commit()

async def update_tag_mapping_stats(mappings: dict[str, str]):
    """
    更新标签映射统计
    mappings: {original_tag: normalized_tag}
    """
    async with aiosqlite.connect(DB_PATH) as db:
        for original, normalized in mappings.items():
            await db.execute("""
                INSERT INTO tag_mapping_stats (normalized_tag, original_tag, frequency)
                VALUES (?, ?, 1)
                ON CONFLICT(normalized_tag, original_tag) 
                DO UPDATE SET frequency = frequency + 1
            """, (normalized, original))
        await db.commit()

async def get_best_search_tag(normalized_tag: str) -> str:
    """
    获取某标准化标签对应的最高频原始标签
    """
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("""
            SELECT original_tag FROM tag_mapping_stats
            WHERE normalized_tag = ?
            ORDER BY frequency DESC
            LIMIT 1
        """, (normalized_tag,))
        row = await cursor.fetchone()
        if row:
            return row[0]
        return normalized_tag

async def get_db():
    """获取数据库连接"""
    return await aiosqlite.connect(DB_PATH)


# ============ 推送历史 ============
async def is_pushed(illust_id: int) -> bool:
    """检查作品是否已推送"""
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT 1 FROM push_history WHERE illust_id = ?", (illust_id,)
        )
        return await cursor.fetchone() is not None


async def get_pushed_ids_batch(illust_ids: list[int]) -> set[int]:
    """
    批量查询已推送的作品 ID 集合 (性能优化)
    
    将 O(n) 次数据库查询优化为 O(1) 次查询
    """
    if not illust_ids:
        return set()
    
    async with aiosqlite.connect(DB_PATH) as db:
        # 使用 IN 查询批量获取
        placeholders = ",".join("?" * len(illust_ids))
        cursor = await db.execute(
            f"SELECT illust_id FROM push_history WHERE illust_id IN ({placeholders})",
            illust_ids
        )
        rows = await cursor.fetchall()
        return {row[0] for row in rows}


async def get_bookmarked_ids_batch(illust_ids: list[int], user_id: int) -> set[int]:
    """
    批量查询已收藏的作品 ID 集合 (基于 xp_bookmarks 本地缓存)
    """
    if not illust_ids or user_id <= 0:
        return set()

    async with aiosqlite.connect(DB_PATH) as db:
        placeholders = ",".join("?" * len(illust_ids))
        params = [user_id, *illust_ids]
        cursor = await db.execute(
            f"SELECT illust_id FROM xp_bookmarks WHERE user_id = ? AND illust_id IN ({placeholders})",
            params
        )
        rows = await cursor.fetchall()
        return {row[0] for row in rows}


async def mark_pushed(illust_id: int, source: str):
    """记录推送"""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR REPLACE INTO push_history (illust_id, source) VALUES (?, ?)",
            (illust_id, source)
        )
        await db.commit()

async def get_push_source(illust_id: int) -> Optional[str]:
    """获取推送来源"""
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT source FROM push_history WHERE illust_id = ?", (illust_id,)) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else None


async def get_push_history_paginated(limit: int = 24, offset: int = 0) -> tuple[list[dict], int]:
    """
    获取分页的推送历史
    
    Returns:
        (items, total): items 是包含 illust_id 和 pushed_at 的字典列表，total 是总数
    """
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        
        # 获取总数
        cursor = await db.execute("SELECT COUNT(*) FROM push_history")
        total = (await cursor.fetchone())[0]
        
        # 获取分页数据
        cursor = await db.execute(
            "SELECT illust_id, pushed_at, source FROM push_history ORDER BY pushed_at DESC LIMIT ? OFFSET ?",
            (limit, offset)
        )
        rows = await cursor.fetchall()
        
        items = [{"illust_id": row["illust_id"], "pushed_at": row["pushed_at"], "source": row["source"]} for row in rows]
        
        return items, total


# ============ XP画像 ============
async def get_xp_profile() -> dict[str, float]:
    """获取XP画像"""
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT tag, weight FROM xp_profile ORDER BY weight DESC")
        rows = await cursor.fetchall()
        return {tag: weight for tag, weight in rows}


async def update_xp_profile(profile: dict[str, float]):
    """更新XP画像"""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM xp_profile")
        await db.executemany(
            "INSERT INTO xp_profile (tag, weight, updated_at) VALUES (?, ?, ?)",
            [(tag, weight, datetime.now()) for tag, weight in profile.items()]
        )
        await db.commit()


async def adjust_tag_weight(tag: str, delta: float):
    """调整Tag权重"""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO xp_profile (tag, weight, updated_at) VALUES (?, ?, ?)
            ON CONFLICT(tag) DO UPDATE SET 
                weight = weight + excluded.weight,
                updated_at = excluded.updated_at
        """, (tag, delta, datetime.now()))
        await db.commit()


async def update_xp_tag_pairs(pairs: list[tuple[str, str, float]]):
    """更新Tag组合权重"""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM xp_tag_pairs")
        await db.executemany(
            "INSERT INTO xp_tag_pairs (tag1, tag2, weight) VALUES (?, ?, ?)",
            pairs
        )
        await db.commit()


async def get_top_tag_pairs(limit: int = 20) -> list[tuple[str, str, float]]:
    """获取热门Tag组合"""
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT tag1, tag2, weight FROM xp_tag_pairs ORDER BY weight DESC LIMIT ?",
            (limit,)
        )
        return await cursor.fetchall()


# ============ 反馈 ============
async def record_feedback(illust_id: int, action: str):
    """记录反馈"""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR REPLACE INTO feedback (illust_id, action, created_at) VALUES (?, ?, ?)",
            (illust_id, action, datetime.now())
        )
        await db.commit()


async def get_recent_liked_tags(limit: int = 10) -> list[str]:
    """
    获取近期喜欢的作品的标签 (用于 AI 评分)
    
    从 feedback 关联 illust_cache 获取标签
    """
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("""
            SELECT c.tags FROM feedback f
            JOIN illust_cache c ON f.illust_id = c.illust_id
            WHERE f.action = 'like'
            ORDER BY f.created_at DESC
            LIMIT ?
        """, (limit,))
        rows = await cursor.fetchall()
        
        # 收集所有标签
        all_tags = []
        for row in rows:
            try:
                tags = json.loads(row[0])
                all_tags.extend(tags[:5])  # 每个作品取前 5 个标签
            except:
                pass
        return all_tags[:limit * 3]  # 返回适量标签


async def get_recent_disliked_tags(limit: int = 10) -> list[str]:
    """
    获取近期不喜欢的作品的标签 (用于 AI 评分)
    """
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("""
            SELECT c.tags FROM feedback f
            JOIN illust_cache c ON f.illust_id = c.illust_id
            WHERE f.action = 'dislike'
            ORDER BY f.created_at DESC
            LIMIT ?
        """, (limit,))
        rows = await cursor.fetchall()
        
        all_tags = []
        for row in rows:
            try:
                tags = json.loads(row[0])
                all_tags.extend(tags[:5])
            except:
                pass
        return all_tags[:limit * 3]


async def get_liked_illusts() -> set[int]:
    """获取所有被点赞的作品ID"""
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT illust_id FROM feedback WHERE action = 'like'"
        )
        rows = await cursor.fetchall()
        return {row[0] for row in rows}


async def increment_tag_dislike(tag: str) -> int:
    """增加Tag否认计数，返回当前计数"""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO tag_blacklist (tag, dislike_count) VALUES (?, 1)
            ON CONFLICT(tag) DO UPDATE SET dislike_count = dislike_count + 1
        """, (tag,))
        await db.commit()
        cursor = await db.execute(
            "SELECT dislike_count FROM tag_blacklist WHERE tag = ?", (tag,)
        )
        row = await cursor.fetchone()
        return row[0] if row else 0


async def get_blacklisted_tags() -> set[str]:
    """获取所有黑名单Tag"""
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT tag FROM tag_blacklist WHERE dislike_count >= 1"
        )
        rows = await cursor.fetchall()
        return {row[0] for row in rows}


# ============ 收藏同步 ============
async def get_scanned_bookmarks() -> set[int]:
    """获取已扫描的收藏ID"""
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT illust_id FROM bookmarks")
        rows = await cursor.fetchall()
        return {row[0] for row in rows}


async def mark_bookmark_scanned(illust_id: int):
    """标记收藏已扫描"""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR IGNORE INTO bookmarks (illust_id) VALUES (?)", (illust_id,)
        )
        await db.commit()


# ============ 作品缓存 ============

async def cache_illust(
    illust_id: int, 
    tags: list[str], 
    user_id: int = 0, 
    user_name: str = "",
    source: str = "xp_search",  # 新增：作品来源策略
    chain_depth: int = 0,
    chain_parent_id: int = None,
    chain_msg_id: int = None
):
    """缓存作品信息 (v4: 包含来源归因 + 连锁元数据)"""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """INSERT OR REPLACE INTO illust_cache 
               (illust_id, tags, user_id, user_name, source, chain_depth, chain_parent_id, chain_msg_id, created_at) 
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (illust_id, json.dumps(tags), user_id, user_name, source, chain_depth, chain_parent_id, chain_msg_id, datetime.now())
        )
        await db.commit()


async def get_push_source(illust_id: int) -> str | None:
    """获取作品的推送来源策略"""
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT source FROM illust_cache WHERE illust_id = ?",
            (illust_id,)
        )
        row = await cursor.fetchone()
        return row[0] if row else None


async def get_cached_illust_tags(illust_id: int) -> list[str] | None:
    """获取缓存的作品tags (兼容旧接口)"""
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT tags FROM illust_cache WHERE illust_id = ?", (illust_id,)
        )
        row = await cursor.fetchone()
        if row and row[0]:
            return json.loads(row[0])
        return None


        return None


async def get_cached_illust(illust_id: int) -> dict | None:
    """获取缓存的完整作品信息 (用于反馈处理, v3 含连锁信息)"""
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            """SELECT illust_id, tags, user_id, user_name, 
                      chain_depth, chain_parent_id, chain_msg_id 
               FROM illust_cache WHERE illust_id = ?""", 
            (illust_id,)
        )
        row = await cursor.fetchone()
        if row:
            return {
                "id": row[0],
                "tags": json.loads(row[1]) if row[1] else [],
                "user_id": row[2] or 0,
                "user_name": row[3] or "",
                "chain_depth": row[4] or 0,
                "chain_parent_id": row[5],
                "chain_msg_id": row[6]
            }
        return None


async def set_chain_meta(illust_id: int, chain_depth: int, chain_parent_id: int = None, chain_msg_id: int = None):
    """设置作品的连锁元数据 (用于已缓存的作品)"""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """UPDATE illust_cache 
               SET chain_depth = ?, chain_parent_id = ?, chain_msg_id = ?
               WHERE illust_id = ?""",
            (chain_depth, chain_parent_id, chain_msg_id, illust_id)
        )
        await db.commit()


async def get_chain_meta(illust_id: int) -> tuple[int, int | None, int | None]:
    """获取作品的连锁元数据
    Returns: (chain_depth, chain_parent_id, chain_msg_id)
    """
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT chain_depth, chain_parent_id, chain_msg_id FROM illust_cache WHERE illust_id = ?",
            (illust_id,)
        )
        row = await cursor.fetchone()
        if row:
            return (row[0] or 0, row[1], row[2])
        return (0, None, None)


async def delete_cached_illust(illust_id: int):
    """从缓存中删除作品信息"""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "DELETE FROM illust_cache WHERE illust_id = ?", (illust_id,)
        )
        await db.commit()


async def cleanup_old_illust_cache(days: int = 30) -> int:
    """清理 N 天前的旧缓存记录"""
    cutoff = datetime.now() - timedelta(days=days)
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "DELETE FROM illust_cache WHERE created_at < ?", (cutoff,)
        )
        await db.commit()
        return cursor.rowcount


# ============ AI 错误处理 ============
async def add_ai_error(tags: list[str], error: str) -> int:
    """记录 AI 错误"""
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "INSERT INTO ai_error_logs (tags_content, error_msg) VALUES (?, ?)",
            (json.dumps(tags), str(error))
        )
        await db.commit()
        return cursor.lastrowid


async def get_ai_error(error_id: int) -> dict | None:
    """获取单条错误记录"""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM ai_error_logs WHERE id = ?", (error_id,)
        )
        row = await cursor.fetchone()
        return dict(row) if row else None


async def update_ai_error_status(error_id: int, status: str):
    """更新错误状态"""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE ai_error_logs SET status = ? WHERE id = ?",
            (status, error_id)
        )
        await db.commit()


# ============ XP 收藏缓存 ============
async def get_xp_bookmarks(user_id: int) -> list[dict]:
    """获取缓存的XP收藏数据"""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM xp_bookmarks WHERE user_id = ?", (user_id,)
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]

async def save_xp_bookmarks(user_id: int, bookmarks: list):
    """保存收藏数据用于分析"""
    # bookmarks: list of Illust objects or dicts
    data = []
    for b in bookmarks:
        # 兼容 Illust 对象和 dict
        if hasattr(b, 'id'):
             iid = b.id
             tags = json.dumps(b.tags)
             cdate = b.create_date
        else:
             iid = b['id']
             tags = json.dumps(b['tags'])
             cdate = b['create_date']
             
        data.append((iid, user_id, tags, cdate))
        
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executemany(
            """INSERT OR REPLACE INTO xp_bookmarks 
               (illust_id, user_id, tags, illust_create_date) 
               VALUES (?, ?, ?, ?)""",
            data
        )
        await db.commit()


# ============ 系统状态 ============
async def get_state(key: str) -> str | None:
    """获取系统状态值"""
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT value FROM system_state WHERE key = ?", (key,))
        row = await cursor.fetchone()
        return row[0] if row else None

async def set_state(key: str, value: str):
    """设置系统状态值"""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR REPLACE INTO system_state (key, value, updated_at) VALUES (?, ?, ?)",
            (key, value, datetime.now())
        )
        await db.commit()


# ============ 推送统计 ============
async def get_push_stats(days: int = 7) -> dict:
    """
    获取推送统计信息
    
    Args:
        days: 统计天数
    
    Returns:
        {
            "total_pushed": 总推送数,
            "total_feedback": 反馈数,
            "likes": 喜欢数,
            "dislikes": 不喜欢数,
            "top_artists": [(artist_id, count), ...],
            "top_tags": [(tag, count), ...]
        }
    """
    since = datetime.now() - timedelta(days=days)
    
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        
        # 推送总数
        cursor = await db.execute(
            "SELECT COUNT(*) FROM push_history WHERE pushed_at > ?",
            (since,)
        )
        row = await cursor.fetchone()
        total_pushed = row[0] if row else 0
        
        # 反馈统计
        cursor = await db.execute(
            "SELECT action, COUNT(*) as cnt FROM feedback WHERE created_at > ? GROUP BY action",
            (since,)
        )
        feedback_rows = await cursor.fetchall()
        likes = 0
        dislikes = 0
        for r in feedback_rows:
            if r['action'] == 'like':
                likes = r['cnt']
            elif r['action'] == 'dislike':
                dislikes = r['cnt']
        
        # Top 画师（从缓存表查）
        cursor = await db.execute("""
            SELECT ic.user_id, COUNT(*) as cnt 
            FROM push_history ph
            JOIN illust_cache ic ON ph.illust_id = ic.illust_id
            WHERE ph.pushed_at > ?
            GROUP BY ic.user_id
            ORDER BY cnt DESC
            LIMIT 5
        """, (since,))
        top_artists = [(row['user_id'], row['cnt']) for row in await cursor.fetchall()]
        
        # Top 标签（从缓存表查）
        cursor = await db.execute("""
            SELECT ic.tags FROM push_history ph
            JOIN illust_cache ic ON ph.illust_id = ic.illust_id
            WHERE ph.pushed_at > ?
        """, (since,))
        rows = await cursor.fetchall()
        
        tag_count = {}
        for row in rows:
            try:
                tags = json.loads(row['tags']) if row['tags'] else []
                for tag in tags[:5]:  # 只统计前5个标签
                    tag_count[tag] = tag_count.get(tag, 0) + 1
            except:
                pass
        
        top_tags = sorted(tag_count.items(), key=lambda x: x[1], reverse=True)[:5]
        
        return {
            "total_pushed": total_pushed,
            "total_feedback": likes + dislikes,
            "likes": likes,
            "dislikes": dislikes,
            "top_artists": top_artists,
            "top_tags": top_tags
        }


async def format_stats_report(days: int = 7) -> str:
    """生成格式化的统计报告"""
    stats = await get_push_stats(days)
    
    period = "本周" if days == 7 else f"近{days}天"
    
    # 格式化 Top 画师
    artists_str = ""
    if stats["top_artists"]:
        artists_str = "\n".join(f"  - ID {a[0]}: {a[1]}张" for a in stats["top_artists"][:3])
    else:
        artists_str = "  暂无数据"
    
    # 格式化 Top 标签
    tags_str = ""
    if stats["top_tags"]:
        tags_str = ", ".join(f"#{t[0]}({t[1]})" for t in stats["top_tags"][:5])
    else:
        tags_str = "暂无数据"
    
    return f"""📊 {period}推送统计

📤 推送: {stats['total_pushed']} 张作品
👍 喜欢: {stats['likes']} | 👎 不喜欢: {stats['dislikes']}

🎨 Top 画师:
{artists_str}

🏷️ Top 标签: {tags_str}"""

# ============ 数据清理 ============
async def reset_xp_data():
    """
    重置所有 XP 分析数据（适用于Prompt变更后需要重新清洗的情况）
    将会清除：
    1. XP画像 (xp_profile, xp_tag_pairs)
    2. 标签映射统计 (tag_mapping_stats)
    3. 系统状态中的处理进度 (system_state)
    
    保留：
    1. 推送历史 (push_history)
    2. 用户反馈 (feedback)
    3. 黑名单 (tag_blacklist)
    """
    async with aiosqlite.connect(DB_PATH) as db:
        # 清除画像数据
        await db.execute("DELETE FROM xp_profile")
        await db.execute("DELETE FROM xp_tag_pairs")
        
        # 清除 AI 映射统计
        await db.execute("DELETE FROM tag_mapping_stats")
        
        # 清除 AI 错误日志
        await db.execute("DELETE FROM ai_error_logs")
        
        # 清除 MAB 策略统计
        await db.execute("DELETE FROM strategy_stats")
        
        # 清除 AI 处理结果缓存 (让 AI 重新清洗)
        await db.execute("DELETE FROM ai_tag_cache")
        
        # 注意：不清除 system_state 中的同步进度
        # 这样 Profiler 会跳过 Pixiv API 抓取，直接从 xp_bookmarks 读取缓存进行重分析
        
        await db.commit()


# ============ MAB 策略统计 ============
async def update_strategy_stats(strategy: str, is_success: bool):
    """
    更新策略统计
    success_count += 1 (if success)
    total_count += 1
    """
    success_inc = 1 if is_success else 0
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO strategy_stats (strategy, success_count, total_count)
            VALUES (?, ?, 1)
            ON CONFLICT(strategy) DO UPDATE SET
                success_count = success_count + excluded.success_count,
                total_count = total_count + 1,
                updated_at = CURRENT_TIMESTAMP
        """, (strategy, success_inc))
        await db.commit()

async def get_strategy_stats(strategy: str) -> tuple[int, int]:
    """
    获取策略统计
    Returns: (success_count, total_count)
    """
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT success_count, total_count FROM strategy_stats WHERE strategy = ?",
            (strategy,)
        )
        row = await cursor.fetchone()
        if row:
            return row[0], row[1]
        return 0, 0


# ============ 快速屏蔽 (Bot /block) ============
async def block_tag(tag: str):
    """添加标签到屏蔽列表"""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR IGNORE INTO blocked_tags (tag) VALUES (?)",
            (tag.lower().strip(),)
        )
        await db.commit()


async def unblock_tag(tag: str) -> bool:
    """从屏蔽列表移除标签，并重置其厌恶计数"""
    tag = tag.lower().strip()
    async with aiosqlite.connect(DB_PATH) as db:
        # 1. 移除手动屏蔽
        cursor = await db.execute(
            "DELETE FROM blocked_tags WHERE tag = ?",
            (tag,)
        )
        manual_deleted = cursor.rowcount > 0
        
        # 2. 重置厌恶计数 (针对自动屏蔽)
        stats_updated = False
        try:
            cursor = await db.execute(
                "UPDATE tag_feedback_stats SET dislike_count = 0 WHERE tag = ?",
                (tag,)
            )
            stats_updated = cursor.rowcount > 0
        except Exception:
            # 兼容未创建 tag_feedback_stats 表的部署
            stats_updated = False
        
        await db.commit()
        return manual_deleted or stats_updated


async def get_blocked_tags() -> list[str]:
    """获取所有屏蔽的标签 (手动 + 自动)"""
    async with aiosqlite.connect(DB_PATH) as db:
        # 1. 手动屏蔽
        cursor = await db.execute("SELECT tag FROM blocked_tags")
        rows = await cursor.fetchall()
        manual = {row[0] for row in rows}
        
        # 2. 自动屏蔽 (dislike >= 3)
        # 注意：这里硬编码了 3，最好从 config 传参，但 database 层通常不读 config
        # 或者我们只利用这个函数返回 manual，profiler 自己处理 auto
        # 但为了 /unblock 能查到，我们需要在这里聚合
        # 实际上用户更关心的是"生效的屏蔽"
        # 让我们把阈值作为参数，默认为 3
        return list(manual)

async def get_all_blocked_tags(dislike_threshold: int = 3) -> list[str]:
    """获取所有生效的屏蔽标签 (包括手动和高厌恶)"""
    async with aiosqlite.connect(DB_PATH) as db:
        # 手动
        cursor = await db.execute("SELECT tag FROM blocked_tags")
        manual = {row[0] for row in (await cursor.fetchall())}
        
        # 自动
        cursor = await db.execute(
            "SELECT tag FROM tag_feedback_stats WHERE dislike_count >= ?",
            (dislike_threshold,)
        )
        auto = {row[0] for row in (await cursor.fetchall())}
        
        return list(manual | auto)


async def is_tag_blocked(tag: str) -> bool:
    """检查标签是否被屏蔽"""
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT 1 FROM blocked_tags WHERE tag = ?",
            (tag.lower().strip(),)
        )
        return await cursor.fetchone() is not None


# ============ 画师屏蔽 (/block_artist) ============
async def block_artist(artist_id: int, artist_name: str = None):
    """添加画师到屏蔽列表"""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR IGNORE INTO blocked_artists (artist_id, artist_name) VALUES (?, ?)",
            (artist_id, artist_name)
        )
        await db.commit()


async def unblock_artist(artist_id: int) -> bool:
    """从屏蔽列表移除画师，返回是否成功移除"""
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "DELETE FROM blocked_artists WHERE artist_id = ?",
            (artist_id,)
        )
        await db.commit()
        return cursor.rowcount > 0

async def update_artist_score(artist_id: int, delta: float):
    """更新画师权重分数 (增量)"""
    async with aiosqlite.connect(DB_PATH) as db:
        # Upsert logic: insert or update
        await db.execute("""
            INSERT INTO artist_profile (artist_id, score, updated_at)
            VALUES (?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(artist_id) DO UPDATE SET
                score = score + ?,
                updated_at = CURRENT_TIMESTAMP
        """, (artist_id, delta, delta))
        await db.commit()

async def get_artist_score(artist_id: int) -> float:
    """获取画师权重分数"""
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT score FROM artist_profile WHERE artist_id = ?", (artist_id,))
        row = await cursor.fetchone()
        return row[0] if row else 0.0


async def get_blocked_artists() -> list[tuple[int, str]]:
    """获取所有屏蔽的画师，返回 [(artist_id, artist_name), ...]"""
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT artist_id, artist_name FROM blocked_artists")
        rows = await cursor.fetchall()
        return [(row[0], row[1] or str(row[0])) for row in rows]


async def is_artist_blocked(artist_id: int) -> bool:
    """检查画师是否被屏蔽"""
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT 1 FROM blocked_artists WHERE artist_id = ?",
            (artist_id,)
        )
        return await cursor.fetchone() is not None


# ============ XP 画像查询 (/xp) ============
async def get_top_xp_tags(limit: int = 15) -> list[tuple[str, float]]:
    """
    获取权重最高的 Top N 标签
    Returns: [(tag, weight), ...]
    """
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT tag, weight FROM xp_profile ORDER BY weight DESC LIMIT ?",
            (limit,)
        )
        rows = await cursor.fetchall()
        return [(row[0], row[1]) for row in rows]


# ============ 互动画师发现 (策略E) ============
async def get_top_engaged_artists(limit: int = 10) -> list[tuple[int, str, int]]:
    """
    获取用户互动最多的画师列表 (用于 Engagement-Based Discovery 策略)
    
    通过 feedback + illust_cache 联表查询，统计各画师被点赞的次数。
    
    Returns: [(artist_id, artist_name, like_count), ...]
    """
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("""
            SELECT ic.user_id, ic.user_name, COUNT(*) as like_count
            FROM feedback f
            JOIN illust_cache ic ON f.illust_id = ic.illust_id
            WHERE f.action = 'like' AND ic.user_id IS NOT NULL AND ic.user_id > 0
            GROUP BY ic.user_id
            ORDER BY like_count DESC
            LIMIT ?
        """, (limit,))
        rows = await cursor.fetchall()
        return [(row[0], row[1] or "", row[2]) for row in rows]


async def get_recent_engagement_sequence(limit: int = 50) -> list[tuple[int, str, str]]:
    """
    获取最近的用户互动序列 (用于历史序列建模)
    
    Returns: [(illust_id, action, timestamp), ...]
    """
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("""
            SELECT illust_id, action, created_at
            FROM feedback
            ORDER BY created_at DESC
            LIMIT ?
        """, (limit,))
        rows = await cursor.fetchall()
        return [(row[0], row[1], row[2]) for row in rows]


# ============ Embedding 缓存 ============
async def get_illust_embedding(illust_id: int) -> Optional[list[float]]:
    """获取作品的缓存 Embedding"""
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT embedding FROM illust_embeddings WHERE illust_id = ?",
            (illust_id,)
        )
        row = await cursor.fetchone()
        if row and row[0]:
            return json.loads(row[0])
        return None


async def save_illust_embedding(illust_id: int, embedding: list[float], model: str):
    """保存作品的 Embedding"""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT OR REPLACE INTO illust_embeddings (illust_id, embedding, model, created_at)
            VALUES (?, ?, ?, ?)
        """, (illust_id, json.dumps(embedding), model, datetime.now()))
        await db.commit()


async def get_illust_embeddings_batch(illust_ids: list[int]) -> dict[int, list[float]]:
    """批量获取作品的缓存 Embedding"""
    if not illust_ids:
        return {}
    
    async with aiosqlite.connect(DB_PATH) as db:
        placeholders = ",".join("?" * len(illust_ids))
        cursor = await db.execute(
            f"SELECT illust_id, embedding FROM illust_embeddings WHERE illust_id IN ({placeholders})",
            illust_ids
        )
        rows = await cursor.fetchall()
        return {row[0]: json.loads(row[1]) for row in rows if row[1]}


async def save_illust_embeddings_batch(items: list[tuple[int, list[float], str]]):
    """
    批量保存作品 Embedding
    
    Args:
        items: [(illust_id, embedding, model), ...]
    """
    if not items:
        return
    
    async with aiosqlite.connect(DB_PATH) as db:
        now = datetime.now()
        data = [(iid, json.dumps(emb), model, now) for iid, emb, model in items]
        await db.executemany("""
            INSERT OR REPLACE INTO illust_embeddings (illust_id, embedding, model, created_at)
            VALUES (?, ?, ?, ?)
        """, data)
        await db.commit()


async def get_user_embedding(user_id: int) -> Optional[tuple[list[float], str]]:
    """
    获取用户画像 Embedding
    
    Returns: (embedding, profile_hash) or None
    """
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT embedding, profile_hash FROM user_embedding WHERE user_id = ?",
            (user_id,)
        )
        row = await cursor.fetchone()
        if row and row[0]:
            return (json.loads(row[0]), row[1])
        return None


async def save_user_embedding(user_id: int, embedding: list[float], model: str, profile_hash: str):
    """保存用户画像 Embedding"""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT OR REPLACE INTO user_embedding (user_id, embedding, model, profile_hash, updated_at)
            VALUES (?, ?, ?, ?, ?)
        """, (user_id, json.dumps(embedding), model, profile_hash, datetime.now()))
        await db.commit()


async def cleanup_old_embeddings(days: int = 60) -> int:
    """清理过期的作品 Embedding 缓存"""
    cutoff = datetime.now() - timedelta(days=days)
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "DELETE FROM illust_embeddings WHERE created_at < ?",
            (cutoff,)
        )
        await db.commit()
        return cursor.rowcount


# ============ MAB 策略统计汇总 (/stats) ============
async def get_all_strategy_stats() -> dict[str, dict]:
    """
    获取所有策略的统计数据
    Returns: {strategy: {"success": int, "total": int, "rate": float}, ...}
    """
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT strategy, success_count, total_count FROM strategy_stats"
        )
        rows = await cursor.fetchall()
        result = {}
        for strategy, success, total in rows:
            success = int(success or 0)
            total = int(total or 0)
            rate = success / total if total > 0 else 0.0
            result[strategy] = {"success": success, "total": total, "rate": rate}
        return result


# ============ 每日维护辅助函数 ============
async def sync_blocked_tags_to_xp() -> int:
    """将屏蔽的标签从 XP 画像中移除，返回移除数量"""
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("""
            DELETE FROM xp_profile 
            WHERE tag IN (SELECT tag FROM blocked_tags)
        """)
        await db.commit()
        return cursor.rowcount


async def get_uncached_tags(limit: int = 100) -> list[str]:
    """
    获取尚未被 AI 处理过的标签 (在 xp_profile 中但不在 ai_tag_cache 中)
    """
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("""
            SELECT DISTINCT tag FROM xp_profile 
            WHERE tag NOT IN (SELECT original_tag FROM ai_tag_cache)
            LIMIT ?
        """, (limit,))
        rows = await cursor.fetchall()
        return [row[0] for row in rows]


async def cleanup_old_sent_history(days: int = 30) -> int:
    """清理 N 天前的推送历史记录，返回删除数量"""
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("""
            DELETE FROM push_history 
            WHERE pushed_at < datetime('now', ?)
        """, (f'-{days} days',))
        await db.commit()
        return cursor.rowcount


# ============ 负向画像 (负反馈记录) ============
async def get_negative_profile() -> dict[str, float]:
    """获取负向画像"""
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT tag, weight FROM negative_profile ORDER BY weight DESC")
        rows = await cursor.fetchall()
        return {tag: weight for tag, weight in rows}


async def adjust_negative_weight(tag: str, delta: float):
    """调整负向画像权重"""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO negative_profile (tag, weight, updated_at) VALUES (?, ?, ?)
            ON CONFLICT(tag) DO UPDATE SET 
                weight = weight + excluded.weight,
                updated_at = excluded.updated_at
        """, (tag, delta, datetime.now()))
        await db.commit()


async def get_top_negative_tags(limit: int = 20) -> list[tuple[str, float]]:
    """获取权重最高的负向 Tag"""
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT tag, weight FROM negative_profile ORDER BY weight DESC LIMIT ?",
            (limit,)
        )
        rows = await cursor.fetchall()
        return [(row[0], row[1]) for row in rows]


# ============ 冷启动支持 ============
async def get_popular_tags(limit: int = 20) -> list[tuple[str, float]]:
    """
    获取热门 Tag（基于收藏频率）
    用于冷启动时注入先验权重
    """
    async with aiosqlite.connect(DB_PATH) as db:
        # 从 xp_bookmarks 统计标签出现频率
        cursor = await db.execute("""
            SELECT tag, COUNT(*) as freq
            FROM (
                SELECT json_each.value as tag 
                FROM xp_bookmarks, json_each(xp_bookmarks.tags)
            )
            GROUP BY tag
            ORDER BY freq DESC
            LIMIT ?
        """, (limit,))
        rows = await cursor.fetchall()
        if rows:
            return [(row[0], row[1]) for row in rows]
        
        # Fallback: 如果 xp_bookmarks 为空，从现有画像中取 top tags
        cursor = await db.execute(
            "SELECT tag, weight FROM xp_profile ORDER BY weight DESC LIMIT ?",
            (limit,)
        )
        rows = await cursor.fetchall()
        return [(row[0], row[1]) for row in rows]


async def get_bookmark_count(user_id: int = None) -> int:
    """获取收藏数量（用于检测冷启动）"""
    async with aiosqlite.connect(DB_PATH) as db:
        if user_id:
            cursor = await db.execute(
                "SELECT COUNT(*) FROM xp_bookmarks WHERE user_id = ?",
                (user_id,)
            )
        else:
            cursor = await db.execute("SELECT COUNT(*) FROM xp_bookmarks")
        row = await cursor.fetchone()
        return row[0] if row else 0


# ============ 批量消息映射 (Telegraph 模式) ============
async def save_batch_mapping(message_id: int, chat_id: str, illusts: list):
    """
    保存批量消息与作品的映射关系
    
    Args:
        message_id: Telegram 消息 ID
        chat_id: 聊天 ID
        illusts: 作品列表 (需要有 .id 属性)
    """
    async with aiosqlite.connect(DB_PATH) as db:
        data = [(message_id, str(chat_id), i + 1, illust.id) 
                for i, illust in enumerate(illusts)]
        await db.executemany(
            """INSERT OR REPLACE INTO batch_message_map 
               (message_id, chat_id, illust_index, illust_id) VALUES (?, ?, ?, ?)""",
            data
        )
        await db.commit()


async def get_batch_illust_id(message_id: int, chat_id: str, index: int) -> int | None:
    """
    根据消息 ID 和编号获取作品 ID
    
    Args:
        message_id: Telegram 消息 ID
        chat_id: 聊天 ID
        index: 作品编号 (1-based)
    
    Returns:
        作品 ID，不存在时返回 None
    """
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            """SELECT illust_id FROM batch_message_map 
               WHERE message_id = ? AND chat_id = ? AND illust_index = ?""",
            (message_id, str(chat_id), index)
        )
        row = await cursor.fetchone()
        return row[0] if row else None


async def get_batch_all_illust_ids(message_id: int, chat_id: str) -> list[int]:
    """获取批量消息中所有作品 ID"""
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            """SELECT illust_id FROM batch_message_map 
               WHERE message_id = ? AND chat_id = ? 
               ORDER BY illust_index""",
            (message_id, str(chat_id))
        )
        rows = await cursor.fetchall()
        return [row[0] for row in rows]


async def cleanup_old_batch_mappings(days: int = 7) -> int:
    """清理旧的批量消息映射"""
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            """DELETE FROM batch_message_map 
               WHERE created_at < datetime('now', ?)""",
            (f'-{days} days',)
        )
        await db.commit()
        return cursor.rowcount
