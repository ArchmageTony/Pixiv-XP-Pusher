"""
OneBot 协议推送实现
兼容 go-cqhttp, Lagrange 等
"""
import asyncio
import hmac
import logging
import json
import uuid
import urllib.request
from typing import Callable, Optional

import aiohttp
from aiohttp import web

from .base import BaseNotifier
from pixiv_client import Illust
from utils import get_pixiv_cat_url
import base64

logger = logging.getLogger(__name__)


class OneBotNotifier(BaseNotifier):
    """OneBot v11 协议推送（支持正向与反向 WebSocket）。"""
    
    def __init__(
        self,
        ws_url: str | None = None,
        mode: str = "forward",
        reverse_config: dict | None = None,
        # 推送目标配置
        private_id: str | None = None,    # 私聊推送目标 QQ
        group_id: str | None = None,       # 群聊推送目标群号
        push_to_private: bool = True,      # 是否推送到私聊
        push_to_group: bool = False,       # 是否推送到群聊
        # 权限控制
        master_id: str | None = None,      # 主人 QQ（只有主人指令有效）
        on_feedback: Optional[Callable] = None,
        on_action: Optional[Callable] = None,
        client: Optional['PixivClient'] = None,
        max_pages: int = 10,
        proxy_url: str | None = None
    ):
        self.mode = mode.lower().strip()
        if self.mode not in ("forward", "reverse"):
            raise ValueError("OneBot mode 必须是 forward 或 reverse")

        self.ws_url = ws_url
        if self.mode == "forward" and not self.ws_url:
            raise ValueError("OneBot 正向模式必须配置 ws_url")

        reverse_config = reverse_config or {}
        self.reverse_host = str(reverse_config.get("host", "0.0.0.0"))
        self.reverse_port = int(reverse_config.get("port", 8765))
        self.reverse_path = str(reverse_config.get("path", "/onebot/v11/ws"))
        if not self.reverse_path.startswith("/"):
            self.reverse_path = f"/{self.reverse_path}"
        self.access_token = str(reverse_config.get("access_token", ""))
        self.connection_timeout = float(reverse_config.get("connection_timeout", 30))
        if self.mode == "reverse" and not self.access_token:
            raise ValueError("OneBot 反向模式必须配置 access_token")

        self.client = client
        self.private_id = int(private_id) if private_id else None
        self.group_id = int(group_id) if group_id else None
        self.push_to_private = push_to_private and self.private_id is not None
        self.push_to_group = push_to_group and self.group_id is not None
        self.master_id = int(master_id) if master_id else None
        self.on_feedback = on_feedback
        self.on_action = on_action
        self.max_pages = max_pages
        
        self._ws: Optional[aiohttp.ClientWebSocketResponse | web.WebSocketResponse] = None
        self._session: Optional[aiohttp.ClientSession] = None
        self._running = False
        self._pending_requests: dict[str, asyncio.Future] = {}
        self._connection_ready = asyncio.Event()
        self._send_lock = asyncio.Lock()
        self._runner: web.AppRunner | None = None
        self._site: web.TCPSite | None = None
        self._server_start_lock = asyncio.Lock()
        self._message_illust_map: dict[int, int] = {}
        self._last_illust_id: int | None = None
        if not proxy_url:
            proxies = urllib.request.getproxies()
            proxy_url = proxies.get("https") or proxies.get("http")
        self.proxy_url = proxy_url
        
        # 日志
        targets = []
        if self.push_to_private:
            targets.append(f"私聊:{self.private_id}")
        if self.push_to_group:
            targets.append(f"群:{self.group_id}")
        logger.info(f"OneBot 推送目标: {', '.join(targets) or '无'}")
        if self.master_id:
            logger.info(f"主人 QQ: {self.master_id}")
        logger.info(f"OneBot 连接模式: {self.mode}")
    
    async def connect(self):
        """连接WebSocket"""
        if self.mode == "reverse":
            await self.start_reverse_server()
            await self._wait_for_connection()
            return

        if self._ws and not self._ws.closed:
            return
        if self._session and not self._session.closed:
            await self._session.close()
        self._session = aiohttp.ClientSession()
        self._ws = await self._session.ws_connect(self.ws_url)
        self._connection_ready.set()
        logger.info(f"已连接到 OneBot: {self.ws_url}")

    async def start_reverse_server(self):
        """启动反向 WebSocket 监听服务（幂等）。"""
        if self.mode != "reverse" or self._runner is not None:
            return

        async with self._server_start_lock:
            if self._runner is not None:
                return
            app = web.Application()
            app.router.add_get(self.reverse_path, self._handle_reverse_websocket)
            runner = web.AppRunner(app)
            await runner.setup()
            site = web.TCPSite(runner, self.reverse_host, self.reverse_port)
            try:
                await site.start()
            except Exception:
                await runner.cleanup()
                raise
            self._runner = runner
            self._site = site
            logger.info(
                f"OneBot 反向 WebSocket 已监听: "
                f"ws://{self.reverse_host}:{self.reverse_port}{self.reverse_path}"
            )

    def _is_authorized(self, request: web.Request) -> bool:
        authorization = request.headers.get("Authorization", "")
        bearer = ""
        if authorization.lower().startswith("bearer "):
            bearer = authorization[7:].strip()
        supplied = bearer or request.query.get("access_token", "")
        return bool(supplied) and hmac.compare_digest(supplied, self.access_token)

    async def _handle_reverse_websocket(self, request: web.Request):
        if not self._is_authorized(request):
            logger.warning("拒绝未经授权的 OneBot 反向 WebSocket 连接")
            raise web.HTTPUnauthorized(text="Invalid OneBot access token")

        ws = web.WebSocketResponse(heartbeat=30)
        await ws.prepare(request)

        old_ws = self._ws
        if old_ws and old_ws is not ws and not old_ws.closed:
            self._fail_pending_requests(ConnectionError("OneBot 连接已被新连接替换"))
            await old_ws.close(code=1000, message=b"Replaced by a new connection")

        self._ws = ws
        self._running = True
        self._connection_ready.set()
        logger.info("OneBot 反向 WebSocket 已连接")

        try:
            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    try:
                        self._handle_ws_data(json.loads(msg.data))
                    except json.JSONDecodeError:
                        logger.warning("收到无效的 OneBot JSON 数据")
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    logger.warning(f"OneBot 反向 WebSocket 错误: {ws.exception()}")
        finally:
            if self._ws is ws:
                self._ws = None
                self._running = False
                self._connection_ready.clear()
                self._fail_pending_requests(ConnectionError("OneBot 反向 WebSocket 已断开"))
                logger.warning("OneBot 反向 WebSocket 已断开，等待本地客户端重连")
        return ws

    async def _wait_for_connection(self):
        if self._ws and not self._ws.closed:
            return
        try:
            await asyncio.wait_for(
                self._connection_ready.wait(), timeout=self.connection_timeout
            )
        except asyncio.TimeoutError as exc:
            raise TimeoutError(
                f"等待 OneBot 反向连接超时 ({self.connection_timeout:g}s)"
            ) from exc
        if not self._ws or self._ws.closed:
            raise ConnectionError("OneBot 反向连接不可用")

    def _fail_pending_requests(self, exc: Exception):
        for future in list(self._pending_requests.values()):
            if not future.done():
                future.set_exception(type(exc)(str(exc)))
    
    async def send(self, illusts: list[Illust]) -> list[int]:
        """发送推送"""
        if not illusts:
            return []
        
        if not self._ws:
            await self.connect()
        
        success_ids = []
        
        # 预先处理所有图片（下载+压缩+Base64）
        # 为了不阻塞太久，我们并发处理
        tasks = [self._prepare_illust_content(ill) for ill in illusts]
        prepared_data = await asyncio.gather(*tasks)
        
        # 多个作品优先使用 OneBot 合并转发，避免连续发送大量单独消息。
        if len(illusts) > 1:
            nodes = [self._create_node(content) for content in prepared_data]
            try:
                await self._send_forward(nodes)
                success_ids = [ill.id for ill in illusts]
                logger.info(f"OneBot 合并转发成功 ({len(illusts)} 条)")
                return success_ids
            except Exception as exc:
                logger.error(f"OneBot 合并转发失败: {exc}")
                logger.info("降级为逐条发送...")

        # 单个作品直接发送；合并转发失败时也走这里兜底。
        for ill, content in zip(illusts, prepared_data):
            try:
                await self._send_message(content)
                success_ids.append(ill.id)
                await asyncio.sleep(2)
            except Exception as exc:
                logger.error(f"发送作品 {ill.id} 失败: {exc}")
        
        return success_ids
    
    async def _prepare_illust_content(self, illust: Illust) -> str:
        """下载图片并生成最终消息内容"""
        image_cq = ""
        
        # 0. 动图特殊处理 (改为 GIF 以实现 QQ 自动播放)
        if getattr(illust, 'type', 'illust') == 'ugoira':
            logger.info(f"OneBot: 正在为作品 {illust.id} 生成预览动图...")
            try:
                from utils import convert_ugoira_to_gif
                meta = await self.client.get_ugoira_metadata(illust.id)
                if meta and meta.get('ugoira_metadata'):
                    u_meta = meta['ugoira_metadata']
                    zip_url = u_meta['zip_urls']['medium']
                    frames = u_meta['frames']
                    
                    zip_data = await self.client.download_image(zip_url)
                    if zip_data:
                        gif_data = convert_ugoira_to_gif(zip_data, frames)
                        if gif_data:
                            b64 = base64.b64encode(gif_data).decode()
                            # 使用 as_gif=1 提示一些兼容层尝试展示为动图
                            image_cq = f"[CQ:image,file=base64://{b64}]"
            except Exception as e:
                logger.warning(f"OneBot 本地转 GIF 失败: {e}")
            
            # 失败则退而求其次使用反代视频或封面
            if not image_cq:
                video_url = f"https://pixiv.cat/{illust.id}.mp4"
                cover_url = f"https://pixiv.cat/{illust.id}.jpg"
                image_cq = f"[CQ:video,file={video_url},cover={cover_url}]"
            
            return self.format_message(illust, image_cq)

        try:
            # 确定要发送的图片列表
            urls_to_send = []
            is_long_work = illust.page_count > self.max_pages
            
            if is_long_work or not illust.image_urls:
                # 仅封面
                urls_to_send = [illust.image_urls[0]] if illust.image_urls else []
            else:
                # 打包模式 (2 到 max_pages 页)
                urls_to_send = illust.image_urls[:self.max_pages]
            
            # 并发下载所有图片
            async def download_and_encode(url: str) -> str | None:
                try:
                    from utils import download_image_with_referer
                    image_data = await download_image_with_referer(
                        self._session, url, proxy=self.proxy_url
                    )
                    
                    import io
                    from PIL import Image
                    
                    with Image.open(io.BytesIO(image_data)) as img:
                        # 修复透明度警告和转换问题
                        if img.mode == 'P':
                            img = img.convert('RGBA')
                        
                        if img.mode in ('RGBA', 'LA'):
                            # 透明背景填充白色
                            bg = Image.new('RGB', img.size, (255, 255, 255))
                            bg.paste(img, mask=img.split()[-1])
                            img = bg
                        elif img.mode != 'RGB':
                            img = img.convert('RGB')
                        
                        # 激进压缩以确保合并转发不超时
                        max_dim = 1080  # 限制最大边长 1080p
                        if max(img.size) > max_dim:
                            img.thumbnail((max_dim, max_dim), Image.Resampling.LANCZOS)
                        
                        output = io.BytesIO()
                        # 降低质量，且不包含 metadata
                        img.save(output, format="JPEG", quality=75, optimize=True)
                        
                        # 检查大小，如果还是太大(>500KB)，继续压缩
                        if output.tell() > 500 * 1024:
                            output.seek(0)
                            output.truncate()
                            img.save(output, format="JPEG", quality=60, optimize=True)
                            
                        b64 = base64.b64encode(output.getvalue()).decode()
                        return f"[CQ:image,file=base64://{b64}]"
                except Exception as e:
                    logger.warning(f"图片下载/处理失败 {illust.id} @ {url}: {e}")
                    return None
            
            # 使用 asyncio.gather 并发下载
            results = await asyncio.gather(*[download_and_encode(url) for url in urls_to_send])
            cq_codes = [r for r in results if r]
            
            if cq_codes:
                image_cq = "".join(cq_codes)
            
        except Exception as e:
            logger.warning(f"图片下载/处理过程中出错 {illust.id}: {e}")
            # 失败兜底：使用 pixiv.cat 反代链接
            cat_url = get_pixiv_cat_url(illust.id)
            image_cq = f"[CQ:image,file={cat_url}]"

        # 如果上面都没生成（比如没URL），再兜底
        if not image_cq:
             cat_url = get_pixiv_cat_url(illust.id)
             image_cq = f"[CQ:image,file={cat_url}]"

        return self.format_message(illust, image_cq)
            
    async def _send_single(self, illust: Illust):
        """发送单条消息 (已弃用，逻辑合并到 send)"""
        pass

    async def push_illusts(
        self,
        illusts: list[Illust],
        message_prefix: str = "",
        reply_to_message_id: int | None = None,
    ) -> dict[int, int]:
        """发送连锁推荐，并返回作品 ID 到 OneBot 消息 ID 的映射。"""
        if not illusts:
            return {}

        if not self._ws:
            await self.connect()

        result_map: dict[int, int] = {}
        for illust in illusts:
            try:
                content = await self._prepare_illust_content(illust)
                if message_prefix:
                    content = f"{message_prefix}\n\n{content}"

                # 只有确定父消息属于当前 OneBot 实例时才引用，避免把 Telegram
                # 或其他推送器的消息 ID 误当作 QQ 消息 ID。
                if (
                    reply_to_message_id is not None
                    and reply_to_message_id in self._message_illust_map
                ):
                    content = f"[CQ:reply,id={reply_to_message_id}]{content}"

                message_ids = await self._send_message(content)
                if message_ids:
                    message_id = message_ids[0]
                    result_map[illust.id] = message_id
                    for sent_id in message_ids:
                        self._message_illust_map[sent_id] = illust.id
                    logger.info(
                        f"OneBot 连锁推送成功: {illust.id} -> msg_id={message_id}"
                    )
            except Exception as exc:
                logger.error(f"OneBot 连锁推送作品 {illust.id} 失败: {exc}")

            await asyncio.sleep(1)

        # 限制内存映射大小。
        if len(self._message_illust_map) > 200:
            oldest_keys = list(self._message_illust_map)[:100]
            for key in oldest_keys:
                del self._message_illust_map[key]

        return result_map
    
    def format_message(self, illust: Illust, image_cq: str = None) -> str:
        """格式化消息"""
        tags = " ".join(f"#{t}" for t in illust.tags[:5])
        r18_mark = "🔞 " if illust.is_r18 else ""
        ugoira_mark = "🎞️ " if getattr(illust, 'type', 'illust') == 'ugoira' else ""
        
        # 多页提示
        page_info = f" ({illust.page_count}P)" if illust.page_count > 1 else ""
        
        # 匹配度显示
        match_score = getattr(illust, 'match_score', None)
        match_line = f"🎯 匹配度: {match_score*100:.0f}%\n" if match_score is not None else ""
        
        # 如果未传入 image_cq (兼容旧调用)，生成反代链接
        if not image_cq:
             url = get_pixiv_cat_url(illust.id)
             image_cq = f"[CQ:image,file={url}]"
        
        # 状态标记
        long_mark = "📚 [长篇精选] " if illust.page_count > self.max_pages else ""
        page_tip = f"\n(本作品共 {illust.page_count} 页，仅展示封面)" if illust.page_count > self.max_pages else ""
        
        return (
            f"{image_cq}\n"
            f"{long_mark}{r18_mark}{ugoira_mark}🎨 {illust.title}{page_info}\n"
            f"👤 {illust.user_name}\n"
            f"❤️ {illust.bookmark_count}\n"
            f"{match_line}"
            f"🏷️ {tags}\n"
            f"🔗 https://pixiv.net/i/{illust.id}{page_tip}\n\n"
            f"💬 反馈: {illust.id} 1=喜欢 2=不喜欢"
        )
    
    async def _send_message(self, content: str, target_type: str = None, target_id: int = None):
        """
        发送普通消息
        
        Args:
            content: 消息内容
            target_type: 指定目标类型 ('private'|'group')，None 则发送到所有配置目标
            target_id: 指定目标 ID，None 则使用配置
        """
        targets = []
        
        if target_type and target_id:
            # 指定目标
            targets.append((target_type, target_id))
        else:
            # 发送到所有配置目标
            if self.push_to_private:
                targets.append(("private", self.private_id))
            if self.push_to_group:
                targets.append(("group", self.group_id))
        
        message_ids = []
        for t_type, t_id in targets:
            action = "send_private_msg" if t_type == "private" else "send_group_msg"
            id_field = "user_id" if t_type == "private" else "group_id"
            
            response = await self._call_api({
                "action": action,
                "params": {
                    id_field: t_id,
                    "message": content
                }
            })
            if response.get("status") != "ok":
                raise RuntimeError(response.get("wording") or response.get("message") or "OneBot 请求失败")
            message_id = (response.get("data") or {}).get("message_id")
            logger.info(f"OneBot {t_type} 消息已确认发送 (message_id={message_id})")
            if message_id is not None:
                message_ids.append(message_id)

        return message_ids

    async def _call_api(self, payload: dict, timeout: float = 30) -> dict:
        """调用 OneBot API 并等待同一 echo 的响应。"""
        if self.mode == "reverse":
            await self.start_reverse_server()
            await self._wait_for_connection()
        elif not self._ws or self._ws.closed:
            await self.connect()

        echo = str(uuid.uuid4())
        future = asyncio.get_running_loop().create_future()
        self._pending_requests[echo] = future
        payload["echo"] = echo
        try:
            async with self._send_lock:
                if not self._ws or self._ws.closed:
                    raise ConnectionError("OneBot WebSocket 连接不可用")
                await self._ws.send_json(payload)

            if self._running:
                return await asyncio.wait_for(future, timeout)

            while not future.done():
                msg = await self._ws.receive(timeout=timeout)
                if msg.type != aiohttp.WSMsgType.TEXT:
                    raise RuntimeError(f"OneBot 连接异常: {msg.type}")
                self._handle_ws_data(json.loads(msg.data))
            return future.result()
        finally:
            self._pending_requests.pop(echo, None)

    def _handle_ws_data(self, data: dict):
        echo = data.get("echo")
        future = self._pending_requests.get(echo)
        if future and not future.done():
            future.set_result(data)
            return
        asyncio.create_task(self._process_message(data))
    
    async def _send_forward(self, nodes: list[dict]):
        """发送合并转发消息到所有配置目标"""
        targets = []
        if self.push_to_private:
            targets.append(("private", self.private_id))
        if self.push_to_group:
            targets.append(("group", self.group_id))
        
        for t_type, t_id in targets:
            action = "send_private_forward_msg" if t_type == "private" else "send_group_forward_msg"
            id_field = "user_id" if t_type == "private" else "group_id"
            
            response = await self._call_api({
                "action": action,
                "params": {
                    id_field: t_id,
                    "messages": nodes
                }
            })
            if response.get("status") != "ok":
                raise RuntimeError(
                    response.get("wording")
                    or response.get("message")
                    or "OneBot 合并转发请求失败"
                )
            message_id = (response.get("data") or {}).get("message_id")
            logger.info(
                f"OneBot {t_type} 合并转发已确认发送 "
                f"(message_id={message_id}, nodes={len(nodes)})"
            )
    
    def _create_node(self, content: str) -> dict:
        """创建转发节点"""
        return {
            "type": "node",
            "data": {
                "name": "Pixiv推送",
                "uin": "10000",
                "content": content
            }
        }
    
    async def close(self):
        """关闭连接"""
        self._running = False
        self._connection_ready.clear()
        self._fail_pending_requests(ConnectionError("OneBot 通知器正在关闭"))
        if self._ws:
            await self._ws.close()
            self._ws = None
        if self._site:
            await self._site.stop()
            self._site = None
        if self._runner:
            await self._runner.cleanup()
            self._runner = None
        if self._session:
            await self._session.close()
            self._session = None

    
    async def handle_feedback(self, illust_id: int, action: str) -> bool:
        """处理反馈"""
        if self.on_feedback:
            await self.on_feedback(illust_id, action)
        return True
    
    async def start_listening(self):
        """监听消息（用于反馈处理）"""
        if self.mode == "reverse":
            await self.start_reverse_server()
            return

        if not self._ws or self._ws.closed:
            await self.connect()
        
        self._running = True
        
        while self._running:
            try:
                msg = await self._ws.receive()
                if msg.type == aiohttp.WSMsgType.TEXT:
                    data = json.loads(msg.data)
                    self._handle_ws_data(data)
                elif msg.type == aiohttp.WSMsgType.CLOSED:
                    break
            except Exception as e:
                logger.error(f"消息处理错误: {e}")
                break

        self._running = False
        self._connection_ready.clear()
        self._fail_pending_requests(ConnectionError("OneBot 正向 WebSocket 已断开"))
        if self._ws:
            await self._ws.close()
            self._ws = None
    
    async def _process_message(self, data: dict):
        """处理收到的消息"""
        if data.get("post_type") != "message":
            return
        
        # 获取发送者 QQ
        sender_id = data.get("sender", {}).get("user_id") or data.get("user_id")
        raw_message = data.get("raw_message", "").strip()
        
        # 主人权限验证：只有主人的指令才有效
        if self.master_id and sender_id != self.master_id:
            return
        
        # 解析指令
        if raw_message.startswith("/"):
            parts = raw_message.split()
            cmd = parts[0].lower()
            args = parts[1:]
            
            # --- /push ---
            if cmd == "/push":
                if self.on_action:
                    await self._send_message("🚀 正在触发推送任务...", "private", sender_id)
                    await self.on_action("run_task", None)
                return

            # --- /xp ---
            elif cmd == "/xp":
                try:
                    from database import get_top_xp_tags
                    top_tags = await get_top_xp_tags(15)
                    if not top_tags:
                        await self._send_message("📊 暂无 XP 画像数据", "private", sender_id)
                        return
                    
                    lines = ["🎯 您的 XP 画像 Top 15"]
                    for i, (tag, weight) in enumerate(top_tags, 1):
                        bar = "█" * min(int(weight), 10)
                        lines.append(f"{i}. {tag} {bar} ({weight:.1f})")
                    await self._send_message("\n".join(lines), "private", sender_id)
                except Exception as e:
                    await self._send_message(f"❌ 获取 XP 失败: {e}", "private", sender_id)
                return

            # --- /stats ---
            elif cmd == "/stats":
                try:
                    from database import get_all_strategy_stats
                    stats = await get_all_strategy_stats()
                    if not stats:
                        await self._send_message("📊 暂无策略统计数据", "private", sender_id)
                        return
                    
                    lines = ["📈 MAB 策略表现"]
                    strategy_names = {
                        "xp_search": "XP搜索", 
                        "search": "XP搜索(旧)", 
                        "subscription": "订阅更新", 
                        "ranking": "排行榜"
                    }
                    for strategy, data in stats.items():
                        name = strategy_names.get(strategy, strategy)
                        rate_pct = data["rate"] * 100
                        lines.append(f"• {name}: {data['success']}/{data['total']} ({rate_pct:.1f}%)")
                    await self._send_message("\n".join(lines), "private", sender_id)
                except Exception as e:
                    await self._send_message(f"❌ 获取统计失败: {e}", "private", sender_id)
                return

            # --- /block ---
            elif cmd == "/block":
                if not args:
                    try:
                        from database import get_blocked_tags
                        blocked = await get_blocked_tags()
                        if blocked:
                            await self._send_message(f"🚫 当前屏蔽列表:\n{', '.join(blocked)}", "private", sender_id)
                        else:
                            await self._send_message("🚫 屏蔽列表为空\n用法: /block <tag>", "private", sender_id)
                    except Exception as e:
                        await self._send_message(f"❌ 查询失败: {e}", "private", sender_id)
                    return
                
                tag = " ".join(args).strip()
                try:
                    from database import block_tag
                    await block_tag(tag)
                    await self._send_message(f"✅ 已屏蔽标签: {tag}", "private", sender_id)
                except Exception as e:
                    await self._send_message(f"❌ 屏蔽失败: {e}", "private", sender_id)
                return

            # --- /unblock ---
            elif cmd == "/unblock":
                if not args:
                    await self._send_message("用法: /unblock <tag>", "private", sender_id)
                    return
                
                tag = " ".join(args).strip()
                try:
                    from database import unblock_tag
                    result = await unblock_tag(tag)
                    if result:
                        await self._send_message(f"✅ 已取消屏蔽: {tag}", "private", sender_id)
                    else:
                        await self._send_message(f"⚠️ 该标签未在屏蔽列表中: {tag}", "private", sender_id)
                except Exception as e:
                    await self._send_message(f"❌ 取消屏蔽失败: {e}", "private", sender_id)
                return

            # --- /schedule ---
            elif cmd == "/schedule":
                try:
                    from database import get_state
                    import re
                    
                    current_cron = await get_state("schedule_cron")
                    if not current_cron:
                         # Fallback unknown (usually from config)
                         current_cron = "未配置(使用默认)"
                    
                    if not args:
                        await self._send_message(f"⏰ 当前定时: {current_cron}\n修改: /schedule 9:30,21:00", "private", sender_id)
                        return
                    
                    time_input = args[0].strip()
                    # 简单校验
                    if not re.match(r"^[\d:,]+$", time_input):
                         await self._send_message("❌ 格式错误，示例: 12:30 或 9:00,21:30", "private", sender_id)
                         return

                    # 转换逻辑 (复用): "9:30" -> "30 9 * * *"
                    new_crons = []
                    for t in time_input.split(","):
                         t = t.strip()
                         if ":" in t:
                             parts = t.split(":")
                             h, m = int(parts[0]), int(parts[1])
                             new_crons.append(f"{m} {h} * * *")
                         else:
                             # 假设是小时
                             new_crons.append(f"0 {int(t)} * * *")
                    
                    final_cron_str = ", ".join(new_crons)
                    
                    if self.on_action:
                         await self.on_action("update_schedule", final_cron_str)
                         await self._send_message(f"✅ 定时已更新为: {final_cron_str}", "private", sender_id)
                    else:
                         await self._send_message("❌ 无法更新调度", "private", sender_id)
                         
                except Exception as e:
                    await self._send_message(f"❌ 设置失败: {e}", "private", sender_id)
                return

            # --- /help ---
            elif cmd == "/help":
                help_text = (
                    "🤖 Bot 指令帮助\n\n"
                    "/push - 🚀 立即推送\n"
                    "/xp - 🎯 查看 XP 画像\n"
                    "/stats - 📈 策略表现\n"
                    "/schedule - ⏰ 调整时间\n"
                    "/block - 🚫 屏蔽标签\n"
                    "/unblock - ✅ 取消屏蔽标签\n"
                    "/block_artist - 🚫 屏蔽画师\n"
                    "/unblock_artist - ✅ 取消屏蔽画师\n"
                    "/help - ℹ️ 显示此帮助"
                )
                await self._send_message(help_text, "private", sender_id)
                return

            # --- /block_artist ---
            elif cmd == "/block_artist":
                if not args:
                    try:
                        from database import get_blocked_artists
                        blocked = await get_blocked_artists()
                        if blocked:
                            lines = ["🚫 当前屏蔽的画师:"]
                            for artist_id, name in blocked:
                                lines.append(f"  • {artist_id} ({name})")
                            await self._send_message("\n".join(lines), "private", sender_id)
                        else:
                            await self._send_message("🚫 屏蔽列表为空\n用法: /block_artist <画师ID> [画师名]", "private", sender_id)
                    except Exception as e:
                        await self._send_message(f"❌ 查询失败: {e}", "private", sender_id)
                    return
                
                try:
                    artist_id = int(args[0])
                    artist_name = " ".join(args[1:]).strip() if len(args) > 1 else None
                    
                    from database import block_artist
                    await block_artist(artist_id, artist_name)
                    await self._send_message(f"✅ 已屏蔽画师: {artist_id}" + (f" ({artist_name})" if artist_name else ""), "private", sender_id)
                except ValueError:
                    await self._send_message("❌ 画师 ID 必须是数字", "private", sender_id)
                except Exception as e:
                    await self._send_message(f"❌ 屏蔽失败: {e}", "private", sender_id)
                return

            # --- /unblock_artist ---
            elif cmd == "/unblock_artist":
                if not args:
                    await self._send_message("用法: /unblock_artist <画师ID>", "private", sender_id)
                    return
                
                try:
                    artist_id = int(args[0])
                    
                    from database import unblock_artist
                    result = await unblock_artist(artist_id)
                    if result:
                        await self._send_message(f"✅ 已取消屏蔽画师: {artist_id}", "private", sender_id)
                    else:
                        await self._send_message(f"⚠️ 该画师未在屏蔽列表中: {artist_id}", "private", sender_id)
                except ValueError:
                    await self._send_message("❌ 画师 ID 必须是数字", "private", sender_id)
                except Exception as e:
                    await self._send_message(f"❌ 取消屏蔽失败: {e}", "private", sender_id)
                return

        # 解析反馈命令：ID 1 = 喜欢，ID 2 = 不喜欢
        # 支持格式：
        #   123456 1   (喜欢作品 123456)
        #   123456 2   (不喜欢作品 123456)
        parts = raw_message.split()
        if len(parts) == 2:
            try:
                illust_id = int(parts[0])
                action_code = parts[1]
                
                if action_code == "1":
                    await self.handle_feedback(illust_id, "like")
                    # 回复到私聊（主人）
                    await self._send_message(f"❤️ 已记录对作品 {illust_id} 的喜欢", "private", sender_id)
                    return
                elif action_code == "2":
                    await self.handle_feedback(illust_id, "dislike")
                    await self._send_message(f"👎 已记录对作品 {illust_id} 的不喜欢", "private", sender_id)
                    return
            except ValueError:
                pass
    
    async def stop_listening(self):
        """停止监听"""
        self._running = False
