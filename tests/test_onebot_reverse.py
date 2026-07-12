import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock

import aiohttp

from notifier.onebot import OneBotNotifier


class OneBotReverseWebSocketTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.feedback = AsyncMock()
        self.notifier = OneBotNotifier(
            mode="reverse",
            reverse_config={
                "host": "127.0.0.1",
                "port": 0,
                "path": "/onebot/v11/ws",
                "access_token": "test-secret",
                "connection_timeout": 0.1,
            },
            private_id=123456,
            master_id=123456,
            on_feedback=self.feedback,
        )
        await self.notifier.start_reverse_server()
        sockets = self.notifier._site._server.sockets
        self.port = sockets[0].getsockname()[1]
        self.url = f"http://127.0.0.1:{self.port}/onebot/v11/ws"
        self.session = aiohttp.ClientSession()

    async def asyncTearDown(self):
        await self.notifier.close()
        await self.session.close()

    async def _connect(self, token="test-secret", query=False):
        if query:
            return await self.session.ws_connect(f"{self.url}?access_token={token}")
        return await self.session.ws_connect(
            self.url, headers={"Authorization": f"Bearer {token}"}
        )

    async def test_rejects_missing_and_invalid_tokens(self):
        response = await self.session.get(self.url)
        self.assertEqual(response.status, 401)
        await response.release()

        response = await self.session.get(
            self.url, headers={"Authorization": "Bearer wrong-secret"}
        )
        self.assertEqual(response.status, 401)
        await response.release()

    async def test_accepts_query_token_and_routes_echo_response(self):
        ws = await self._connect(query=True)
        request_task = asyncio.create_task(
            self.notifier._call_api({"action": "get_status", "params": {}}, timeout=1)
        )
        command = await ws.receive_json(timeout=1)
        self.assertEqual(command["action"], "get_status")
        await ws.send_json({"status": "ok", "data": {"online": True}, "echo": command["echo"]})
        response = await request_task
        self.assertTrue(response["data"]["online"])
        await ws.close()

    async def test_routes_feedback_event_and_command_reply(self):
        ws = await self._connect()
        await ws.send_json(
            {
                "post_type": "message",
                "message_type": "private",
                "user_id": 123456,
                "sender": {"user_id": 123456},
                "raw_message": "987654 1",
            }
        )

        for _ in range(20):
            if self.feedback.await_count:
                break
            await asyncio.sleep(0.01)
        self.feedback.assert_awaited_once_with(987654, "like")

        reply_command = await ws.receive_json(timeout=1)
        self.assertEqual(reply_command["action"], "send_private_msg")
        await ws.send_json(
            {
                "status": "ok",
                "data": {"message_id": 42},
                "echo": reply_command["echo"],
            }
        )
        await asyncio.sleep(0.01)
        await ws.close()

    async def test_new_connection_replaces_old_connection(self):
        old_ws = await self._connect()
        new_ws = await self._connect(query=True)
        message = await old_ws.receive(timeout=1)
        self.assertIn(message.type, (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED))
        self.assertFalse(new_ws.closed)
        await new_ws.close()

    async def test_disconnect_fails_pending_request(self):
        ws = await self._connect()
        request_task = asyncio.create_task(
            self.notifier._call_api({"action": "get_status", "params": {}}, timeout=5)
        )
        await ws.receive_json(timeout=1)
        await ws.close()
        with self.assertRaises(ConnectionError):
            await request_task

    async def test_merge_send_and_chain_push_use_reverse_connection(self):
        ws = await self._connect()
        self.notifier._prepare_illust_content = AsyncMock(
            side_effect=["first", "second", "chain"]
        )
        first = SimpleNamespace(id=1)
        second = SimpleNamespace(id=2)
        chain = SimpleNamespace(id=3)

        send_task = asyncio.create_task(self.notifier.send([first, second]))
        merge_command = await ws.receive_json(timeout=1)
        self.assertEqual(merge_command["action"], "send_private_forward_msg")
        self.assertEqual(len(merge_command["params"]["messages"]), 2)
        await ws.send_json(
            {
                "status": "ok",
                "data": {"message_id": 50},
                "echo": merge_command["echo"],
            }
        )
        self.assertEqual(await send_task, [1, 2])

        chain_task = asyncio.create_task(
            self.notifier.push_illusts([chain], message_prefix="连锁推荐")
        )
        chain_command = await ws.receive_json(timeout=1)
        self.assertEqual(chain_command["action"], "send_private_msg")
        self.assertIn("连锁推荐", chain_command["params"]["message"])
        await ws.send_json(
            {
                "status": "ok",
                "data": {"message_id": 51},
                "echo": chain_command["echo"],
            }
        )
        self.assertEqual(await chain_task, {3: 51})
        self.assertEqual(self.notifier._message_illust_map[51], 3)
        await ws.close()

    async def test_merge_failure_falls_back_to_individual_messages(self):
        ws = await self._connect()
        self.notifier._prepare_illust_content = AsyncMock(
            side_effect=["first", "second"]
        )
        illusts = [SimpleNamespace(id=10), SimpleNamespace(id=11)]

        send_task = asyncio.create_task(self.notifier.send(illusts))
        merge_command = await ws.receive_json(timeout=1)
        await ws.send_json(
            {
                "status": "failed",
                "message": "forward unsupported",
                "echo": merge_command["echo"],
            }
        )

        for message_id in (60, 61):
            command = await ws.receive_json(timeout=3)
            self.assertEqual(command["action"], "send_private_msg")
            await ws.send_json(
                {
                    "status": "ok",
                    "data": {"message_id": message_id},
                    "echo": command["echo"],
                }
            )

        self.assertEqual(await send_task, [10, 11])
        await ws.close()

    async def test_request_times_out_without_lagrange_connection(self):
        with self.assertRaises(TimeoutError):
            await self.notifier._call_api(
                {"action": "get_status", "params": {}}, timeout=1
            )

    async def test_forward_mode_remains_default(self):
        forward = OneBotNotifier(ws_url="ws://127.0.0.1:3001")
        self.assertEqual(forward.mode, "forward")
        await forward.close()

    async def test_reverse_mode_requires_access_token(self):
        with self.assertRaises(ValueError):
            OneBotNotifier(mode="reverse", reverse_config={"port": 8765})


if __name__ == "__main__":
    unittest.main()
