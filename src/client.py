import asyncio
import argparse
import os
import random
import logging
from datetime import datetime
from typing import Optional, Tuple

DATE_FMT = "%Y-%m-%d"
TIME_FMT = "%H:%M:%S.%f"

def format_ts(dt: datetime) -> str:
    return dt.strftime(TIME_FMT)[:-3]

class Client:
    """Async client sending PINGs and receiving PONG/keepalive over a TCP stream.

    Attributes:
        timeout_ms: per-request timeout for PONG.
        send_min_ms/send_max_ms: bounds for PING interval.
    """
    def __init__(self, host: str, port: int, log_path: str, timeout_ms: int = 2000,
                 send_min_ms: int = 300, send_max_ms: int = 3000) -> None:
        self.host = host
        self.port = port
        self.log_path = log_path
        self.timeout_ms = timeout_ms
        self.send_min_ms = send_min_ms
        self.send_max_ms = send_max_ms
        self.reader: Optional[asyncio.StreamReader] = None
        self.writer: Optional[asyncio.StreamWriter] = None
        self.next_request_index = 0
        self.pending: dict[int, Tuple[str, str]] = {}
        self.lock = asyncio.Lock()
        self.running = True

    async def connect(self) -> None:
        self.reader, self.writer = await asyncio.open_connection(self.host, self.port)

    async def close(self) -> None:
        self.running = False
        if self.writer:
            try:
                self.writer.close()
                await self.writer.wait_closed()
            except Exception:
                logging.exception("writer close failed")

    async def send_loop(self) -> None:
        assert self.writer is not None
        try:
            while self.running:
                await asyncio.sleep(random.randint(self.send_min_ms, self.send_max_ms) / 1000.0)
                await self.send_ping()
        except asyncio.CancelledError:
            return

    async def send_ping(self) -> None:
        assert self.writer is not None
        req_index = self.next_request_index
        self.next_request_index += 1
        text = f"[{req_index}] PING"
        now = datetime.now()
        date_str = now.strftime(DATE_FMT)
        time_sent = format_ts(now)
        try:
            self.writer.write((text + "\n").encode("ascii"))
            await self.writer.drain()
        except Exception:
            logging.exception("send PING failed")
            return
        async with self.lock:
            self.pending[req_index] = (date_str, time_sent)
        asyncio.create_task(self.handle_timeout(req_index))

    async def handle_timeout(self, req_index: int) -> None:
        await asyncio.sleep(self.timeout_ms / 1000.0)
        async with self.lock:
            if req_index in self.pending:
                date_str, time_sent = self.pending.pop(req_index)
                await self.append_client_log(date_str, time_sent, f"[{req_index}] PING", format_ts(datetime.now()), "(таймаут)")

    async def recv_loop(self) -> None:
        assert self.reader is not None
        try:
            while self.running:
                line = await self.reader.readline()
                if not line:
                    break
                now = datetime.now()
                try:
                    text = line.decode("ascii").rstrip("\n")
                except UnicodeDecodeError:
                    logging.exception("non-ascii data from server")
                    continue
                await self.handle_incoming(text, now)
        except asyncio.CancelledError:
            return
        except Exception:
            logging.exception("error in recv_loop")

    async def handle_incoming(self, text: str, received_at: datetime) -> None:
        """Handle PONG and keepalive; ensure single log line per request."""
        if text.endswith(" keepalive"):
            date_str = received_at.strftime(DATE_FMT)
            time_recv = format_ts(received_at)
            await self.append_client_log(date_str, "", "", time_recv, text)
            return
        req_index = self._extract_req_index_from_pong(text)
        if req_index is None:
            return
        async with self.lock:
            meta = self.pending.pop(req_index, None)
        if meta is None:
            return
        date_str, time_sent = meta
        await self.append_client_log(date_str, time_sent, f"[{req_index}] PING", format_ts(received_at), text)

    @staticmethod
    def _extract_req_index_from_pong(text: str) -> Optional[int]:
        try:
            if not text.startswith("["):
                return None
            right = text.index("]")
            inside = text[1:right]
            if "/" not in inside:
                return None
            _resp_str, req_str = inside.split("/", 1)
            return int(req_str)
        except Exception:
            return None

    async def append_client_log(self, date_str: str, time_sent: str, request_text: str, time_recv: str, response_text: str) -> None:
        line = f"{date_str};{time_sent};{request_text};{time_recv};{response_text}\n"
        await asyncio.to_thread(self._write_log_line, self.log_path, line)

    @staticmethod
    def _write_log_line(path: str, line: str) -> None:
        with open(path, "a", encoding="utf-8") as f:
            f.write(line)

async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default=os.getenv("CLIENT_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.getenv("CLIENT_PORT", "9999")))
    parser.add_argument("--log", required=True)
    parser.add_argument("--timeout_ms", type=int, default=int(os.getenv("CLIENT_TIMEOUT_MS", "2000")))
    parser.add_argument("--send_min_ms", type=int, default=int(os.getenv("CLIENT_SEND_MIN_MS", "300")))
    parser.add_argument("--send_max_ms", type=int, default=int(os.getenv("CLIENT_SEND_MAX_MS", "3000")))
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    client = Client(args.host, args.port, args.log, args.timeout_ms, args.send_min_ms, args.send_max_ms)
    await client.connect()

    send_task = asyncio.create_task(client.send_loop())
    recv_task = asyncio.create_task(client.recv_loop())
    try:
        await asyncio.gather(send_task, recv_task)
    except asyncio.CancelledError:
        pass
    finally:
        await client.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
