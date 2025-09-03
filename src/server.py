import asyncio
import argparse
import os
import random
import logging
from datetime import datetime
from typing import Dict, Optional

DATE_FMT = "%Y-%m-%d"
TIME_FMT = "%H:%M:%S.%f"

def format_ts(dt: datetime) -> str:
    return dt.strftime(TIME_FMT)[:-3]

class PingPongServer:
    """Async TCP server handling PING/PONG protocol with keepalive.

    Attributes:
        ignore_prob: Probability [0..1] to ignore an incoming request.
        delay_min_ms/delay_max_ms: Bounds for artificial response delay.
        keepalive_sec: Interval for broadcasting keepalive to all clients.
    """
    def __init__(self, host: str, port: int, log_path: str,
                 ignore_prob: float = 0.10,
                 delay_min_ms: int = 100,
                 delay_max_ms: int = 1000,
                 keepalive_sec: int = 5) -> None:
        self.host = host
        self.port = port
        self.log_path = log_path
        self.ignore_prob = ignore_prob
        self.delay_min_ms = delay_min_ms
        self.delay_max_ms = delay_max_ms
        self.keepalive_sec = keepalive_sec
        self.server: Optional[asyncio.AbstractServer] = None
        self.next_client_no = 1
        self.writers: Dict[int, asyncio.StreamWriter] = {}
        self.response_index = 0
        self.keepalive_task: Optional[asyncio.Task] = None
        self.lock = asyncio.Lock()

    async def start(self) -> None:
        """Start listening and spawn keepalive loop."""
        self.server = await asyncio.start_server(self.handle_client, self.host, self.port)
        self.keepalive_task = asyncio.create_task(self.keepalive_loop())

    async def stop(self) -> None:
        """Gracefully stop keepalive, close server and client sockets."""
        if self.keepalive_task:
            self.keepalive_task.cancel()
            try:
                await self.keepalive_task
            except asyncio.CancelledError:
                pass
        if self.server:
            self.server.close()
            await self.server.wait_closed()
        for writer in list(self.writers.values()):
            try:
                writer.close()
            except Exception:
                logging.exception("writer.close failed")
        await asyncio.gather(*(w.wait_closed() for w in list(self.writers.values())), return_exceptions=True)

    async def keepalive_loop(self) -> None:
        """Periodically broadcast keepalive to all connected clients."""
        try:
            while True:
                await asyncio.sleep(self.keepalive_sec)
                await self.broadcast_keepalive()
        except asyncio.CancelledError:
            return

    async def broadcast_keepalive(self) -> None:
        text = await self.allocate_response_and_format_keepalive()
        data = (text + "\n").encode("ascii")
        to_remove = []
        for client_no, writer in self.writers.items():
            try:
                writer.write(data)
                await writer.drain()
            except Exception:
                logging.exception("keepalive send failed for client %s", client_no)
                to_remove.append(client_no)
        for client_no in to_remove:
            self.writers.pop(client_no, None)

    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        client_no = self.next_client_no
        self.next_client_no += 1
        self.writers[client_no] = writer
        try:
            while True:
                line = await reader.readline()
                if not line:
                    break
                received_at = datetime.now()
                try:
                    text = line.decode("ascii").rstrip("\n")
                except UnicodeDecodeError:
                    logging.exception("non-ascii data from client %s", client_no)
                    continue
                await self.process_request(text, client_no, writer, received_at)
        except asyncio.CancelledError:
            pass
        except Exception:
            logging.exception("error in client handler %s", client_no)
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                logging.exception("writer close failed for client %s", client_no)
            self.writers.pop(client_no, None)

    async def process_request(self, text: str, client_no: int, writer: asyncio.StreamWriter, received_at: datetime) -> None:
        """Process single request: maybe ignore, else delay and reply, log both."""
        date_str = received_at.strftime(DATE_FMT)
        time_received = format_ts(received_at)
        is_ignored = random.random() < self.ignore_prob
        if is_ignored:
            await self.append_server_log(date_str, time_received, text, "(проигнорировано)", "(проигнорировано)")
            return
        delay_ms = random.randint(self.delay_min_ms, self.delay_max_ms)
        await asyncio.sleep(delay_ms / 1000.0)
        response_text = await self.allocate_response_and_format_pong(text, client_no)
        time_sent = format_ts(datetime.now())
        try:
            writer.write((response_text + "\n").encode("ascii"))
            await writer.drain()
        except Exception:
            logging.exception("send PONG failed for client %s", client_no)
        await self.append_server_log(date_str, time_received, text, time_sent, response_text)

    async def allocate_response_and_format_pong(self, request_text: str, client_no: int) -> str:
        req_index = self._extract_request_index(request_text)
        async with self.lock:
            resp_index = self.response_index
            self.response_index += 1
        return f"[{resp_index}/{req_index}] PONG ({client_no})"

    async def allocate_response_and_format_keepalive(self) -> str:
        async with self.lock:
            resp_index = self.response_index
            self.response_index += 1
        return f"[{resp_index}] keepalive"

    @staticmethod
    def _extract_request_index(text: str) -> int:
        if text.startswith("["):
            try:
                right = text.index("]")
                inside = text[1:right]
                return int(inside)
            except Exception:
                return -1
        return -1

    async def append_server_log(self, date_str: str, time_received: str, request_text: str, time_sent: str, response_text: str) -> None:
        line = f"{date_str};{time_received};{request_text};{time_sent};{response_text}\n"
        async with self.lock:
            await asyncio.to_thread(self._write_log_line, self.log_path, line)

    @staticmethod
    def _write_log_line(path: str, line: str) -> None:
        with open(path, "a", encoding="utf-8") as f:
            f.write(line)

async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default=os.getenv("SERVER_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.getenv("SERVER_PORT", "9999")))
    parser.add_argument("--log", default=os.getenv("SERVER_LOG", "logs/server.log"))
    parser.add_argument("--ignore_prob", type=float, default=float(os.getenv("SERVER_IGNORE_PROB", "0.10")))
    parser.add_argument("--delay_min_ms", type=int, default=int(os.getenv("SERVER_DELAY_MIN_MS", "100")))
    parser.add_argument("--delay_max_ms", type=int, default=int(os.getenv("SERVER_DELAY_MAX_MS", "1000")))
    parser.add_argument("--keepalive_sec", type=int, default=int(os.getenv("SERVER_KEEPALIVE_SEC", "5")))
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    server = PingPongServer(
        args.host,
        args.port,
        args.log,
        args.ignore_prob,
        args.delay_min_ms,
        args.delay_max_ms,
        args.keepalive_sec,
    )
    await server.start()
    try:
        await asyncio.Event().wait()
    except asyncio.CancelledError:
        pass
    finally:
        await server.stop()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
