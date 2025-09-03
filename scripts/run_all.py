import argparse
import asyncio
import signal
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
LOGS = ROOT / "logs"

async def run_process(cmd):
    return await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL)

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--duration", type=int, default=300)
    parser.add_argument("--port", type=int, default=9999)
    args = parser.parse_args()

    LOGS.mkdir(parents=True, exist_ok=True)
    for name in ["server.log", "client1.log", "client2.log"]:
        try:
            (LOGS / name).unlink(missing_ok=True)
        except Exception:
            pass

    server_cmd = [sys.executable, str(SRC / "server.py"), "--port", str(args.port), "--log", str(LOGS / "server.log")]
    client1_cmd = [sys.executable, str(SRC / "client.py"), "--port", str(args.port), "--log", str(LOGS / "client1.log")]
    client2_cmd = [sys.executable, str(SRC / "client.py"), "--port", str(args.port), "--log", str(LOGS / "client2.log")]

    server = await run_process(server_cmd)
    await asyncio.sleep(0.5)
    client1 = await run_process(client1_cmd)
    client2 = await run_process(client2_cmd)

    try:
        await asyncio.sleep(args.duration)
    finally:
        for proc in [client1, client2, server]:
            if proc and proc.returncode is None:
                try:
                    proc.send_signal(signal.SIGINT)
                except ProcessLookupError:
                    pass
        await asyncio.gather(*(p.wait() for p in [client1, client2, server] if p), return_exceptions=True)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
