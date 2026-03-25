"""다중 SSE 클라이언트에 로그를 브로드캐스트 (asyncio.Queue 기반)"""

import asyncio


class LogBroadcaster:
    def __init__(self):
        self._subscribers: list[asyncio.Queue] = []

    def subscribe(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=1000)
        self._subscribers.append(q)
        return q

    def unsubscribe(self, q: asyncio.Queue):
        if q in self._subscribers:
            self._subscribers.remove(q)

    async def broadcast(self, log: dict):
        for q in self._subscribers:
            if q.full():
                try:
                    q.get_nowait()  # 오래된 로그 드롭
                except asyncio.QueueEmpty:
                    pass
            try:
                q.put_nowait(log)
            except asyncio.QueueFull:
                pass


# 싱글턴
broadcaster = LogBroadcaster()
