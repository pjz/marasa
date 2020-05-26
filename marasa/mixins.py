import asyncio
import threading

class AsyncSafeLogMixin:

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self.writelock = asyncio.Lock()

    async def put(self, *a, **kw):
        async with self.writelock:
            return super().put(*a, **kw)


class ThreadSafeLogMixin:
    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self.writelock = threading.Lock()

    def put(self, *a, **kw):
        with self.writelock:
            return super().put(*a, **kw)


