"""Bounded, per-process budgets for the two anonymous service commands."""
import json
import time


class PublicRateLimitMiddleware:
    LIMITS = {("GET", "/api/health"): 600, ("POST", "/api/route"): 60}

    def __init__(self, app, clock=time.monotonic):
        self.app = app
        self.clock = clock
        self.windows = {}

    async def __call__(self, scope, receive, send):
        key = (scope.get("method"), scope.get("path"))
        if scope["type"] != "http" or key not in self.LIMITS:
            return await self.app(scope, receive, send)
        now = self.clock()
        start, count = self.windows.get(key, (now, 0))
        if now - start >= 60:
            start, count = now, 0
        limit = self.LIMITS[key]
        headers = [(b"x-ratelimit-limit", str(limit).encode()),
                   (b"x-ratelimit-remaining", str(max(0, limit-count-1)).encode())]
        if count >= limit:
            headers += [(b"retry-after", str(max(1, int(60-(now-start))+1)).encode()),
                        (b"content-type", b"application/json")]
            await send({"type": "http.response.start", "status": 429, "headers": headers})
            await send({"type": "http.response.body", "body": json.dumps({"detail": "Public API rate limit exceeded"}).encode()})
            return
        self.windows[key] = (start, count + 1)

        async def limited_send(message):
            if message["type"] == "http.response.start":
                message = dict(message, headers=list(message.get("headers", [])) + headers)
            await send(message)
        await self.app(scope, receive, limited_send)
