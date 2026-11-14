import unittest
from server.rate_limit import PublicRateLimitMiddleware


class RateLimitTests(unittest.IsolatedAsyncioTestCase):
    async def test_budget_reset_and_other_endpoints(self):
        now = [0.0]
        async def app(scope, receive, send):
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send({"type": "http.response.body", "body": b"ok"})
        middleware = PublicRateLimitMiddleware(app, clock=lambda: now[0])
        async def request(path="/api/route", method="POST"):
            output = []
            async def send(message):
                output.append(message)
            await middleware({"type": "http", "method": method, "path": path}, None, send)
            return output[0]
        for _ in range(60):
            self.assertEqual((await request())["status"], 200)
        rejected = await request()
        self.assertEqual(rejected["status"], 429)
        self.assertIn(b"retry-after", dict(rejected["headers"]))
        self.assertEqual((await request("/api/trips"))["status"], 200)
        self.assertEqual((await request("/api/health", "GET"))["status"], 200)
        now[0] = 60
        self.assertEqual((await request())["status"], 200)
