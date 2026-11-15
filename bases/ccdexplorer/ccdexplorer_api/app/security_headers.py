"""Headers every api response carries.

The api origin serves the account pages -- login, password reset, plans, keys
-- so it is browser-facing as much as the site is. Reset and verification
tokens travel in url paths here, which is why the referrer policy is the one
that matters most: without it, following a link off such a page tells the
destination the token.

Same bar as the site's set: each is one no page here can be broken by.
There is no script-src, for the reason the site's comment gives.
"""

from starlette.datastructures import MutableHeaders

API_SECURITY_HEADERS = {
    "X-Content-Type-Options": "nosniff",
    "Referrer-Policy": "no-referrer",
    "Content-Security-Policy": "object-src 'none'; base-uri 'self'; frame-ancestors 'self'",
    # A year, and no includeSubDomains or preload: those make a promise on
    # behalf of hostnames this app does not own.
    "Strict-Transport-Security": "max-age=31536000",
}


class SecurityHeadersMiddleware:
    """Adds API_SECURITY_HEADERS to every http response.

    Plain ASGI, so it never sits between a streamed body and its client. A
    header a route set itself is left as the route set it.
    """

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        async def send_with_headers(message):
            if message["type"] == "http.response.start":
                headers = MutableHeaders(scope=message)
                for name, value in API_SECURITY_HEADERS.items():
                    if name not in headers:
                        headers[name] = value
            await send(message)

        await self.app(scope, receive, send_with_headers)
