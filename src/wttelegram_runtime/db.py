import ssl

import asyncpg

from .config import Settings


async def create_pool(settings: Settings) -> asyncpg.Pool:
    if not settings.database_url:
        raise RuntimeError("DATABASE_URL is required")

    ssl_context = None
    if settings.database_ssl:
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

    return await asyncpg.create_pool(
        dsn=settings.database_url,
        ssl=ssl_context,
        min_size=1,
        max_size=10
    )
