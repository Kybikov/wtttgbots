from __future__ import annotations

import argparse
import asyncio

from uvicorn import run as uvicorn_run

from .bot_registry import BotRegistry
from .config import get_settings
from .db import create_pool
from .repository import TelegramRepository


async def sync_webhooks(bot_id: str | None) -> None:
    settings = get_settings()
    if not settings.public_base_url:
        raise RuntimeError("PUBLIC_BASE_URL is required to sync webhooks")

    pool = await create_pool(settings)
    repository = TelegramRepository(pool)
    registry = BotRegistry()

    try:
        bots = await repository.list_bots_for_webhook_sync(bot_id)
        if not bots:
            print("No telegram bots found for webhook sync.")
            return

        for bot_record in bots:
            bot = await registry.get_bot(bot_record)
            webhook_url = settings.build_webhook_url(bot_record.id)
            await bot.set_webhook(
                url=webhook_url,
                secret_token=bot_record.webhook_secret,
                allowed_updates=["message"]
            )
            await repository.mark_webhook_sync(bot_record.id)
            print(f"Synced webhook for {bot_record.name}: {webhook_url}")
    finally:
        await registry.close()
        await pool.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="wttelegram-runtime")
    subparsers = parser.add_subparsers(dest="command", required=True)

    serve_parser = subparsers.add_parser("serve", help="Run the FastAPI webhook runtime")
    serve_parser.set_defaults(command="serve")

    sync_parser = subparsers.add_parser("sync-webhooks", help="Register Telegram webhooks for bots from DB")
    sync_parser.add_argument("--bot-id", dest="bot_id", default=None, help="Sync one bot by UUID")
    sync_parser.set_defaults(command="sync-webhooks")

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    settings = get_settings()

    if args.command == "serve":
        uvicorn_run(
            "wttelegram_runtime.app:app",
            host=settings.host,
            port=settings.port,
            factory=False
        )
        return

    if args.command == "sync-webhooks":
        asyncio.run(sync_webhooks(args.bot_id))
        return

    parser.error("Unknown command")
