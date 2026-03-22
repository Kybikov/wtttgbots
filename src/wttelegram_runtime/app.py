from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from typing import Any

from aiogram import Dispatcher
from aiogram.filters import CommandStart
from aiogram.types import Message, Update
from fastapi import FastAPI, Header, HTTPException, Request

from .bot_registry import BotRegistry
from .config import Settings, get_settings
from .db import create_pool
from .delivery import send_step, serialize_step
from .repository import TelegramRepository


def register_handlers(dispatcher: Dispatcher, repository: TelegramRepository) -> None:
    @dispatcher.message(CommandStart())
    async def handle_start(message: Message, bot_record: Any) -> None:
        chat_id = message.chat.id
        steps = await repository.get_steps_for_trigger(bot_record.id, "start")

        await repository.record_event(
            bot_id=bot_record.id,
            chat_id=chat_id,
            event_type="command.start",
            payload={
                "username": message.from_user.username if message.from_user else "",
                "message_id": message.message_id
            }
        )

        if not steps:
            await message.answer("Сценарій ще не налаштований. Поверніться трохи пізніше.")
            return

        await repository.cancel_pending_jobs(bot_record.id, chat_id)

        queued_jobs: list[dict[str, Any]] = []
        now = datetime.now(UTC)
        for step in steps:
            payload = serialize_step(step, bot_record)
            if step.delay_seconds <= 0:
                await send_step(message.bot, bot_record, chat_id, payload)
                await repository.record_event(
                    bot_id=bot_record.id,
                    chat_id=chat_id,
                    event_type="step.sent.immediate",
                    payload={
                        "step_id": step.id,
                        "step_key": step.step_key,
                        "trigger_key": step.trigger_key
                    }
                )
                continue

            queued_jobs.append(
                {
                    "step_id": step.id,
                    "trigger_key": step.trigger_key,
                    "run_at": now + timedelta(seconds=step.delay_seconds),
                    "message_payload": payload
                }
            )

        await repository.enqueue_delivery_jobs(
            bot_id=bot_record.id,
            chat_id=chat_id,
            jobs=queued_jobs
        )


async def run_delivery_worker(
    *,
    repository: TelegramRepository,
    registry: BotRegistry,
    settings: Settings
) -> None:
    while True:
        jobs = await repository.claim_due_jobs(settings.delivery_batch_size)
        if not jobs:
            await asyncio.sleep(settings.delivery_poll_interval)
            continue

        for job in jobs:
            try:
                bot = await registry.get_bot(job.bot)
                await send_step(bot, job.bot, job.chat_id, job.message_payload)
                await repository.mark_job_sent(job.id)
                await repository.record_event(
                    bot_id=job.bot.id,
                    chat_id=job.chat_id,
                    event_type="step.sent.delayed",
                    payload={
                        "job_id": job.id,
                        "step_id": job.step_id,
                        "trigger_key": job.trigger_key
                    }
                )
            except Exception as error:  # noqa: BLE001
                await repository.retry_or_fail_job(
                    job.id,
                    attempts=job.attempts,
                    max_attempts=settings.max_job_attempts,
                    error_message=str(error)
                )
                await repository.record_event(
                    bot_id=job.bot.id,
                    chat_id=job.chat_id,
                    event_type="step.failed",
                    payload={
                        "job_id": job.id,
                        "step_id": job.step_id,
                        "attempts": job.attempts,
                        "error": str(error)
                    }
                )

        await asyncio.sleep(0)


def create_app() -> FastAPI:
    settings = get_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        pool = await create_pool(settings)
        repository = TelegramRepository(pool)
        registry = BotRegistry()
        dispatcher = Dispatcher()
        register_handlers(dispatcher, repository)
        worker_task = asyncio.create_task(
            run_delivery_worker(repository=repository, registry=registry, settings=settings)
        )

        app.state.repository = repository
        app.state.registry = registry
        app.state.dispatcher = dispatcher
        app.state.worker_task = worker_task

        try:
            yield
        finally:
            worker_task.cancel()
            try:
                await worker_task
            except asyncio.CancelledError:
                pass
            await registry.close()
            await pool.close()

    app = FastAPI(title="WTtelegram Runtime", lifespan=lifespan)

    @app.get("/healthz")
    async def healthcheck():
        repository: TelegramRepository = app.state.repository
        return {
            "ok": True,
            "activeBots": await repository.count_active_bots()
        }

    @app.post(f"{settings.webhook_path_prefix}" + "/{bot_id}")
    async def telegram_webhook(
        bot_id: str,
        request: Request,
        x_telegram_bot_api_secret_token: str | None = Header(default=None)
    ):
        repository: TelegramRepository = app.state.repository
        registry: BotRegistry = app.state.registry
        dispatcher: Dispatcher = app.state.dispatcher

        bot_record = await repository.get_bot_by_id(bot_id)
        if not bot_record or bot_record.status != "active":
            raise HTTPException(status_code=404, detail="Bot not found")

        if x_telegram_bot_api_secret_token != bot_record.webhook_secret:
            await repository.record_event(
                bot_id=bot_id,
                chat_id=None,
                event_type="webhook.rejected",
                payload={"reason": "invalid_secret"}
            )
            raise HTTPException(status_code=403, detail="Forbidden")

        payload = await request.json()
        bot = await registry.get_bot(bot_record)
        update = Update.model_validate(payload, context={"bot": bot})
        await dispatcher.feed_update(bot, update, bot_record=bot_record)
        return {"ok": True}

    return app


app = create_app()
