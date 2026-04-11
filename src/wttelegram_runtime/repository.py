from __future__ import annotations

from datetime import UTC, datetime, timedelta
import json
from typing import Any

import asyncpg

from .models import BotRecord, ButtonRecord, DeliveryJob, ReminderDeliveryJob, StepRecord


def _map_bot(row: asyncpg.Record) -> BotRecord:
    return BotRecord(
        id=str(row["id"]),
        name=row["name"],
        public_title=row["public_title"] or "",
        bot_username=row["bot_username"] or "",
        bot_token=row["bot_token"],
        webhook_secret=row["webhook_secret"],
        template_key=row["template_key"] or "promo_funnel",
        status=row["status"] or "draft",
        default_parse_mode=row["default_parse_mode"] or "markdown",
        support_username=row["support_username"] or "",
        website_url=row["website_url"] or "",
        channel_url=row["channel_url"] or "",
        buy_url=row["buy_url"] or "",
        notes=row["notes"] or "",
        last_error=row["last_error"] or ""
    )


def _map_step(row: asyncpg.Record) -> StepRecord:
    return StepRecord(
        id=str(row["id"]),
        bot_id=str(row["bot_id"]),
        trigger_key=row["trigger_key"] or "start",
        step_key=row["step_key"] or "",
        sort_order=int(row["sort_order"] or 100),
        is_active=bool(row["is_active"]),
        delay_seconds=int(row["delay_seconds"] or 0),
        message_kind=row["message_kind"] or "text",
        title=row["title"] or "",
        body_text=row["body_text"] or "",
        media_url=row["media_url"] or "",
        parse_mode=row["parse_mode"] or "inherit",
        promo_code=row["promo_code"] or "",
        promo_note=row["promo_note"] or ""
    )


def _map_button(row: asyncpg.Record) -> ButtonRecord:
    return ButtonRecord(
        id=str(row["id"]),
        label=row["label"],
        action_type=row["action_type"] or "url",
        url=row["url"] or "",
        sort_order=int(row["sort_order"] or 100)
    )


class TelegramRepository:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self.pool = pool

    async def ping(self) -> None:
        await self.pool.execute("select 1")

    async def count_active_bots(self) -> int:
        row = await self.pool.fetchrow(
            """
            select count(*)::int as total
            from public.telegram_bots
            where status = 'active'
            """
        )
        return int(row["total"] if row else 0)

    async def get_bot_by_id(self, bot_id: str) -> BotRecord | None:
        row = await self.pool.fetchrow(
            """
            select
              id,
              name,
              public_title,
              bot_username,
              bot_token,
              webhook_secret,
              template_key,
              status,
              default_parse_mode,
              support_username,
              website_url,
              channel_url,
              buy_url,
              notes,
              last_error
            from public.telegram_bots
            where id = $1::uuid
            limit 1
            """,
            bot_id
        )
        return _map_bot(row) if row else None

    async def list_bots_for_webhook_sync(self, bot_id: str | None = None) -> list[BotRecord]:
        rows = await self.pool.fetch(
            """
            select
              id,
              name,
              public_title,
              bot_username,
              bot_token,
              webhook_secret,
              template_key,
              status,
              default_parse_mode,
              support_username,
              website_url,
              channel_url,
              buy_url,
              notes,
              last_error
            from public.telegram_bots
            where ($1::uuid is null or id = $1::uuid)
              and status in ('active', 'paused', 'draft')
            order by lower(name)
            """,
            bot_id
        )
        return [_map_bot(row) for row in rows]

    async def mark_webhook_sync(self, bot_id: str, error_message: str = "") -> None:
        await self.pool.execute(
            """
            update public.telegram_bots
            set
              last_webhook_sync_at = now(),
              last_error = $2,
              updated_at = now()
            where id = $1::uuid
            """,
            bot_id,
            str(error_message or "")
        )

    async def record_event(
        self,
        *,
        bot_id: str | None,
        chat_id: int | None,
        event_type: str,
        payload: dict[str, Any] | None = None
    ) -> None:
        await self.pool.execute(
            """
            insert into public.telegram_bot_events (bot_id, chat_id, event_type, payload)
            values ($1::uuid, $2, $3, $4::jsonb)
            """,
            bot_id,
            chat_id,
            event_type,
            json.dumps(payload or {})
        )

    async def get_steps_for_trigger(self, bot_id: str, trigger_key: str) -> list[StepRecord]:
        step_rows = await self.pool.fetch(
            """
            select
              id,
              bot_id,
              trigger_key,
              step_key,
              sort_order,
              is_active,
              delay_seconds,
              message_kind,
              title,
              body_text,
              media_url,
              parse_mode,
              promo_code,
              promo_note
            from public.telegram_bot_steps
            where bot_id = $1::uuid
              and trigger_key = $2
              and is_active = true
            order by sort_order asc, created_at asc
            """,
            bot_id,
            trigger_key
        )

        steps = [_map_step(row) for row in step_rows]
        if not steps:
            return []

        step_ids = [step.id for step in steps]
        button_rows = await self.pool.fetch(
            """
            select
              id,
              step_id,
              sort_order,
              label,
              action_type,
              url
            from public.telegram_bot_step_buttons
            where step_id = any($1::uuid[])
            order by sort_order asc, created_at asc
            """,
            step_ids
        )

        buttons_by_step: dict[str, list[ButtonRecord]] = {step.id: [] for step in steps}
        for row in button_rows:
            buttons_by_step[str(row["step_id"])].append(_map_button(row))

        for step in steps:
            step.buttons = buttons_by_step.get(step.id, [])

        return steps

    async def cancel_pending_jobs(self, bot_id: str, chat_id: int) -> None:
        await self.pool.execute(
            """
            update public.telegram_bot_delivery_jobs
            set
              status = 'cancelled',
              updated_at = now()
            where bot_id = $1::uuid
              and chat_id = $2
              and status in ('queued', 'processing')
            """,
            bot_id,
            chat_id
        )

    async def enqueue_delivery_jobs(
        self,
        *,
        bot_id: str,
        chat_id: int,
        jobs: list[dict[str, Any]]
    ) -> None:
        if not jobs:
            return

        async with self.pool.acquire() as connection:
            async with connection.transaction():
                for job in jobs:
                    await connection.execute(
                        """
                        insert into public.telegram_bot_delivery_jobs (
                          bot_id,
                          step_id,
                          chat_id,
                          trigger_key,
                          run_at,
                          message_payload
                        )
                        values ($1::uuid, $2::uuid, $3, $4, $5, $6::jsonb)
                        """,
                        bot_id,
                        job.get("step_id"),
                        chat_id,
                        job.get("trigger_key", "start"),
                        job["run_at"],
                        json.dumps(job.get("message_payload") or {})
                    )

    async def claim_due_jobs(self, limit: int) -> list[DeliveryJob]:
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                rows = await connection.fetch(
                    """
                    with due as (
                      select id
                      from public.telegram_bot_delivery_jobs
                      where status = 'queued'
                        and run_at <= now()
                      order by run_at asc, created_at asc
                      for update skip locked
                      limit $1
                    ),
                    claimed as (
                      update public.telegram_bot_delivery_jobs job
                      set
                        status = 'processing',
                        attempts = job.attempts + 1,
                        updated_at = now()
                      from due
                      where job.id = due.id
                      returning
                        job.id,
                        job.bot_id,
                        job.step_id,
                        job.chat_id,
                        job.trigger_key,
                        job.run_at,
                        job.attempts,
                        job.message_payload
                    )
                    select
                      claimed.id,
                      claimed.step_id,
                      claimed.chat_id,
                      claimed.trigger_key,
                      claimed.run_at,
                      claimed.attempts,
                      claimed.message_payload,
                      jsonb_build_object(
                        'id', b.id,
                        'name', b.name,
                        'public_title', b.public_title,
                        'bot_username', b.bot_username,
                        'bot_token', b.bot_token,
                        'webhook_secret', b.webhook_secret,
                        'template_key', b.template_key,
                        'status', b.status,
                        'default_parse_mode', b.default_parse_mode,
                        'support_username', b.support_username,
                        'website_url', b.website_url,
                        'channel_url', b.channel_url,
                        'buy_url', b.buy_url,
                        'notes', b.notes,
                        'last_error', b.last_error
                      ) as bot_payload
                    from claimed
                    join public.telegram_bots b on b.id = claimed.bot_id
                    """,
                    limit
                )

        jobs: list[DeliveryJob] = []
        for row in rows:
            bot_payload = row["bot_payload"]
            if not bot_payload:
                continue

            bot = BotRecord(
                id=str(bot_payload["id"]),
                name=bot_payload["name"],
                public_title=bot_payload["public_title"] or "",
                bot_username=bot_payload["bot_username"] or "",
                bot_token=bot_payload["bot_token"],
                webhook_secret=bot_payload["webhook_secret"],
                template_key=bot_payload["template_key"] or "promo_funnel",
                status=bot_payload["status"] or "draft",
                default_parse_mode=bot_payload["default_parse_mode"] or "markdown",
                support_username=bot_payload["support_username"] or "",
                website_url=bot_payload["website_url"] or "",
                channel_url=bot_payload["channel_url"] or "",
                buy_url=bot_payload["buy_url"] or "",
                notes=bot_payload["notes"] or "",
                last_error=bot_payload["last_error"] or ""
            )
            jobs.append(
                DeliveryJob(
                    id=str(row["id"]),
                    bot=bot,
                    step_id=str(row["step_id"]) if row["step_id"] else None,
                    chat_id=int(row["chat_id"]),
                    trigger_key=row["trigger_key"] or "start",
                    run_at=row["run_at"],
                    attempts=int(row["attempts"] or 0),
                    message_payload=row["message_payload"] or {}
                )
            )

        return jobs

    async def mark_job_sent(self, job_id: str) -> None:
        await self.pool.execute(
            """
            update public.telegram_bot_delivery_jobs
            set
              status = 'sent',
              sent_at = now(),
              updated_at = now(),
              last_error = ''
            where id = $1::uuid
            """,
            job_id
        )

    async def retry_or_fail_job(self, job_id: str, attempts: int, max_attempts: int, error_message: str) -> None:
        next_status = "failed" if attempts >= max_attempts else "queued"
        next_run_at = datetime.now(UTC) + timedelta(minutes=min(attempts * 2, 30))

        await self.pool.execute(
            """
            update public.telegram_bot_delivery_jobs
            set
              status = $2,
              last_error = $3,
              run_at = case when $2 = 'queued' then $4 else run_at end,
              updated_at = now()
            where id = $1::uuid
            """,
            job_id,
            next_status,
            error_message[:2000],
            next_run_at
        )

    async def claim_telegram_connect_token(
        self,
        *,
        token: str,
        bot_id: str,
        chat_id: int,
        telegram_user_id: int | None,
        telegram_username: str
    ) -> dict[str, Any] | None:
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                token_row = await connection.fetchrow(
                    """
                    select
                      t.id,
                      t.client_id,
                      t.portal_user_id,
                      t.bot_id
                    from public.crm_client_telegram_connect_tokens t
                    where t.token = $1
                      and t.bot_id = $2::uuid
                      and t.consumed_at is null
                      and t.expires_at > now()
                    for update
                    """,
                    token,
                    bot_id
                )
                if not token_row:
                    return None

                await connection.execute(
                    """
                    update public.crm_client_telegram_connect_tokens
                    set consumed_at = now()
                    where id = $1::uuid
                    """,
                    token_row["id"]
                )

                row = await connection.fetchrow(
                    """
                    insert into public.crm_client_telegram_links (
                      client_id,
                      portal_user_id,
                      bot_id,
                      chat_id,
                      telegram_user_id,
                      telegram_username,
                      linked_at,
                      last_seen_at,
                      created_at,
                      updated_at
                    )
                    values (
                      $1::uuid,
                      $2::uuid,
                      $3::uuid,
                      $4,
                      $5,
                      $6,
                      now(),
                      now(),
                      now(),
                      now()
                    )
                    on conflict (client_id) do update
                    set
                      portal_user_id = excluded.portal_user_id,
                      bot_id = excluded.bot_id,
                      chat_id = excluded.chat_id,
                      telegram_user_id = excluded.telegram_user_id,
                      telegram_username = excluded.telegram_username,
                      linked_at = coalesce(public.crm_client_telegram_links.linked_at, excluded.linked_at),
                      last_seen_at = now(),
                      updated_at = now()
                    returning
                      client_id,
                      portal_user_id,
                      bot_id,
                      chat_id,
                      telegram_username,
                      linked_at,
                      last_seen_at
                    """,
                    token_row["client_id"],
                    token_row["portal_user_id"],
                    token_row["bot_id"],
                    chat_id,
                    telegram_user_id,
                    telegram_username
                )

                return dict(row) if row else None

    async def enqueue_due_subscription_reminders(self, local_date) -> int:
        row = await self.pool.fetchrow(
            """
            select public.crm_enqueue_due_subscription_reminders($1::date) as total
            """,
            local_date
        )
        return int(row["total"] if row else 0)

    async def claim_due_subscription_reminders(self, limit: int) -> list[ReminderDeliveryJob]:
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                rows = await connection.fetch(
                    """
                    with due as (
                      select d.id
                      from public.crm_subscription_reminder_deliveries d
                      where d.status = 'queued'
                      order by d.created_at asc
                      for update skip locked
                      limit $1
                    ),
                    claimed as (
                      update public.crm_subscription_reminder_deliveries d
                      set
                        status = 'processing',
                        attempt_count = d.attempt_count + 1,
                        updated_at = now()
                      from due
                      where d.id = due.id
                      returning
                        d.id,
                        d.subscription_id,
                        d.client_id,
                        d.channel,
                        d.reminder_day,
                        d.due_date,
                        d.attempt_count,
                        d.target_email,
                        d.bot_id,
                        d.chat_id,
                        d.payload
                    )
                    select
                      claimed.*,
                      s.expires_at,
                      coalesce(claimed.payload ->> 'clientName', '') as client_name,
                      coalesce(claimed.payload ->> 'serviceName', '') as service_name,
                      coalesce(claimed.payload ->> 'planName', '') as plan_name,
                      jsonb_build_object(
                        'id', b.id,
                        'name', b.name,
                        'public_title', b.public_title,
                        'bot_username', b.bot_username,
                        'bot_token', b.bot_token,
                        'webhook_secret', b.webhook_secret,
                        'template_key', b.template_key,
                        'status', b.status,
                        'default_parse_mode', b.default_parse_mode,
                        'support_username', b.support_username,
                        'website_url', b.website_url,
                        'channel_url', b.channel_url,
                        'buy_url', b.buy_url,
                        'notes', b.notes,
                        'last_error', b.last_error
                      ) as bot_payload
                    from claimed
                    join public.crm_subscriptions s on s.id = claimed.subscription_id
                    left join public.telegram_bots b on b.id = claimed.bot_id
                    """
                    ,
                    limit
                )

        jobs: list[ReminderDeliveryJob] = []
        for row in rows:
            bot_payload = row["bot_payload"]
            bot = None
            if bot_payload and bot_payload.get("id"):
                bot = BotRecord(
                    id=str(bot_payload["id"]),
                    name=bot_payload["name"],
                    public_title=bot_payload["public_title"] or "",
                    bot_username=bot_payload["bot_username"] or "",
                    bot_token=bot_payload["bot_token"],
                    webhook_secret=bot_payload["webhook_secret"],
                    template_key=bot_payload["template_key"] or "promo_funnel",
                    status=bot_payload["status"] or "draft",
                    default_parse_mode=bot_payload["default_parse_mode"] or "markdown",
                    support_username=bot_payload["support_username"] or "",
                    website_url=bot_payload["website_url"] or "",
                    channel_url=bot_payload["channel_url"] or "",
                    buy_url=bot_payload["buy_url"] or "",
                    notes=bot_payload["notes"] or "",
                    last_error=bot_payload["last_error"] or ""
                )

            jobs.append(
                ReminderDeliveryJob(
                    id=str(row["id"]),
                    channel=row["channel"],
                    bot=bot,
                    subscription_id=str(row["subscription_id"]),
                    client_id=str(row["client_id"]),
                    reminder_day=int(row["reminder_day"] or 0),
                    due_date=row["due_date"],
                    attempts=int(row["attempt_count"] or 0),
                    target_email=row["target_email"] or "",
                    chat_id=int(row["chat_id"]) if row["chat_id"] is not None else None,
                    client_name=row["client_name"] or "",
                    service_name=row["service_name"] or "",
                    plan_name=row["plan_name"] or "",
                    expires_at=row["expires_at"],
                    payload=row["payload"] or {}
                )
            )

        return jobs

    async def mark_subscription_reminder_sent(self, reminder_id: str, provider_message_id: str = "") -> None:
        await self.pool.execute(
            """
            update public.crm_subscription_reminder_deliveries
            set
              status = 'sent',
              sent_at = now(),
              processed_at = now(),
              provider_message_id = $2,
              last_error = '',
              updated_at = now()
            where id = $1::uuid
            """,
            reminder_id,
            provider_message_id[:2000]
        )

    async def retry_or_fail_subscription_reminder(
        self,
        reminder_id: str,
        *,
        attempts: int,
        max_attempts: int,
        error_message: str
    ) -> None:
        next_status = "failed" if attempts >= max_attempts else "queued"

        await self.pool.execute(
            """
            update public.crm_subscription_reminder_deliveries
            set
              status = $2,
              last_error = $3,
              processed_at = case when $2 = 'failed' then now() else processed_at end,
              updated_at = now()
            where id = $1::uuid
            """,
            reminder_id,
            next_status,
            error_message[:2000]
        )
