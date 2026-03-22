from __future__ import annotations

from html import escape
from typing import Any

from aiogram import Bot
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from .markdown import render_text
from .models import BotRecord, StepRecord


def resolve_button_url(button: dict[str, Any], bot_record: BotRecord) -> str:
    action_type = str(button.get("action_type") or "url").lower()
    raw_url = str(button.get("url") or "").strip()

    if action_type == "buy":
        return bot_record.buy_url
    if action_type == "website":
        return bot_record.website_url
    if action_type == "channel":
        return bot_record.channel_url
    if action_type == "support":
        username = bot_record.support_username.strip().lstrip("@")
        return f"https://t.me/{username}" if username else ""
    return raw_url


def build_keyboard(payload: dict[str, Any], bot_record: BotRecord) -> InlineKeyboardMarkup | None:
    rows = []
    for button in payload.get("buttons") or []:
        url = resolve_button_url(button, bot_record)
        label = str(button.get("label") or "").strip()
        if not url or not label:
            continue
        rows.append([InlineKeyboardButton(text=label, url=url)])

    if not rows:
        return None

    return InlineKeyboardMarkup(inline_keyboard=rows)


def serialize_step(step: StepRecord, bot_record: BotRecord) -> dict[str, Any]:
    return {
        "step_id": step.id,
        "trigger_key": step.trigger_key,
        "message_kind": step.message_kind,
        "title": step.title,
        "body_text": step.body_text,
        "media_url": step.media_url,
        "parse_mode": step.effective_parse_mode(bot_record.default_parse_mode),
        "promo_code": step.promo_code,
        "promo_note": step.promo_note,
        "buttons": [
            {
                "id": button.id,
                "label": button.label,
                "action_type": button.action_type,
                "url": button.url,
                "sort_order": button.sort_order
            }
            for button in step.buttons
        ]
    }


def compose_step_text(payload: dict[str, Any]) -> tuple[str, str | None]:
    parts: list[str] = []
    title = str(payload.get("title") or "").strip()
    body = str(payload.get("body_text") or "").strip()
    promo_code = str(payload.get("promo_code") or "").strip()
    promo_note = str(payload.get("promo_note") or "").strip()

    if title:
        parts.append(f"**{title}**")
    if body:
        parts.append(body)
    if promo_code:
        parts.append(f"`{promo_code}`")
    if promo_note:
        parts.append(promo_note)

    raw_text = "\n\n".join(part for part in parts if part).strip()
    if not raw_text:
        return "", None

    return render_text(raw_text, str(payload.get("parse_mode") or "plain"))


async def send_step(bot: Bot, bot_record: BotRecord, chat_id: int, payload: dict[str, Any]) -> None:
    text, parse_mode = compose_step_text(payload)
    keyboard = build_keyboard(payload, bot_record)
    media_url = str(payload.get("media_url") or "").strip()
    message_kind = str(payload.get("message_kind") or "text").strip().lower()

    if message_kind == "photo" and media_url:
        if text and len(text) <= 900:
            await bot.send_photo(
                chat_id=chat_id,
                photo=media_url,
                caption=text,
                parse_mode=parse_mode,
                reply_markup=keyboard
            )
            return

        await bot.send_photo(chat_id=chat_id, photo=media_url)
        if text:
            await bot.send_message(
                chat_id=chat_id,
                text=text,
                parse_mode=parse_mode,
                reply_markup=keyboard
            )
        elif keyboard:
            await bot.send_message(
                chat_id=chat_id,
                text=escape(bot_record.public_title or bot_record.name or "Деталі"),
                parse_mode="HTML",
                reply_markup=keyboard
            )
        return

    message_text = text or escape(bot_record.public_title or bot_record.name or "WTmelon bot")
    await bot.send_message(
        chat_id=chat_id,
        text=message_text,
        parse_mode=parse_mode or "HTML",
        reply_markup=keyboard
    )
