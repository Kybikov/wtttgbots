from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass(slots=True)
class ButtonRecord:
    id: str
    label: str
    action_type: str
    url: str
    sort_order: int


@dataclass(slots=True)
class StepRecord:
    id: str
    bot_id: str
    trigger_key: str
    step_key: str
    sort_order: int
    is_active: bool
    delay_seconds: int
    message_kind: str
    title: str
    body_text: str
    media_url: str
    parse_mode: str
    promo_code: str
    promo_note: str
    buttons: list[ButtonRecord] = field(default_factory=list)

    def effective_parse_mode(self, default_parse_mode: str) -> str:
        return default_parse_mode if self.parse_mode == "inherit" else self.parse_mode


@dataclass(slots=True)
class BotRecord:
    id: str
    name: str
    public_title: str
    bot_username: str
    bot_token: str
    webhook_secret: str
    template_key: str
    status: str
    default_parse_mode: str
    support_username: str
    website_url: str
    channel_url: str
    buy_url: str
    notes: str
    last_error: str


@dataclass(slots=True)
class DeliveryJob:
    id: str
    bot: BotRecord
    step_id: str | None
    chat_id: int
    trigger_key: str
    run_at: datetime
    attempts: int
    message_payload: dict[str, Any]

