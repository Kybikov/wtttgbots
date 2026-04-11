from __future__ import annotations

from datetime import datetime

from .models import ReminderDeliveryJob


def _format_expiry(value: datetime | None) -> str:
    if value is None:
        return "найближчим часом"
    return value.strftime("%d.%m.%Y")


def build_reminder_subject(job: ReminderDeliveryJob) -> str:
    service_name = job.service_name or "підписку"
    return f"Нагадування: {service_name} завершується через {job.reminder_day} дн."


def build_reminder_email_body(job: ReminderDeliveryJob) -> str:
    plan_line = f"Тариф: {job.plan_name}\n" if job.plan_name else ""
    return (
        f"Привіт, {job.client_name or 'клієнте'}!\n\n"
        f"Нагадуємо, що ваша підписка {job.service_name or 'на сервіс'} завершується через {job.reminder_day} дн.\n"
        f"{plan_line}"
        f"Дата завершення: {_format_expiry(job.expires_at)}\n\n"
        "Якщо хочете продовжити підписку, зайдіть у свій кабінет або зв’яжіться з менеджером.\n\n"
        "WTmelon"
    )


def build_reminder_telegram_text(job: ReminderDeliveryJob) -> str:
    parts = [
        f"Нагадування: {job.service_name or 'Підписка'} завершується через {job.reminder_day} дн.",
        f"Дата завершення: {_format_expiry(job.expires_at)}"
    ]
    if job.plan_name:
        parts.insert(1, f"Тариф: {job.plan_name}")
    return "\n".join(parts)
