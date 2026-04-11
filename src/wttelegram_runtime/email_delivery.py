from __future__ import annotations

import asyncio
import smtplib
from email.message import EmailMessage

from .config import Settings


class SmtpEmailSender:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    @property
    def is_configured(self) -> bool:
        return bool(
            self.settings.smtp_host
            and self.settings.smtp_from_email
        )

    async def send_plaintext(self, *, recipient: str, subject: str, body: str) -> None:
        if not self.is_configured:
            raise RuntimeError("SMTP is not configured for reminder delivery")

        await asyncio.to_thread(
            self._send_plaintext_sync,
            recipient.strip(),
            subject,
            body
        )

    def _send_plaintext_sync(self, recipient: str, subject: str, body: str) -> None:
        message = EmailMessage()
        message["From"] = f"{self.settings.smtp_from_name} <{self.settings.smtp_from_email}>"
        message["To"] = recipient
        message["Subject"] = subject
        message.set_content(body)

        if self.settings.smtp_use_ssl:
            with smtplib.SMTP_SSL(self.settings.smtp_host, self.settings.smtp_port, timeout=30) as smtp:
                self._authenticate(smtp)
                smtp.send_message(message)
            return

        with smtplib.SMTP(self.settings.smtp_host, self.settings.smtp_port, timeout=30) as smtp:
            smtp.ehlo()
            if self.settings.smtp_use_tls:
                smtp.starttls()
                smtp.ehlo()
            self._authenticate(smtp)
            smtp.send_message(message)

    def _authenticate(self, smtp: smtplib.SMTP) -> None:
        if self.settings.smtp_username:
            smtp.login(self.settings.smtp_username, self.settings.smtp_password)
