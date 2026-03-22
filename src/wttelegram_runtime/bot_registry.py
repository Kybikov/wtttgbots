from aiogram import Bot
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode

from .models import BotRecord


class BotRegistry:
    def __init__(self) -> None:
        self._bots: dict[str, tuple[str, Bot]] = {}

    async def get_bot(self, bot_record: BotRecord) -> Bot:
        cached = self._bots.get(bot_record.id)
        if cached and cached[0] == bot_record.bot_token:
            return cached[1]

        if cached:
            await cached[1].session.close()

        bot = Bot(
            token=bot_record.bot_token,
            default=DefaultBotProperties(parse_mode=ParseMode.HTML)
        )
        self._bots[bot_record.id] = (bot_record.bot_token, bot)
        return bot

    async def close(self) -> None:
        for _, bot in self._bots.values():
            await bot.session.close()
        self._bots.clear()
