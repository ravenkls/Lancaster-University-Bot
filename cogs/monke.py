import os
import json
from discord.ext import commands
import datetime
import pytz

from .base import BaseCog


class Monke(BaseCog):
    def __init__(self, bot):
        super().__init__(bot)
        self.emoji = "🦧"
        with open(os.path.join("data", "monke_days.json")) as f:
            self.monke_days = json.load(f)

    def get_daily_video(self):
        tz = pytz.timezone("Europe/London")
        day = datetime.datetime.now(tz).weekday()
        return self.monke_days[day]

    @commands.command()
    async def day(self, ctx):
        await ctx.send(self.get_daily_video())


def setup(bot):
    bot.add_cog(Monke(bot))
