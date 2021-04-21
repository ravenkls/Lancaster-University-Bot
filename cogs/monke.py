import io
import os
import textwrap

import discord
from discord.ext import commands
from PIL import Image, ImageDraw, ImageSequence

from .base import BaseCog


class Monke(BaseCog):
    @commands.command()
    async def monke(self, ctx, *, text):
        im = Image.open(os.path.join("data", "orangutan.gif"))

        margin = offset = 10

        frames = []
        for frame in ImageSequence.Iterator(im):
            d = ImageDraw.Draw(frame)

            for line in textwrap.wrap(text, width=70):
                d.text((margin, offset), text)
                offset += 20

            b = io.BytesIO()
            frame.save(b, format="gif")
            frame = Image.open(b)

            frames.append(frame)

        byte_io = io.BytesIO()
        frames[0].save(byte_io, "gif", save_all=True, append_images=frames[1:])
        await ctx.send(file=discord.File(byte_io, "monke.gif"))


def setup(bot):
    bot.add_cog(Monke(bot))
