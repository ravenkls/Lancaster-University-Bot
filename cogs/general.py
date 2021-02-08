import datetime
import inspect
import io
import subprocess
import traceback
from contextlib import redirect_stdout

import discord
import humanize
from discord.ext import commands

from .utils.db.database import DBFilter
from .utils.messages import MessageBox

from .base import BaseCog


class General(BaseCog):
    def __init__(self, bot):
        super().__init__(bot)
        self.bot.remove_command("help")
        self.start_time = datetime.datetime.now()
        self.sessions = set()

    def get_usage(self, command):
        """Gets the usage of a command."""
        arguments = []

        for param_name, param in command.clean_params.items():
            if param.default == inspect.Parameter.empty:
                arguments.append(f"<{param_name}>")
            elif param.default is not None:
                arguments.append(f"[{param_name}={param.default!r}]")
            else:
                arguments.append(f"[{param_name}]")

        return f"{self.bot.command_prefix}{command.name} " + " ".join(arguments)

    @commands.command()
    async def help(self, ctx, command=None):
        """Displays a list of bot commands."""
        if command is None:
            embed = discord.Embed(
                title="Commands are listed below",
                description=f"Type `{self.bot.command_prefix}help <command>`"
                " more details on a command.",
            )

            for name, cog in self.bot.cogs.items():
                cog_commands = [c for c in cog.get_commands() if not c.hidden]
                if cog_commands:
                    commands_list = "\n".join(
                        [f"`{self.bot.command_prefix}{cc.name}`" for cc in cog_commands]
                    )
                    embed.add_field(name=name + "  " + cog.emoji, value=commands_list)

        elif command := self.bot.get_command(command):
            embed = discord.Embed(
                title=self.bot.command_prefix + command.name,
                description=command.callback.__doc__,
            )
            embed.add_field(name="Usage", value=f"`{self.get_usage(command)}`")

        embed.set_author(
            name=self.bot.user.name,
            icon_url=self.bot.user.avatar_url_as(format="png", static_format="png"),
        )

        await ctx.send(embed=embed)

    @commands.is_owner()
    @commands.command(hidden=True)
    async def update(self, ctx):
        """Get the latest from the git repository."""
        message = await ctx.send(
            embed=MessageBox.loading("Checking GitHub for updates.")
        )
        out = subprocess.run("git pull", capture_output=True, shell=True)
        status = out.stdout.decode().strip().split("\n")[-1]
        if status == "Already up to date.":
            await message.edit(embed=MessageBox.info(status))
        else:
            await message.edit(embed=MessageBox.confirmed(f"Update complete: {status}"))
            reload_command = self.bot.get_command("reload")
            await ctx.invoke(reload_command)

    @commands.is_owner()
    @commands.command(hidden=True)
    async def reload(self, ctx):
        """Reload all extensions."""
        extensions = list(self.bot.extensions.keys())
        message = await ctx.send(
            embed=MessageBox.loading(f"Reloading {len(extensions)} extensions...")
        )
        for e in extensions:
            self.bot.unload_extension(e)
            self.bot.load_extension(e)
        await message.edit(
            embed=MessageBox.success(
                f"{len(extensions)} extensions have been reloaded."
            )
        )

    @commands.is_owner()
    @commands.command(name="eval", hidden=True)
    async def _eval(self, ctx, *, code):
        try:
            if code.startswith("await "):
                response = await eval(code.replace("await ", ""))
            else:
                response = eval(code)
            if response is not None:
                await ctx.send("```py\n{}```".format(response))
        except Exception as e:
            await ctx.send("```py\n{}```".format(e))

    @commands.command()
    async def uptime(self, ctx):
        """Displays how long I've been online for."""
        uptime = datetime.datetime.now() - self.start_time
        uptime_string = humanize.naturaldelta(uptime)
        await ctx.send(embed=MessageBox.info(f"I have been online for {uptime_string}"))

    @commands.command()
    async def userinfo(self, ctx, member: discord.Member):
        """Displays information about a user."""
        now = datetime.datetime.now()

        join_date = member.joined_at.strftime("%d %B %Y")
        join_ago = (now - member.joined_at).days

        creation_date = member.created_at.strftime("%d %B %Y")
        creation_ago = (now - member.created_at).days

        embed = discord.Embed(
            title="User Details", description=member.mention, colour=member.colour
        )
        embed.set_author(
            name=member.name,
            icon_url=member.avatar_url_as(format="png", static_format="png"),
        )
        embed.set_footer(text=str(member.id))
        embed.set_thumbnail(url=member.avatar_url_as(format="png", static_format="png"))

        embed.add_field(
            name="Guild Join Date", value=f"{join_date} ({join_ago} days ago)"
        )
        embed.add_field(
            name="Account Creation Date",
            value=f"{creation_date} ({creation_ago} days ago)",
        )
        if member.roles[1:]:
            embed.add_field(
                name="Roles",
                value=" ".join([r.mention for r in member.roles[1:]]),
                inline=False,
            )

        await ctx.send(embed=embed)

    @commands.command()
    async def deadchannels(self, ctx, limit: int = 10):
        """Returns the top 10 channels which are most dead in the server."""
        message = await ctx.send(embed=MessageBox.loading("Gathering channel data."))
        channels = []
        for ch in ctx.guild.channels:
            if isinstance(ch, discord.TextChannel):
                last_message = await ch.history(limit=1).flatten()
                if last_message:
                    channels.append((ch, last_message[0].created_at))
                else:
                    channels.append((ch, datetime.datetime(1990, 1, 1)))
        dead_channels = list(sorted(channels, key=lambda x: x[1]))[:limit]
        msg = "\n".join(
            [f"{n}. {ch[0].mention}" for n, ch in enumerate(dead_channels, start=1)]
        )
        await message.edit(embed=MessageBox.info(msg))

    @commands.command()
    async def ping(self, ctx):
        """Pong!"""
        response = await ctx.send("ping...")
        difference = response.created_at - ctx.message.created_at
        latency = int(difference.total_seconds() * 1000)
        await response.edit(content=f"Pong! `{latency}ms`")

    @commands.Cog.listener()
    async def on_command_error(self, ctx, exception):
        responses = {
            commands.errors.NoPrivateMessage: "This command can only be used in servers",
            commands.errors.PrivateMessageOnly: "This command can only be used in private messages",
        }

        embed = discord.Embed(colour=0xC42929, description=str(exception))

        if isinstance(exception, commands.errors.CheckFailure):
            return
        elif isinstance(exception, commands.errors.CommandNotFound):
            return

        if message := responses.get(type(exception)):
            embed.description = message
            return await ctx.send(embed=embed)
        elif isinstance(exception, commands.errors.MissingRequiredArgument):
            if len(ctx.args) <= 2 and not ctx.kwargs:
                help_cmd = self.bot.get_command("help")
                return await ctx.invoke(help_cmd, ctx.command.name)
            message = f"{exception.param.name} is a required argument"
        elif isinstance(exception, commands.errors.BadArgument):
            message = str(exception)
        elif type(exception) == commands.CommandInvokeError:
            if type(exception.original) == discord.errors.Forbidden:
                message = "I don't have permission to carry out that action"
            else:
                message = f"`{str(exception)}`"

        await ctx.send(embed=embed)


def setup(bot):
    bot.add_cog(General(bot))
