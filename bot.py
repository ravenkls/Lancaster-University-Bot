import logging
import os
import sys
import configparser

from discord.ext import commands

from cogs.utils.db import Database
from cogs.utils.db.fields import *


class LancasterUniBot(commands.Bot):
    def __init__(self, prefix, database_url, login_data):
        super().__init__(command_prefix=prefix)
        self.login_data = login_data
        self.database_url = database_url
        self.database = Database(self.database_url, ssl=True)
        self.load_extension("cogs.general")
        self.load_extension("cogs.lancaster")
        self.load_extension("cogs.monke")
        self.logger = logging.getLogger(__name__)

    async def on_connect(self):
        await self.database.connect()

    async def on_ready(self):
        self.logger.info("Bot is ready and accepting commands.")
        self.logger.info(
            f"Invite link: https://discord.com/oauth2/authorize?client_id={self.user.id}&permissions=8&scope=bot"
        )


def generate_settings():
    config = configparser.ConfigParser()
    config["BotSettings"] = {
        "token": "",
        "database_url": "",
        "prefix": "",
        "portal_username": "",
        "portal_password": "",
    }
    with open("settings.cfg", "w") as f:
        config.write(f)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="[%(levelname)s] [%(name)s] %(message)s"
    )

    try:
        token = os.environ["TOKEN"]
        database_url = os.environ["DATABASE_URL"]
        prefix = os.environ["PREFIX"]
        login_data = (os.environ["PORTAL_USERNAME"], os.environ["PORTAL_PASSWORD"])
    except KeyError:

        if not os.path.exists("settings.cfg"):
            generate_settings()
            logging.critical(
                "No config found. generating 'settings.cfg', please fill "
                "in the required settings before running the bot"
            )
            sys.exit()

        config = configparser.ConfigParser()
        config.read("settings.cfg")

        try:
            token = config["BotSettings"]["token"]
            database_url = config["BotSettings"]["database_url"]
            prefix = config["BotSettings"]["prefix"]
            login_data = (
                config["BotSettings"]["portal_username"],
                config["BotSettings"]["portal_password"],
            )
        except (configparser.NoSectionError, KeyError):
            logging.critical(
                "Malformed 'settings.cfg' file, please fix this before running the bot."
            )
            sys.exit()

    bot = LancasterUniBot(prefix, database_url, login_data)
    bot.run(token)
