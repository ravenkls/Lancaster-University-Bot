import asyncio
import asyncpg
from .fields import *
from collections import defaultdict


class DBFilter:
    """Specifies how to filter items in a database query."""

    def __init__(self, **kwargs):
        self.filter_kwargs = kwargs

    def sql(self, placeholders_from=1):
        conditions = defaultdict(list)
        values = list(self.filter_kwargs.values())
        removes = []

        for num, field_name in enumerate(self.filter_kwargs.keys(), start=placeholders_from):
            n = num - len(removes)
            i = num - placeholders_from
            if field_name.endswith("__in"):
                conditions[field_name[:-4]].append(f"{field_name[:-4]} IN ${n}")
            elif field_name.endswith("__gt"):
                conditions[field_name[:-4]].append(f"{field_name[:-4:]} > ${n}")
            elif field_name.endswith("__ge"):
                conditions[field_name[:-4]].append(f"{field_name[:-4:]} >= ${n}")
            elif field_name.endswith("__lt"):
                conditions[field_name[:-4]].append(f"{field_name[:-4:]} < ${n}")
            elif field_name.endswith("__le"):
                conditions[field_name[:-4]].append(f"{field_name[:-4:]} <= ${n}")
            elif field_name.endswith("__lt"):
                conditions[field_name[:-4]].append(f"{field_name[:-4:]} < ${n}")
            elif field_name.endswith("__ne"):
                if values[i] is None:
                    conditions[field_name[:-4]].append(f"{field_name[:-4:]} IS NOT NULL")
                    removes.append(i)
                else:
                    conditions[field_name[:-4]].append(f"{field_name[:-4:]} != ${n}")
            else:
                if values[i] is None:
                    conditions[field_name].append(f"{field_name} IS NULL")
                    removes.append(i)
                else:
                    conditions[field_name].append(f"{field_name} = ${n}")

        filters = []
        for field, conds in conditions.items():
            if len(conds) > 1:
                cond = "(" + " OR ".join(conds) + ")"
            else:
                cond = conds[0]
            filters.append(cond)

        values = [v for n, v in enumerate(values) if n not in removes]

        return "WHERE " + " AND ".join(filters), values


class DBQuery:
    """Queries a table on the database."""

    def __init__(self, database, name):
        self.database = database
        self.url = self.database.url
        self.name = name

    async def all(self, limit=None, order_by=None, desc=False):
        """Get all records in the table."""
        limit_sql = f"LIMIT {limit}" if limit is not None else ""
        order_by_sql = f"ORDER BY {order_by}" + (" DESC" if desc else "") if order_by is not None else ""
        conn = await asyncpg.connect(self.url)
        records = await conn.fetch(f"SELECT * FROM {self.name} {limit_sql} {order_by_sql};")
        await conn.close()
        return records

    async def filter(self, where: DBFilter, limit=None, order_by=None, desc=False):
        """Get records in the table based on a filter."""
        limit_sql = f"LIMIT {limit}" if limit is not None else ""
        order_by_sql = f"ORDER BY {order_by}" + (" DESC" if desc else "") if order_by is not None else ""
        where_sql, where_values = where.sql()
        conn = await asyncpg.connect(self.url)
        records = await conn.fetch(f"SELECT * FROM {self.name} {where_sql} {limit_sql} {order_by_sql};", *where_values)
        await conn.close()
        return records

    async def new_record(self, **kwargs):
        """Create a new record in a database."""
        fields_sql = ", ".join(kwargs.keys())
        values_sql = ", ".join([f"${n}" for n, _ in enumerate(kwargs, start=1)])
        conn = await asyncpg.connect(self.url)
        result = await conn.execute(
            f"INSERT INTO {self.name} ({fields_sql}) VALUES ({values_sql});", *kwargs.values()
        )
        await conn.close()
        return result

    async def new_record_with_id(self, **kwargs):
        """Create a new record in a database and return the 'id' value.
        Note: this only works on tables with a SerialIdentifier field."""
        fields_sql = ", ".join(kwargs.keys())
        values_sql = ", ".join([f"${n}" for n, _ in enumerate(kwargs, start=1)])
        conn = await asyncpg.connect(self.url)
        result =  await conn.fetchval(
            f"INSERT INTO {self.name} ({fields_sql}) VALUES ({values_sql}) RETURNING id;", *kwargs.values()
        )
        await conn.close()
        return result

    async def update_records(self, where: DBFilter = None, **kwargs):
        """Update records in a database table."""
        updates_sql = ", ".join([f"{field}=${n}" for n, field in enumerate(kwargs.keys(), start=1)])

        conn = await asyncpg.connect(self.url)
        if where:
            where_sql, where_values = where.sql(placeholders_from=len(kwargs)+1)
            result = await conn.execute(
                f"UPDATE {self.name} SET {updates_sql} {where_sql};", *kwargs.values(), *where_values
            )
        else:
            result = await conn.execute(f"UPDATE {self.name} SET {updates_sql};", *kwargs.values())
        await conn.close()
        return result

    async def delete_records(self, *, where: DBFilter = None):
        """Delete records in a database table."""
        conn = await asyncpg.connect(self.url)
        if where:
            where_sql, where_values = where.sql()
            result = await conn.execute(f"DELETE FROM {self.name} {where_sql};", *where_values)
        else:
            result = await conn.execute(f"DELETE FROM {self.name};")
        await conn.close()
        return result


class Database:
    """Allows you to interact with a postgresql database
    easily and asyncronously."""

    settings_table = "server_setting"

    def __init__(self, url, ssl=False):
        self.url = url + ("?sslmode=require" if ssl else "")

    async def connect(self):
        await self.new_table(
            self.settings_table, (BigInteger("guild_id"), Text("key"), Text("value"),)
        )

    async def get_setting(self, guild, key):
        records = await self.table(self.settings_table).filter(
            where=DBFilter(guild_id=guild.id, key=key)
        )
        if records:
            return records[0]["value"]

    async def set_setting(self, guild, key, value):
        await self.table(self.settings_table).delete_records(
            where=DBFilter(guild_id=guild.id, key=str(key))
        )
        if value is not None:
            await self.table(self.settings_table).new_record(
                guild_id=guild.id, key=str(key), value=str(value)
            )

    async def new_table(self, name, fields):
        fields = ", ".join([f'"{f.name}" {f.datatype}' for f in fields])
        conn = await asyncpg.connect(self.url)
        await conn.execute(f"CREATE TABLE IF NOT EXISTS {name} ({fields});")
        await conn.close()
        return self.table(name)

    async def execute_sql(self, sql, *params, fetch=False):
        conn = await asyncpg.connect(self.url)
        if fetch:
            result = await conn.fetch(sql, *params)
        else:
            result = await conn.execute(sql, *params)
        await conn.close()
        return result

    def table(self, name):
        return DBQuery(self, name)
