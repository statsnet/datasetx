"""
PostgreSQL batch upsert/update util
Example:

```python
db = dataset.connect("postgres://localhost/test")
table = db["table"]
rows = [
    {
        "field": "value",
    }
]
table.upsert_many(rows, ["key1", "key2"])
```

"""

import asyncio
import functools
import io
import json
import logging
import os
import re
import warnings
from collections import OrderedDict
from copy import copy
from datetime import datetime, timedelta
from typing import (
    Any,
    Coroutine,
    Iterable,
    List,
    Optional,
    Tuple,
    Union,
)

import asyncpg
from aiogram.bot import Bot
from asyncpg.connection import Connection
from dotenv import load_dotenv
import paramiko
from pytz import UnknownTimeZoneError, timezone
from tqdm import tqdm
from tqdm.asyncio import tqdm_asyncio
from asyncpg import Record
from sshtunnel import SSHTunnelForwarder
import boto3
from urllib.parse import urlparse

from typing import Callable, TypeVar

try:
    from typing import ParamSpec  # novm
except ImportError:
    from typing_extensions import ParamSpec

warnings.simplefilter("ignore", DeprecationWarning)


dotenv_path = os.path.join(os.path.dirname(__file__), ".env")
load_dotenv(dotenv_path)

DEFAULT_LOG_LEVEL = "warning"
logging.basicConfig(level=os.getenv("LOG_LEVEL", DEFAULT_LOG_LEVEL))


P = ParamSpec("P")
T = TypeVar("T")


def copy_doc(wrapper: Callable[P, T]):
    """Copy function docstring"""

    def decorator(func: Callable) -> Callable[P, T]:
        func.__doc__ = wrapper.__doc__
        return func

    return decorator


class DatasetException(ValueError):
    pass


class DBEmptyException(DatasetException):
    pass


class InvalidKeyException(DatasetException):
    pass


class FieldSerializationException(DatasetException):
    pass


class _WrapCallback:
    def __init__(self, callback):
        self.callback = callback
        self.text = self.prev_text = "<< Init tqdm >>"

    def write(self, s):
        new_text = s.strip().replace("\r", "").replace("\n", "")
        if len(new_text) != 0:
            self.text = new_text

    def flush(self):
        if self.prev_text != self.text:
            loop = asyncio.get_event_loop()
            asyncio.ensure_future(self.callback(self.text), loop=loop)
            self.prev_text = self.text


class BotProgressReport:
    """Telegram bot progress report"""

    enable = True
    started = False
    finished = False
    bot_token = os.getenv("BOT_TOKEN")
    bot_chat = os.getenv("BOT_CHAT")
    total: Optional[int] = 0
    completed: int = 0
    bar_width: Optional[int] = int(os.getenv("BAR_WIDTH", 80))
    min_percent: Optional[int] = int(os.getenv("MINPERCENT", 1))
    state_icons = {
        False: "🔴",
        True: "🟢",
        None: "🟡",
    }

    bot: Optional[Bot] = None
    msg_id: Optional[int] = None
    title: Optional[str] = None
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    error: bool = False
    exception: Optional[Exception] = None
    last_text: Optional[str] = None
    last_bar: Optional[str] = None

    notif_progress: bool = bool(os.getenv("BOT_PROGRESS", "true") == "true")
    notif_end_split: bool = bool(os.getenv("BOT_END_SPLIT", "false") == "true")
    timezone = None
    log = logging.getLogger("BotProgress")

    def __init__(self, title: str, total: Optional[int], auto_init: bool = False, **kwargs):

        self.title = title
        self.log.setLevel(os.getenv("LOG_LEVEL", kwargs.get("LOG_LEVEL", DEFAULT_LOG_LEVEL)))

        self.callback = _WrapCallback(self._tqdm_callback)

        params = kwargs.copy()
        params.update(
            {
                "file": self.callback,
                "ascii": False,
                "ncols": self.bar_width,
                "unit_scale": True,
            }
        )
        if "mininterval" not in params:
            params["mininterval"] = int(os.getenv("MININTERVAL", 2))
        self.pbar = tqdm_asyncio(**params)
        if total:
            self.set_total(total)
        if auto_init:
            self._init()

        raw_timezone = os.getenv("TIMEZONE", "UTC")
        try:
            self.timezone = timezone(raw_timezone)
        except UnknownTimeZoneError as e:
            self.log.warning(f"Invalid timezone: {raw_timezone}", exc_info=e)

    @property
    def _is_enabled(self) -> bool:
        return bool(self.enable and self.bot_token and self.bot_chat)

    def _init(self):
        if not self._is_enabled:
            return
        if not self.bot_token:
            warnings.warn("BOT_TOKEN environment variable is empty, telegram notifications disabled")
        if not self.bot_chat:
            warnings.warn("BOT_CHAT environment variable is empty, telegram notifications disabled")

        assert self.bot_token is not None, "Bot token is required"
        self.bot = Bot(token=self.bot_token, parse_mode="MarkdownV2")

    def _now(self) -> datetime:
        return datetime.now(tz=self.timezone)

    def _format_date(self, date: Optional[datetime]) -> Optional[str]:
        """Readable date format"""
        if not date:
            return None
        return date.strftime("%Y.%m.%d %H:%M:%S")

    def _state_icon(self) -> str:
        """Current state icon"""
        if self.error:
            return self.state_icons[False]
        elif self.finished or self.finished_at:
            return self.state_icons[True]

        return self.state_icons[None]

    def _timedelta_str(self, td: timedelta) -> str:
        r = []
        for i in str(td).split(":"):
            r.append(str(int(float(i))).zfill(2))
        return ":".join(r)

    def _template(self, after: str = "", before: str = ""):
        """Message template"""
        if self.finished_at and self.started_at:
            last_run_text = f"*Время завершения*: `{self._format_date(self.finished_at)}`"
            timedelta_str = self._timedelta_str(self.finished_at - self.started_at)
            last_run_text += f"\n*Прошло времени*: `{timedelta_str}`"
        else:
            last_run_text = f"*Последнее обновление*: `{self._format_date(self._now())}`"

        return f"""
{before}
{self._state_icon()} *Задача*: `{self.title}`
*Время запуска*: `{self._format_date(self.started_at)}`
{last_run_text}

{after}
""".strip()

    def _tg_escape(self, text: str) -> str:
        """Escaping progress bar special characters"""
        escape = list("[*_`")
        for symb in escape:
            text = text.replace(symb, "\\" + symb)
        return text

    def set_total(self, total: int):
        """Set total items count for progress bar"""
        self.total = total
        self.pbar.total = total
        self.pbar.miniters = int(total / 100 * (self.min_percent or 1))

    async def start(self):
        """Start progress sending"""
        if self._is_enabled and not self.started and not self.finished:
            self.started = True
            self.started_at = self._now()
            self.log.info(f"Started progress report at {self.started_at}")

    async def stop(
        self,
        error: bool = False,
        send_error: bool = True,
        finish_tasks: bool = True,
        finish_tasks_count: int = 1,
        exc: Optional[Exception] = None,
    ):
        """Stop progress sending"""
        if self._is_enabled and self.started and not self.finished:
            assert self.bot_chat is not None
            assert self.bot is not None

            self.finished = True
            self.error = error
            if exc:
                self.exception = exc
            # self.pbar.close(leave=False)
            if self.notif_end_split:
                await self.bot.send_message(
                    self.bot_chat,
                    f"Выгрузка `{self.title}` завершена.",
                    reply_to_message_id=self.msg_id,
                )

            self.log.info(f"Finished progress report at {self._now()}")
            self.pbar.close()
            await self.bot.close()

            # Finish event loop tasks
            if finish_tasks:
                while len(asyncio.all_tasks()) > finish_tasks_count:
                    self.log.info(f"Finishing {len(asyncio.all_tasks()) - finish_tasks_count} tasks...")
                    await asyncio.sleep(1)

    async def update(self, count: int):
        """Update progress status"""
        if self._is_enabled:
            self.completed += count
            # self.log.debug(
            #     f"Updated progress by {count}, now: {self.completed} / {self.total}"
            # )
            self.pbar.update(count)

    async def _tqdm_callback(self, bar_text: Optional[str] = None):
        """Messages logic on bar update"""
        if not self._is_enabled:
            return
        assert self.bot is not None
        assert self.bot_chat is not None

        if not bar_text:
            bar_text = self.last_bar or ""

        if not self.finished_at and "100%" in bar_text:
            self.finished_at = self._now()
        else:
            if self.msg_id and not self.notif_progress:
                return

        # bar_text = self._tg_escape(bar_text).ljust(self.bar_width or 100)
        bar_text = f"`{bar_text}`"
        if self.exception:
            exc_cls = get_full_class_name(self.exception)
            exc_str = str(self.exception)
            exc_tx = self._tg_escape(f"{exc_cls}: {exc_str}").replace("|", "\\|")
            bar_text += f"\n*Exception*:\n`{exc_tx}`"

        text = self._template(after=bar_text if self.notif_progress else "")
        self.last_text = text
        self.last_bar = bar_text
        self.log.debug(f"Update text: {text}")

        if self.msg_id:
            await self.bot.edit_message_text(text, self.bot_chat, self.msg_id)
        else:
            msg = await self.bot.send_message(self.bot_chat, text)
            self.msg_id = msg.message_id

    on_end = stop
    on_finish = stop


def get_full_class_name(obj):
    module = obj.__class__.__module__
    if module is None or module == str.__class__.__module__:
        return obj.__class__.__name__
    return module + "." + obj.__class__.__name__


def progress_decorator(title: str):
    def wrapper(func):
        @functools.wraps(func)
        async def wrapped(*args, **kwargs):
            bot = BotProgressReport(title=title, total=0)
            kwargs["bot"] = bot
            await bot.start()
            try:
                r = await func(*args, **kwargs)
            except Exception as e:
                await bot.stop(error=True, exc=e)
                raise e

            await bot.stop()
            return r

        return wrapped

    return wrapper


def run_async(func: Coroutine):
    """Run async command as sync"""
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(func)


def chunks(lst: Iterable, n: int):
    """Iterate data in chunks"""
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


class Dataset:
    conn: Optional[Connection] = None
    conn_url: Optional[str] = None
    db: Optional[str] = None  # Filled by cls['table_name']
    fields: Optional[dict] = None
    ignore_fields: List[str] = list()  # List of fields to ignore
    force_fields: List[str] = list()  # List of fields to force add to query
    progressbar = True  # Show tqdm progressbar
    dryrun = False  # Run in dry mode, just print query
    title: Optional[str] = None  # Task title in telegram bot
    bot_enable: bool = True  # Enable bot notifications
    bot_token: Optional[str] = None
    bot_chat: Optional[str] = None

    def __init__(self, log_level: Optional[str] = None, title: Optional[str] = None, bot_token: Optional[str] = None, bot_chat: Optional[str] = None) -> None:
        self.log = logging.getLogger("Dataset")
        self.log.setLevel(log_level or "INFO")
        if title:
            self.title = title
        if bot_token:
            self.bot_token = bot_token
        if bot_chat:
            self.bot_chat = bot_chat

    def set_title(self, title: str):
        """Set bot notification title"""
        self.title = title

    set_desc = set_title

    def _bot_init(self, bot: BotProgressReport):
        bot.enable = self.bot_enable
        bot.bot_token = self.bot_token
        bot.bot_chat = self.bot_chat
        bot._init()
        if bot.title:
            if self.title:
                bot.title += f" - {self.db} ({self.title})"
            else:
                bot.title += f" - {self.db}"

    def __getitem__(self, __name: str) -> "Dataset":
        """Get DB table"""
        return self.table(__name)

    def table(self, name: str) -> "Dataset":
        """Returns copy of class with filled db (table name)"""
        self.log.debug(f"Selected table: {name}")

        cls = copy(self)
        cls.conn = self.conn
        cls.db = name
        return cls

    async def connect_async(
        self,
        url: str,
        ssh_address: Optional[Union[Tuple[str, int], str]] = None,
        ssh_username: Optional[str] = None,
        ssh_key: Optional[str] = None,
        aws_region: str = "eu-north-1",
        aws_profile: Optional[str] = None,
        aws_secret_key: Optional[str] = None,
        aws_secret_id: Optional[str] = None,
        bind_port: Optional[int] = None,
    ) -> Connection:
        """
        Connect to PostgreSQL DB

        :param str url: URL for connecting to postgresql in format: postgres://user:pass@host:port/database
        :param str ssh_address: address to connect to Amazon RDS SSH forwarding server. If set, ssh tunnel will be used.
        :param str ssh_username: username to connect to Amazon RDS
        :param str ssh_key: path to the private SSH key or SSH key contents
        :param str aws_region: AWS region
        :params str aws_profile: can be used as replace for aws* settings.
        [boto3 docs](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html#configuring-credentials)
        :param str aws_secret_key: secret key to connect to Amazon RDS
        :param str aws_secret_id: secret key to connect to Amazon RDS (shorter than aws_secret_key)
        :param str bind_port: local port used for SSH tunneling. Should be different if multiple instances should be run.
        """
        if ssh_address:  # SSH tunnel
            if not all([ssh_address, ssh_username, ssh_key]):
                raise RuntimeError("ssh_address, ssh_username and ssh_key are required for SSH tunnel!")
            self.log.debug("Connecting to SSH Tunnel...")
            parsed = urlparse(url)
            default_port = 5432

            if not os.path.exists(ssh_key):
                ssh_key = paramiko.RSAKey.from_private_key(io.StringIO(ssh_key))

            session = boto3.Session(
                profile_name=aws_profile,
                aws_secret_access_key=aws_secret_key,
                aws_access_key_id=aws_secret_id,
                region_name=aws_region,
            )
            client = session.client("rds")
            token = client.generate_db_auth_token(
                DBHostname=parsed.hostname,
                Port=parsed.port or default_port,
                DBUsername=parsed.username,
                Region=aws_region,
            )

            server = SSHTunnelForwarder(
                ssh_address_or_host=ssh_address,
                ssh_username=ssh_username,
                ssh_pkey=ssh_key,  # Can be file name or key string
                remote_bind_address=(parsed.hostname, parsed.port or default_port),
                local_bind_address=("0.0.0.0", bind_port or 0),
            )
            server.start()

            ## SSH Tunnel DB connection
            db_username = parsed.netloc.split("@")[0].split(":")[0]

            self.log.debug(f"Connecting to DB (Used local port: {server.local_bind_port})...")
            self.conn = await asyncpg.connect(
                user=db_username,
                password=token,
                host="localhost",
                port=server.local_bind_port,
                database=parsed.path[1:],
            )
            return self.conn
        else:
            self.log.debug("Connecting to DB...")
            self.conn = await asyncpg.connect(url)
            return self.conn

    @copy_doc(connect_async)
    def connect(self, url: str, *args, **kwargs):
        self.conn_url = url
        return run_async(self.connect_async(url, *args, **kwargs))

    async def disconnect_async(self):
        """Disconnect from DB. Recommended, but not required"""
        self.log.debug("Disconnecting from DB")
        if self.conn:
            return await self.conn.close()

    @copy_doc(disconnect_async)
    def disconnect(self, *args, **kwargs):
        return run_async(self.disconnect_async(*args, **kwargs))

    def _check_table(self):
        """Check is table selected for operation"""
        if not self.db:
            raise DBEmptyException("Call dataset.connection()['table_name'] ")

    async def _check_keys(self, keys: List[str]):
        """Check that all keys are valid and exists in table"""
        if keys == "__all__":
            fields = await self._get_fields()
            return list(fields.keys())

        assert len(keys) > 0, "At least one key is required"

        for key in keys:
            if key not in self.fields:
                raise InvalidKeyException(f"Key '{key}' is not exists on table '{self.db}' ({self.fields.keys()})")

        return keys

    async def _get_fields(self) -> "OrderedDict[str, str]":
        """Get dict of DB fields. Output format: {name: field_type}"""
        self._check_table()
        q = f"""
SELECT column_name, data_type FROM information_schema.columns
WHERE
    table_name = '{self.db}'
ORDER BY ordinal_position
"""

        raw = await self.conn.fetch(q)
        res = OrderedDict()
        for record in raw:
            key = record["column_name"]
            val = record["data_type"]
            val = re.sub(r"\"(.*)\"", r"\1", val)  # Replace 'char' => char

            res[key] = val

        self.log.debug(f"Parsed fields from DB: {res}")
        if not res:
            raise DBEmptyException(f"Table '{self.db}' fields are not found!")

        return res

    async def _try_convert_date(self, val: str) -> datetime:
        """Try to convert date from different formats"""
        for fmt in ("%Y-%m-%d", "%d-%m-Y", "%d.%m.%Y", "%Y.%m.%d", "%d/%m/%Y"):
            try:
                return datetime.strptime(val, fmt)
            except ValueError:
                pass
        raise ValueError(f"Invalid date format: '{val}'")

    async def _convert_value(self, val: Any, field_type: str, field_name: str) -> Any:
        """Convert passed value to DB field type"""

        if field_type.startswith("char") or field_type.endswith("char"):
            return str(val)
        elif (
            field_type.startswith("int")
            or field_type.endswith("int")
            or field_type in ["smallserial", "serial", "bigserial"]
        ):
            return int(val)
        elif (
            field_type.startswith("float")
            or field_type.endswith("float")
            or field_type in ["decimal", "numeric", "real"]
        ):
            return float(val)
        elif field_type in ["date"]:
            if isinstance(val, str):
                if val.isdigit():
                    return datetime.fromtimestamp(val)
                else:
                    return await self._try_convert_date(val)
            elif isinstance(val, (float, int)):
                return datetime.fromtimestamp(val)
            elif isinstance(val, datetime):
                return val
        elif field_type.startswith("bool"):
            return bool(val)
        elif field_type in ["array"]:
            assert isinstance(val, (list, tuple)), f"Invalid type '{field_type}' of ({val})"
            return val
        elif field_type in ["json", "jsonb"]:
            assert isinstance(val, (list, tuple, dict)), f"Invalid type '{field_type}' of ({val})"
            return json.dumps(val, ensure_ascii=False)

        ## Unknown / ignored

        elif field_type in ["USER-DEFINED"]:
            pass
        else:
            self.log.debug(f"Unknown field type: '{field_type}'")

        return val

    async def _prepare_data(self, rows: List[dict]) -> Tuple[List[Any], List[str]]:
        """Prepare data for DB (convert rows dict to DB argument list)"""
        res = []
        unused_fields = list(self.fields.keys())

        for row in rows:
            r = []
            for field_name, field_type in self.fields.items():
                if row.get(field_name) is None:
                    r.append(None)
                    continue

                try:
                    val = await self._convert_value(row[field_name], field_type.lower(), field_name)
                except TypeError as exc:
                    raise FieldSerializationException() from exc
                r.append(val)

                if field_name in unused_fields and (val is not None or val in self.force_fields):
                    unused_fields.remove(field_name)

            res.append(r)
        return res, unused_fields

    async def _prepare_data_fields(
        self,
        rows: List[dict],
        keys=None,
        update_keys=None,
        remove_non_keys: bool = False,
    ) -> Tuple[list, dict]:
        """Prepare cleaned data and fields dict, remove unused fields"""
        data, unused_fields = await self._prepare_data(rows)
        fields = self.fields.copy()
        remove_ids = []

        if self.ignore_fields:
            unused_fields.extend(self.ignore_fields)

        if self.force_fields:
            for f in self.force_fields:
                unused_fields.remove(f)

        if keys and remove_non_keys:  # Remove non selected keys
            for f in fields:
                if f not in keys and f not in unused_fields:
                    unused_fields.append(f)

        if update_keys:  # Add update keys
            for k in update_keys:
                if k in unused_fields:
                    unused_fields.remove(k)

        # -- Delete unused fields from fields
        for f in unused_fields:
            if f in fields:
                field_id = await self._field_id(f)
                remove_ids.append(field_id)
                fields.pop(f)

        self.log.debug(f"Unused fields: {unused_fields}, {remove_ids}, {fields.keys()}")
        # -- Delete unused fields from data
        res_data = []
        for d in data:
            used_data = []
            for i, elem in enumerate(d):
                if i in remove_ids:
                    continue
                used_data.append(elem)

            res_data.append(used_data)

        return res_data, fields

    async def _field_id(self, field: str, fields: List[str] = None) -> int:
        """Get field ID (serial number of field in fields dict)"""
        if not fields:
            fields = self.fields

        for i, (field_name, field_type) in enumerate(fields.items()):
            if field_name == field:
                return i

        return 0

    async def _field_set(self, field: str, fields: List[str] = None) -> str:
        """Generate field set SQL expression"""
        field_id = await self._field_id(field, fields=fields)
        return f"{field} = ${field_id + 1}"

    def escapestr(self, tx: str, char: str = '"') -> str:
        """Escape string (column name) by quotes"""

        def esc(x):
            return f"{char}{x}{char}"

        if isinstance(tx, (list, tuple)):
            return list(map(esc, tx))
        else:
            return esc(tx)

    def upsert_many(
        self,
        rows: List[dict],
        keys: List[str],
        unique_keys: List[str] = None,
        update_keys: List[str] = None,
        id_columns: List[str] = None,
        chunk_size: int = 50_000,
        desc: Optional[str] = None,
    ):
        """
        Upsert many records in DB. If record not exists, it will be inserted otherwise updated.
        Required index with UNIQUE for all keys fields altogether
        ### Examples:
        >>> rows = [{'id': 1, 'is_active': True}, {'id': 2, 'is_active': False}]
        >>> db = dataset.connect("postgres://localhost/test")
        >>> db['table'].upsert_many(rows, ['id', 'is_active'])
        """
        if desc:
            self.set_desc(desc)
        return run_async(
            self.upsert_many_async(
                rows,
                keys,
                unique_keys=unique_keys,
                update_keys=update_keys,
                id_columns=id_columns,
                chunk_size=chunk_size,
            )
        )
        # return asyncio.run(self.upsert_many_async(rows, keys))

    def update_many_filter(
        self,
        filters: "dict[str, Any]",
        values: "dict[str, Any]",
        chunk_size: int = 50_000,
    ):
        """
        Update many records in DB by filters
        ### Examples:
        >>> db = dataset.connect("postgres://localhost/test")
        >>> db['table'].update_many_filter({'id': 1}, {'is_active': False})
        """
        return run_async(self.update_many_filter_async(filters, values, chunk_size))

    @copy_doc(update_many_filter)
    @progress_decorator("update_many_filter")
    async def update_many_filter_async(
        self,
        filters: "dict[str, Any]",
        values: "dict[str, Any]",
        chunk_size: int = 50_000,
        bot: Optional[BotProgressReport] = None,
        desc: Optional[str] = None,
    ):
        """Run update_many on DB. Only updates already existing records."""
        if desc:
            self.set_desc(desc)
        assert bot is not None
        self._bot_init(bot)

        self.fields = await self._get_fields()
        await self._check_keys(list(filters.keys()))
        await self._check_keys(list(values.keys()))

        assert len(set([*filters.keys(), *values.keys()])) == len(filters) + len(
            values
        ), "Values and filters dicts should be unique, keys can't be in both."

        fields_filters = {k: v for k, v in self.fields.items() if k in filters}
        fields_values = {k: v for k, v in self.fields.items() if k in values}
        fields_all = {**fields_filters, **fields_values}
        data = [filters.get(k) or values.get(k) for k in fields_all]

        expr_set = [await self._field_set(k, fields_all) for k in fields_values.keys()]
        expr_where = [await self._field_set(k, fields_all) for k in fields_filters.keys()]

        expr_set_str = ",\n    ".join(expr_set)
        expr_where_str = "\n    AND ".join(expr_where)

        q = f"""
UPDATE {self.db}
SET
    {expr_set_str}
WHERE
    {expr_where_str}
"""

        self.log.debug(f"Query: {q}")
        if self.progressbar:
            tbar = tqdm(desc="Update", total=None)

        if not self.dryrun:
            updated_text = await self.conn.execute(q, *data)
            updated_count = 1
            try:
                updated_count = int(updated_text.replace("UPDATE ", ""))
            except ValueError:
                pass

            await bot.update(int(updated_count))
            if self.progressbar:
                tbar.update(int(updated_count))

        if self.progressbar:
            tbar.close()

    def update_many(
        self,
        rows: List[Any],
        keys: List[str],
        chunk_size: int = 50_000,
        desc: Optional[str] = None,
    ):
        """Run update_many on DB. Only updates already existing records."""
        """
        Update many records in DB by keys values.
        ### Examples:
        >>> rows = [{'id': 1, 'is_active': True}, {'id': 2, 'is_active': False}]
        >>> db = dataset.connect("postgres://localhost/test")
        >>> db['table'].update_many(rows, ['id'])
        """
        if desc:
            self.set_desc(desc)
        return run_async(self.update_many_async(rows, keys, chunk_size))

    @copy_doc(update_many)
    @progress_decorator("update_many")
    async def update_many_async(
        self,
        rows: List[dict],
        keys: List[str],
        chunk_size: int = 50_000,
        bot: Optional[BotProgressReport] = None,
    ):
        """Run update_many on DB. Only updates already existing records."""
        assert bot is not None
        self._bot_init(bot)

        self.fields = await self._get_fields()
        await self._check_keys(keys)
        data, fields = await self._prepare_data_fields(rows)

        expr_set = [await self._field_set(k, fields) for k in fields.keys() if k not in keys]
        expr_where = [await self._field_set(k, fields) for k in keys]

        expr_set_str = ",\n    ".join(expr_set)
        expr_where_str = "\n    AND ".join(expr_where)

        q = f"""
UPDATE {self.db}
SET
    {expr_set_str}
WHERE
    {expr_where_str}
"""

        self.log.debug(f"Query: {q}")
        bot.set_total(len(data))
        if self.progressbar:
            tbar = tqdm(desc="Update", total=len(data))

        if not self.dryrun:
            for chunk in chunks(data, chunk_size):
                await self.conn.executemany(q, chunk)
                await bot.update(len(chunk))
                if self.progressbar:
                    tbar.update(len(chunk))

        if self.progressbar:
            tbar.close()

    @progress_decorator("upsert_many")
    async def upsert_many_async(
        self,
        rows: List[dict],
        keys: List[str],
        unique_keys: List[str] = None,
        update_keys: List[str] = None,
        id_columns: List[str] = None,
        chunk_size: int = 50_000,
        bot: Optional[BotProgressReport] = None,
    ):
        """Run upsert_many on DB. If record not exists, it will be inserted otherwise updated.
        Required index with UNIQUE for all keys fields altogether."""
        assert bot is not None
        self._bot_init(bot)

        self.fields = await self._get_fields()
        if id_columns:
            print("---", id_columns)
            await self._check_keys(id_columns)

        keys = await self._check_keys(keys)
        if update_keys:
            update_keys = await self._check_keys(update_keys)
        data, fields = await self._prepare_data_fields(rows, update_keys=update_keys)

        expr_set = [
            await self._field_set(k, fields)
            for k in fields.keys()
            if (not update_keys and k not in keys) or (update_keys and k in update_keys)
        ]
        # expr_where = [await self._field_set(k, fields) for k in keys]

        expr_set_str = ",\n    ".join(expr_set)
        # expr_where_str = "\n    AND ".join(expr_where)
        expr_conflict_str = ", ".join(unique_keys or keys)
        fields_str = ", ".join(self.escapestr(list(fields.keys())))
        values_str = ", ".join([f"${i+1}" for i in range(len(fields))])

        q = f"""
INSERT INTO {self.db} ({fields_str})
VALUES ({values_str})
        """

        if update_keys:
            q += f"""
ON CONFLICT ({expr_conflict_str}) DO UPDATE
SET
    {expr_set_str}"""
        else:
            q += """
ON CONFLICT DO NOTHING
"""

        #         q = f"""
        # SELECT (SELECT 1 FROM {self.db} WHERE {expr_where_str})
        # """

        self.log.debug(f"Query: {q}")
        bot.set_total(len(data))
        if self.progressbar:
            tbar = tqdm(desc="Upsert", total=len(data))

        if not self.dryrun:
            for chunk in chunks(data, chunk_size):
                await self.conn.executemany(q, chunk)
                await bot.update(len(chunk))
                if self.progressbar:
                    tbar.update(len(chunk))

        if self.progressbar:
            tbar.close()

        # if id_columns:
        #     fields_column = [i for i, f in enumerate(fields) if f in id_columns]
        #     return await self.select_async(
        #         id_columns,
        #         filters={k: None for k in id_columns},
        #         filters_many=[f for i, f in enumerate(data) if i in fields_column],
        #         limit=None,
        #     )

    def insert_many(
        self,
        rows: List[dict],
        keys: List[str],
        id_column: Optional[str] = None,
        desc: Optional[str] = None,
    ) -> List[int]:
        """
        Insert many records in DB. Slower than upsert_many, but returns id_column value.
        ### Examples:
        >>> rows = [{'id': 1, 'is_active': True}, {'id': 2, 'is_active': False}]
        >>> db = dataset.connect("postgres://localhost/test")
        >>> db['table'].insert_many(rows, ['id', 'is_active'])
        """
        if desc:
            self.set_desc(desc)
        return run_async(self.insert_many_async(rows, keys, id_column=id_column))

    @progress_decorator("insert_many")
    async def insert_many_async(
        self,
        rows: List[dict],
        keys: List[str],
        id_column: Optional[str] = None,
        bot: Optional[BotProgressReport] = None,
    ) -> List[int]:
        """Run many inserts on DB."""
        assert bot is not None
        self._bot_init(bot)
        bot.pbar.miniters = 10

        self.fields = await self._get_fields()
        keys = await self._check_keys(keys)
        data, fields = await self._prepare_data_fields(rows, keys)

        fields_str = ", ".join(self.escapestr(list(fields.keys())))
        values_str = ", ".join([f"${i+1}" for i in range(len(fields))])

        q = f"""
INSERT INTO {self.db} ({fields_str})
VALUES ({values_str})
"""
        if id_column is not None:
            q += f"RETURNING {id_column}"

        self.log.debug(f"Query: {q}")
        bot.set_total(len(data))
        if self.progressbar:
            tbar = tqdm(desc="Insert", total=len(data))

        return_ids = []

        if not self.dryrun:
            for item in data:
                r = await self.conn.fetch(q, *item)
                if id_column:
                    return_ids.append(list(r[0].values())[0])
                await bot.update(1)
                if self.progressbar:
                    tbar.update(1)

        if self.progressbar:
            tbar.close()

        return return_ids

    def select(
        self,
        keys: List[str],
        filters: "dict[str, Any]",
        filters_many: Optional[List] = None,
        limit: Optional[int] = None,
    ) -> List[dict]:
        """
        Select data from DB
        ### Examples:
        >>> db = dataset.connect("postgres://localhost/test")
        >>> db['table'].select(['id'], {'is_active': True})
        >>> db['table'].select(['id', 'is_active'], {'is_active': True})
        """
        return run_async(self.select_async(keys, filters, filters_many, limit))

    def select_where(
        self,
        keys: List[str],
        where: str,
        limit: Optional[int] = None,
    ) -> List[dict]:
        """
        Select data from DB
        ### Examples:
        >>> db = dataset.connect("postgres://localhost/test")
        >>> db['table'].select(['id'], {'is_active': True})
        >>> db['table'].select(['id', 'is_active'], {'is_active': True})
        """
        return run_async(self.select_async(keys, {}, where=where, limit=limit))

    @copy_doc(select)
    async def select_async(
        self,
        keys: List[str],
        filters: "dict[str, Any]",
        filters_many: Optional[List] = None,
        where: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[dict]:

        self.fields = await self._get_fields()
        keys = await self._check_keys(keys)
        fields_filters = {k: v for k, v in self.fields.items() if k in filters}

        fields_str = ", ".join(self.escapestr(list(keys)))
        expr_where = [await self._field_set(k, fields_filters) for k in self.fields.keys() if k in filters]
        expr_where_str = where if where else "\n    AND ".join(expr_where)

        q = f"""
SELECT ({fields_str}) FROM {self.db} """

        if filters or where:
            q += f"""
WHERE
    {expr_where_str}"""

        if limit:
            q += f"""
LIMIT {limit}"""

        data_values = [filters[f] for f in fields_filters if f in filters]
        self.log.debug(f"Query: {q}")
        self.log.debug(f"Data prepaired: ({data_values})")
        res = []
        if self.progressbar:
            tbar = tqdm(desc="Select", total=None)

        if not self.dryrun:
            r = await self.conn.fetch(q, *data_values)

            for item in r:
                fields = item.get("row", list(item.values()))
                res.append({keys[i]: v for i, v in enumerate(fields)})

            if self.progressbar:
                tbar.update(len(r))

        if self.progressbar:
            tbar.close()

        return res

    def delete_many(
        self,
        keys: List[str],
        filters: "List[dict[str, Any]]",
        chunk_size: int = 10_000,
        desc: Optional[str] = None,
    ):
        """
        Delete rows by filter in DB
        ### Examples:
        >>> db = dataset.connect("postgres://localhost/test")
        >>> db['table'].delete_many(['id'], [{'id': 1}, {'id': 2}])
        """
        if desc:
            self.set_desc(desc)
        return run_async(self.delete_many_async(keys, filters, chunk_size))

    @copy_doc(delete_many)
    @progress_decorator("delete_many")
    async def delete_many_async(
        self,
        keys: List[List[str]],
        filters: "List[dict[str, Any]]",
        chunk_size: int = 10_000,
        bot: Optional[BotProgressReport] = None,
    ) -> dict:
        """Delete many on DB"""
        assert bot is not None
        self._bot_init(bot)

        self.fields = await self._get_fields()
        keys = await self._check_keys(keys)
        # data, unused_fields = await self._prepare_data(filters)
        data, fields = await self._prepare_data_fields(filters, keys, remove_non_keys=True)
        fields_filters = {k: v for k, v in self.fields.items() if k in keys}

        expr_where = [await self._field_set(k, fields_filters) for k in keys]
        expr_where_str = "\n    AND ".join(expr_where)

        q = f"""
DELETE FROM {self.db}
WHERE
    {expr_where_str}
"""

        self.log.debug(f"Query: {q}")
        res = []
        bot.set_total(total=len(data))
        if self.progressbar:
            tbar = tqdm(desc="Delete", total=len(data))

        if not self.dryrun:
            for chunk in chunks(data, chunk_size):
                await self.conn.executemany(q, chunk)

                await bot.update(len(chunk))
                if self.progressbar:
                    tbar.update(len(chunk))

        if self.progressbar:
            tbar.close()

        return res

    def _records_to_dict(self, records: List[Record]):
        return [dict(rec) for rec in records]

    def query(self, sql: str, values: List[Any] = [], asdict: bool = True):
        """
        Perform raw SQL query
        ### Examples:
        >>> db = dataset.connect("postgres://localhost/test")
        >>> db['table'].query("SELECT * FROM table")
        """
        return run_async(self.query_async(sql, values, asdict))

    @copy_doc(query)
    async def query_async(self, sql: str, values: List[Any] = [], asdict: bool = True):
        assert self.conn is not None
        r = await self.conn.fetch(sql, *values)

        if asdict:
            # if isinstance(r, Record):
            try:
                return self._records_to_dict(r)
            except Exception as e:
                self.log.error("Error processing records_to_dict", exc_info=e)
                pass
        return r

    def execute(self, sql: str, values: List[Any] = []):
        """
        Perform raw SQL execute
        ### Examples:
        >>> db = dataset.connect("postgres://localhost/test")
        >>> db['table'].execute("INSERT (1,2) INTO table")
        """
        return run_async(self.execute_async(sql, values))

    @copy_doc(execute)
    async def execute_async(self, sql: str, values: List[Any] = []):
        assert self.conn is not None
        r = await self.conn.execute(sql, *values)
        return r


# @copy_doc(Dataset.connect_async)
# @copy_kwargs(Dataset.connect_async, Dataset)
def connect(url: str, *args, log_level: Optional[str] = None, **kwargs):
    ds = Dataset(log_level=log_level, bot_token=kwargs.pop("bot_token"), bot_chat=kwargs.pop("bot_chat"))
    ds.connect(url, *args, **kwargs)
    return ds

async def connect_async(url: str, *args, log_level: Optional[str] = None, **kwargs):
    ds = Dataset(log_level=log_level)
    await ds.connect_async(url, *args, **kwargs)
    return ds
