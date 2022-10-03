"""
PostgreSQL batch upsert/update util
Example:

```python
db = dataset.connect("postgres://localhost/test")
table = db["test_example"]
rows = [
    {
        "field": "value",
    }
]
table.upsert(rows, ["key1", "key2"])
```

"""

import asyncio
import copyreg
import json
import logging
import re
from collections import OrderedDict
from copy import copy
from datetime import datetime
from typing import Any, Callable, Iterable, List, Tuple

import asyncpg
from asyncpg.connection import Connection
from tqdm import tqdm

logging.basicConfig()


class DatasetException(ValueError):
    pass


class DBEmptyException(DatasetException):
    pass


class InvalidKeyException(DatasetException):
    pass


class FieldSerializationException(DatasetException):
    pass


def connect(url: str, *args, log_level: str = None, **kwargs):
    """Connect to PostgreSQL DB"""

    ds = Dataset(log_level=log_level)
    ds.connect(url, *args, **kwargs)
    return ds


def run_async(func: Callable):
    """Run async command as sync"""
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(func)


def chunks(lst: Iterable, n: int):
    """Iterate data in chunks"""
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


class Dataset:
    conn: Connection = None
    conn_url: str = None
    db: str = None  # Filled by cls['table_name']
    fields: dict = None
    ignore_fields: List[str] = list()  # List of fields to ignore
    progressbar = True  # Show tqdm progressbar
    dryrun = False  # Run in dry mode, just print query

    def __init__(self, log_level: str = None) -> None:
        self.log = logging.getLogger("Dataset")
        self.log.setLevel(log_level or "INFO")

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

    def connect(self, url: str, *args, **kwargs):
        """Run connect_async in run_async"""
        self.conn_url = url
        return run_async(self.connect_async(url, *args, **kwargs))

    def disconnect(self, *args, **kwargs):
        """Run disconnect_async in run_async"""
        return run_async(self.disconnect_async(*args, **kwargs))

    async def connect_async(self, url: str) -> Connection:
        """Connect to PostreSQL DB"""
        self.log.debug("Connecting to DB...")
        self.conn = await asyncpg.connect(url)
        return self.conn

    async def disconnect_async(self):
        self.log.debug("Disconnecting from DB")
        return await self.conn.close()

    def _check_table(self):
        """Check is table selected"""
        if not self.db:
            raise DBEmptyException("Call dataset.connection()['table_name'] ")

    def _check_keys(self, keys: List[str]):
        """Check that all keys are valid and exists in table"""
        assert len(keys) > 0, "At least one key is required"

        for key in keys:
            if not key in self.fields:
                raise InvalidKeyException(
                    f"Key '{key}' is not exists on table '{self.db}' ({self.fields.keys()})"
                )

    async def _get_fields(self) -> "OrderedDict[str, str]":
        """Get dict of DB fields {name: field_type}"""
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

    # async def _iter_array_type(self, val: Iterable, field_type: str):
    #     for item in val:
    #         yield self._convert_value(item, field_type)

    async def _convert_value(self, val: Any, field_type: str) -> Any:
        """Convert passed value to DB field type"""

        # field_arr_regex = re.compile(r"\[\d*\]")
        # field_arr = field_arr_regex.search(field_type)
        # field_type_normal = field_arr_regex.sub("", field_type, count=1)

        # if field_arr:
        #     return list(await self._iter_array_type(val, field_type_normal))

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
            assert isinstance(
                val, (list, tuple)
            ), f"Invalid type '{field_type}' of ({val})"
            return val
        elif field_type in ["json", "jsonb"]:
            assert isinstance(
                val, (list, tuple, dict)
            ), f"Invalid type '{field_type}' of ({val})"
            return json.dumps(val, ensure_ascii=False)

        ## Unknown / ignored

        elif field_type in ["USER-DEFINED"]:
            pass
        else:
            self.log.debug(f"Unknown field type: '{field_type}'")

        return val

    async def _prepare_data(self, rows: List[dict]) -> Tuple[List[Any], List[str]]:
        """Prepare data for DB (convert rows dict to list)"""
        res = []
        unused_fields = list(self.fields.keys())

        for row in rows:
            r = []
            for field_name, field_type in self.fields.items():
                if row.get(field_name) is None:
                    r.append(None)
                    continue

                try:
                    val = await self._convert_value(row[field_name], field_type.lower())
                except TypeError as exc:
                    raise FieldSerializationException() from exc
                r.append(val)

                if val and field_name in unused_fields:
                    unused_fields.remove(field_name)

            res.append(r)
        return res, unused_fields

    async def _prepare_data_fields(
        self, rows: List[dict], keys=None
    ) -> Tuple[list, dict]:
        """Prepare cleared data and fields dict, remove unused fields"""
        data, unused_fields = await self._prepare_data(rows)
        fields = self.fields.copy()
        remove_ids = []

        if self.ignore_fields:
            unused_fields.extend(self.ignore_fields)

        if keys:
            for f in fields:
                if not f in keys and not f in unused_fields:
                    unused_fields.append(f)

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
            l = []
            for i, elem in enumerate(d):
                if i in remove_ids:
                    continue
                l.append(elem)

            res_data.append(l)

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
        """Field set SQL expression generation"""
        field_id = await self._field_id(field, fields=fields)
        return f"{field} = ${field_id + 1}"

    def upsert_many(self, rows: List[dict], keys: List[str], chunk_size: int = 50_000):
        """Run upsert_many on DB. If record not exists, it will be inserted otherwise updated. Required index with UNIQUE for all keys fields altogether."""
        return run_async(self.upsert_many_async(rows, keys, chunk_size))
        # return asyncio.run(self.upsert_many_async(rows, keys))

    def update_many(self, rows: List[Any], keys: List[str], chunk_size: int = 50_000):
        """Run update_many on DB. Only updates already existing records."""
        return run_async(self.update_many_async(rows, keys, chunk_size))

    async def update_many_async(
        self, rows: List[dict], keys: List[str], chunk_size: int = 50_000
    ):
        """Run update_many on DB. Only updates already existing records."""
        self.fields = await self._get_fields()

        self._check_keys(keys)
        data, fields = await self._prepare_data_fields(rows)

        expr_set = [
            await self._field_set(k, fields) for k in fields.keys() if not k in keys
        ]
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
        if self.progressbar:
            tbar = tqdm(desc="Update", total=len(data))

        if not self.dryrun:
            for chunk in chunks(data, chunk_size):
                await self.conn.executemany(q, chunk)
                if self.progressbar:
                    tbar.update(len(chunk))

        if self.progressbar:
            tbar.close()

    async def upsert_many_async(
        self, rows: List[dict], keys: List[str], chunk_size: int = 50_000
    ):
        """Run upsert_many on DB. If record not exists, it will be inserted otherwise updated. Required index with UNIQUE for all keys fields altogether."""
        self.fields = await self._get_fields()

        self._check_keys(keys)
        data, fields = await self._prepare_data_fields(rows)

        expr_set = [
            await self._field_set(k, fields) for k in fields.keys() if not k in keys
        ]
        expr_where = [await self._field_set(k, fields) for k in keys]

        expr_set_str = ",\n    ".join(expr_set)
        expr_where_str = "\n    AND ".join(expr_where)
        expr_conflict_str = ", ".join(keys)
        fields_str = ", ".join(fields.keys())
        values_str = ", ".join([f"${i+1}" for i in range(len(fields))])

        q = f"""
INSERT INTO {self.db} ({fields_str})
VALUES ({values_str})
ON CONFLICT ({expr_conflict_str}) DO UPDATE
SET
    {expr_set_str}
        """

        #         q = f"""
        # SELECT (SELECT 1 FROM {self.db} WHERE {expr_where_str})
        # """

        self.log.debug(f"Query: {q}")
        if self.progressbar:
            tbar = tqdm(desc="Upsert", total=len(data))

        if not self.dryrun:
            for chunk in chunks(data, chunk_size):
                await self.conn.executemany(q, chunk)
                if self.progressbar:
                    tbar.update(len(chunk))

        if self.progressbar:
            tbar.close()
