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
import json
import logging
import re
from collections import OrderedDict
from copy import copy
from datetime import datetime
from typing import Any, Callable, Iterable, List, Tuple
from unittest.util import strclass

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
    force_fields: List[str] = list()  # List of fields to force add to query
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

    async def _check_keys(self, keys: List[str]):
        """Check that all keys are valid and exists in table"""
        if keys == "__all__":
            fields = await self._get_fields()
            return list(fields.keys())

        assert len(keys) > 0, "At least one key is required"

        for key in keys:
            if not key in self.fields:
                raise InvalidKeyException(
                    f"Key '{key}' is not exists on table '{self.db}' ({self.fields.keys()})"
                )

        return keys

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
                    val = await self._convert_value(
                        row[field_name], field_type.lower(), field_name
                    )
                except TypeError as exc:
                    raise FieldSerializationException() from exc
                r.append(val)

                if field_name in unused_fields and (
                    val is not None or val in self.force_fields
                ):
                    unused_fields.remove(field_name)

            res.append(r)
        return res, unused_fields

    async def _prepare_data_fields(
        self, rows: List[dict], keys=None, update_keys=None, remove_non_keys: bool = False
    ) -> Tuple[list, dict]:
        """Prepare cleared data and fields dict, remove unused fields"""
        data, unused_fields = await self._prepare_data(rows)
        fields = self.fields.copy()
        remove_ids = []

        if self.ignore_fields:
            unused_fields.extend(self.ignore_fields)

        if self.force_fields:
            for f in self.force_fields:
                unused_fields.remove(f)

        if keys and remove_non_keys: # Remove non selected keys
            for f in fields:
                if not f in keys and not f in unused_fields:
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

    def escapestr(self, tx: str, char: str = '"') -> str:
        esc = lambda x: f"{char}{x}{char}"

        if isinstance(tx, (list, tuple)):
            return list(map(esc, tx))
        else:
            return esc(tx)

    def upsert_many(
        self,
        rows: List[dict],
        keys: List[str],
        update_keys: List[str] = None,
        id_columns: List[str] = None,
        chunk_size: int = 50_000,
    ):
        """Run upsert_many on DB. If record not exists, it will be inserted otherwise updated. Required index with UNIQUE for all keys fields altogether."""
        return run_async(
            self.upsert_many_async(rows, keys, update_keys, id_columns, chunk_size)
        )
        # return asyncio.run(self.upsert_many_async(rows, keys))

    def update_many_filter(
        self,
        filters: "dict[str, Any]",
        values: "dict[str, Any]",
        chunk_size: int = 50_000,
    ):
        """Run update_many on DB. Only updates already existing records."""
        return run_async(self.update_many_filter_async(filters, values, chunk_size))

    async def update_many_filter_async(
        self,
        filters: "dict[str, Any]",
        values: "dict[str, Any]",
        chunk_size: int = 50_000,
    ):
        """Run update_many on DB. Only updates already existing records."""
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
        expr_where = [
            await self._field_set(k, fields_all) for k in fields_filters.keys()
        ]

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

            if self.progressbar:
                tbar.update(int(updated_count))

        if self.progressbar:
            tbar.close()

    def update_many(self, rows: List[Any], keys: List[str], chunk_size: int = 50_000):
        """Run update_many on DB. Only updates already existing records."""
        return run_async(self.update_many_async(rows, keys, chunk_size))

    async def update_many_async(
        self, rows: List[dict], keys: List[str], chunk_size: int = 50_000
    ):
        """Run update_many on DB. Only updates already existing records."""
        self.fields = await self._get_fields()

        await self._check_keys(keys)
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
        self,
        rows: List[dict],
        keys: List[str],
        update_keys: List[str] = None,
        id_columns: List[str] = None,
        chunk_size: int = 50_000,
    ):
        """Run upsert_many on DB. If record not exists, it will be inserted otherwise updated. Required index with UNIQUE for all keys fields altogether."""
        self.fields = await self._get_fields()

        if id_columns:
            await self._check_keys(id_columns)

        keys = await self._check_keys(keys)
        if update_keys:
            update_keys = await self._check_keys(update_keys)
        data, fields = await self._prepare_data_fields(rows, update_keys=update_keys)

        expr_set = [
            await self._field_set(k, fields)
            for k in fields.keys()
            if (not update_keys and not k in keys) or (update_keys and k in update_keys)
        ]
        expr_where = [await self._field_set(k, fields) for k in keys]

        expr_set_str = ",\n    ".join(expr_set)
        expr_where_str = "\n    AND ".join(expr_where)
        expr_conflict_str = ", ".join(keys)
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
            q += f"""
ON CONFLICT DO NOTHING
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

        # if id_columns:
        #     fields_column = [i for i, f in enumerate(fields) if f in id_columns]
        #     return await self.select_async(
        #         id_columns,
        #         filters={k: None for k in id_columns},
        #         filters_many=[f for i, f in enumerate(data) if i in fields_column],
        #         limit=None,
        #     )

    def insert_many(
        self, rows: List[dict], keys: List[str], id_column: str = None
    ) -> List[int]:
        return run_async(self.insert_many_async(rows, keys, id_column=id_column))

    async def insert_many_async(
        self, rows: List[dict], keys: List[str], id_column: str = None
    ) -> List[int]:
        """Run many inserts on DB."""

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
        if self.progressbar:
            tbar = tqdm(desc="Insert", total=len(data))

        return_ids = []

        if not self.dryrun:
            for item in data:
                r = await self.conn.fetch(q, *item)
                if id_column:
                    return_ids.append(list(r[0].values())[0])
                if self.progressbar:
                    tbar.update(1)

        if self.progressbar:
            tbar.close()

        return return_ids

    def select(
        self,
        keys: List[dict],
        filters: "dict[str, Any]",
        filters_many: List = None,
        limit: int = None,
    ):
        return run_async(self.select_async(keys, filters, filters_many, limit))

    async def select_async(
        self,
        keys: List[dict],
        filters: "dict[str, Any]",
        filters_many: List = None,
        limit: int = None,
    ) -> dict:

        self.fields = await self._get_fields()
        keys = await self._check_keys(keys)
        fields_filters = {k: v for k, v in self.fields.items() if k in filters}

        fields_str = ", ".join(self.escapestr(list(keys)))
        expr_where = [await self._field_set(k, fields_filters) for k in filters.keys()]
        expr_where_str = "\n    AND ".join(expr_where)

        q = f"""
SELECT ({fields_str}) FROM {self.db} """

        if filters:
            q += f"""
WHERE
    {expr_where_str}"""

        if limit:
            q += f"""
LIMIT {limit}"""

        self.log.debug(f"Query: {q}")
        res = []
        if self.progressbar:
            tbar = tqdm(desc="Select", total=None)

        if not self.dryrun:
            data_values = list(filters.values())
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
        keys: List[dict],
        filters: "List[dict[str, Any]]",
        chunk_size: int = 10_000
    ):
        return run_async(self.delete_many_async(keys, filters, chunk_size))

    async def delete_many_async(
        self,
        keys: List[dict],
        filters: "dict[str, Any]",
        chunk_size: int = 10_000
    ) -> dict:

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
        if self.progressbar:
            tbar = tqdm(desc="Delete", total=len(data))

        if not self.dryrun:
            for chunk in chunks(data, chunk_size):
                await self.conn.executemany(q, chunk)

                if self.progressbar:
                    tbar.update(len(chunk))

        if self.progressbar:
            tbar.close()

        return res
