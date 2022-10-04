import asyncio
import logging
from datetime import datetime
from random import randint

from click import progressbar

import dataset

db = dataset.connect("postgres://test:Qwerty123$$@45.155.207.86/test", log_level="DEBUG")
table = db["test_example"]
# table = db["test_batch"]
db.dryrun = True

rows = [
    {
        'name': 'Тест',
        'type': 'phone',
        'value': '77072125198',
        'source': 'kgd.gov.kz',
        'identifier': '180640032599',
        'jurisdiction': 'kz',
        'date': '14.08.2022',
        "arr_int": [1, 2, 3],
        "arr_json": ["1", "2", "3"],
        "json": {"a": True, "b": 123},
        "jsonb": {"a": True, "b": 123},
    },
    {
        'name': 'Тест',
        'type': 'phone',
        'value': '77072125198',
        'source': 'kgd.gov.kz',
        'identifier': '1806400325992',
        'jurisdiction': 'kz',
        'date': '2022.08.14',
    },
    {
        'name': 'Тест',
        'type': 'phone',
        'value': '77072125198',
        'source': 'kgd.gov.kz',
        'identifier': '1806400325993',
        'jurisdiction': 'kz',
        'date': '2022-08-14',
    },
]

# for i in range(20):
#     rows.append({
#         "id1": i % 2 == 0,
#         "identifier": i,
#         "jurisdiction": f"Juris {i}",
#         "test_string": randint(1, 100),
#         "date": datetime.today(),
#     })

# keys = ["identifier", "id1"]
keys = ['identifier', 'jurisdiction', 'type', 'value']

table.upsert_many(rows, keys, update_keys=["type", "value"])
table.update_many(rows, keys)
