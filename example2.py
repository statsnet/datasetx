from datetime import datetime
from random import randint

import logging
logging.basicConfig(level="DEBUG")

import dataset

db = dataset.connect("postgres://localhost/test")
table = db["test_batch"]

rows = []

for i in range(10):
    rows.append(
        {
            "id1": i,
            "identifier": i,
            "jurisdiction": f"Juris {i}",
            "test_string": randint(1, 100),
            "date": datetime.today(),
        }
    )

print(rows)

keys = ["identifier", "id1"]  # Keys rows should be CONTSTRAINT UNIQUE

table.update_many(rows, keys)
