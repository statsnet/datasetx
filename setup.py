from setuptools import setup

setup(
    name="datasetx",
    py_modules=["datasetx"],
    description="PostgreSQL dataset export",
    version="0.1",
    author="Djangoner",
    author_email="djangoner6@gmail.com",
    keywords=["postgres", "postgreSQL", "DB", "database", "dataset"],
    install_requires=[
        "asyncpg",
        "tqdm",
        "python-dotenv",
        "aiogram"
    ]
)
