"""Create the Spider schemas in Microsoft SQL Server."""

from pathlib import Path

import pyodbc

CONNECTION_STRING = "Driver={ODBC Driver 17 for SQL Server};Server=BENEYAL;Database=spider;Trusted_Connection=yes;"

SCHEMA_PATHS = Path("C:/Users/beney/Desktop/spider/database").glob("*")

if __name__ == "__main__":
    conn = pyodbc.connect(CONNECTION_STRING, autocommit=True)
    cursor = conn.cursor()
    for p in SCHEMA_PATHS:
        cursor.execute(f"CREATE SCHEMA {p.name}")
    cursor.close()
    conn.close()
