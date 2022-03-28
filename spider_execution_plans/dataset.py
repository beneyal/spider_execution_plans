"""Create dataset of execution plans."""

import json
import re
from pathlib import Path
from typing import Tuple

import pandas as pd
import pyodbc

CONNECTION_STRING = (
    "Driver={ODBC Driver 17 for SQL Server};"
    "Server=BENEYAL;"
    "Database=spider;"
    "Trusted_Connection=yes;"
)

SCHEMAS = [p.name for p in Path("schemas").glob("*") if p.name != ".git"]
EXCLUDE = ["baseball_1", "college_2", "hr_1", "sakila_1", "soccer_1", "wta_1"]
AFTER_FROM_KEYWORDS = ["where", "order", "group"]
SPIDER_PATH = Path("C:/Users/beney/Desktop/spider")
SPIDER_TRAIN = SPIDER_PATH / "train_spider.json"
SPIDER_DEV = SPIDER_PATH / "dev.json"
SPIDER_TABLES = SPIDER_PATH / "tables.json"


def add_execution_plan(
    split: list, tables: dict, cursor: pyodbc.Cursor
) -> Tuple[list, list]:
    instances = []
    errors = []
    for instance in split:
        db_id = instance["db_id"]
        if db_id in EXCLUDE:
            continue
        table = tables[db_id]
        query_tokens = instance["query_toks"]
        lowercase_query = instance["query"].lower()

        if "group by" in lowercase_query:
            query_tokens = copy_columns_from_select_to_groupby(query_tokens)
        if "select distinct" in lowercase_query and "order by" in lowercase_query:
            query_tokens = copy_orderby_to_select_distinct(query_tokens)
        if "limit" in lowercase_query:
            query_tokens = convert_limit_to_top(query_tokens)

        query_tokens = ["'" if t in ("``", "''") else t for t in query_tokens]
        query_tokens = add_schema_name_to_tables(db_id, query_tokens, table)
        new_query = re.sub(r"' ([^']+) '", r"'\1'", " ".join(query_tokens))
        new_query = re.sub(r'"([^"]+)"', r"'\1'", new_query)
        print(f"{db_id}: {new_query}")
        try:
            ep_xml = cursor.execute(new_query).fetchone()[0]
            instance["ep"] = ep_xml
            instances.append(instance)
        except pyodbc.ProgrammingError as e:
            # This is a hack.
            # There are several cases of SELECT ... FROM (SELECT ... )
            # This doesn't work in SQL Server, unless the FROM gets an alias,
            # so I artificially add an alias at the end of the query.
            if "Incorrect syntax near ')'." in e.args[1]:
                new_query = new_query + " AS T10"
                try:
                    ep_xml = cursor.execute(new_query).fetchone()[0]
                    instance["ep"] = ep_xml
                    instances.append(instance)
                except pyodbc.ProgrammingError as e:
                    errors.append({"db_id": db_id, "query": new_query, "error": e})
            else:
                errors.append({"db_id": db_id, "query": new_query, "error": e})
    return instances, errors


def copy_orderby_to_select_distinct(query_tokens):
    lowercase_query_tokens = [t.lower() for t in query_tokens]
    order_idx = lowercase_query_tokens.index("order")
    for i in range(order_idx, -1, -1):
        if lowercase_query_tokens[i] == "distinct":
            distinct_idx = i
            break
    if lowercase_query_tokens[order_idx + 2] in ("sum", "avg", "count", "min", "max"):
        orderby_arg = query_tokens[
            order_idx + 2 : lowercase_query_tokens.index(")", order_idx + 2)
        ]
    else:
        orderby_arg = query_tokens[order_idx + 2]
    from_idx = lowercase_query_tokens.index("from")
    select_args = query_tokens[distinct_idx + 1 : from_idx]
    if type(orderby_arg) == list:
        query_tokens = (
            query_tokens[: distinct_idx + 1]
            + orderby_arg
            + [","]
            + query_tokens[distinct_idx + 1 :]
        )
    elif orderby_arg not in select_args:
        query_tokens = (
            query_tokens[: distinct_idx + 1]
            + [orderby_arg, ","]
            + query_tokens[distinct_idx + 1 :]
        )
    return query_tokens


def copy_columns_from_select_to_groupby(query_tokens):
    lowercase_query_tokens = [t.lower() for t in query_tokens]
    new_query_tokens = []
    i = 0
    while i < len(query_tokens):
        if lowercase_query_tokens[i] == "select":
            if lowercase_query_tokens[i + 1] == "distinct":
                select_idx = i + 1
                new_query_tokens.extend(query_tokens[i : i + 2])
                i += 2
            else:
                select_idx = i
                new_query_tokens.append(query_tokens[i])
                i += 1
        elif lowercase_query_tokens[i] == "from":
            select_args = [t for t in query_tokens[select_idx + 1 : i] if t != ","]
            lowercase_select_args = [t.lower() for t in select_args]
            for agg in ("sum", "avg", "count", "min", "max"):
                while agg in lowercase_select_args:
                    idx = lowercase_select_args.index(agg)
                    del select_args[idx : lowercase_select_args.index(")", idx) + 1]
                    lowercase_select_args = [t.lower() for t in select_args]
            new_query_tokens.append(query_tokens[i])
            i += 1
        elif (
            lowercase_query_tokens[i] == "group"
            and lowercase_query_tokens[i + 1] == "by"
        ):
            groupby_arg = query_tokens[i + 2]
            if groupby_arg not in select_args:
                select_args.append(groupby_arg)
            new_query_tokens.extend(query_tokens[i : i + 2])
            new_query_tokens.extend(" , ".join(select_args).split())
            i += 3
        else:
            new_query_tokens.append(query_tokens[i])
            i += 1
    return new_query_tokens


def add_schema_name_to_tables(db_id, query_tokens, table):
    new_query_tokens = []
    lowercase_table_names = [t.lower() for t in table["table_names_original"]]
    is_from = False
    for token in query_tokens:
        lower = token.lower()
        if lower == "from":
            is_from = True
        if is_from and lower in lowercase_table_names:
            new_query_tokens.append(f"{db_id}.{token}")
        else:
            new_query_tokens.append(token)
        if lower in AFTER_FROM_KEYWORDS:
            is_from = False
    return new_query_tokens


def convert_limit_to_top(query_tokens):
    lowercase_query_tokens = [t.lower() for t in query_tokens]
    while "limit" in lowercase_query_tokens:
        limit_idx = lowercase_query_tokens.index("limit")
        limit_arg = lowercase_query_tokens[limit_idx + 1]
        del query_tokens[limit_idx : limit_idx + 2]
        for i in range(limit_idx, -1, -1):
            if lowercase_query_tokens[i] == "select":
                select_idx = i
                break
        top = ["TOP", str(limit_arg)]
        if "distinct" in lowercase_query_tokens[select_idx:]:
            distinct_idx = lowercase_query_tokens.index("distinct")
            query_tokens = (
                query_tokens[: distinct_idx + 1]
                + top
                + query_tokens[distinct_idx + 1 :]
            )
        else:
            query_tokens = (
                query_tokens[: select_idx + 1] + top + query_tokens[select_idx + 1 :]
            )
        lowercase_query_tokens = [t.lower() for t in query_tokens]
    return query_tokens


def main() -> None:
    with open(SPIDER_TABLES, mode="r", encoding="utf-8") as f:
        tables = json.load(f)
        tables = {table["db_id"]: table for table in tables}
    with open(SPIDER_TRAIN, mode="r", encoding="utf-8") as f:
        train = json.load(f)
    with open(SPIDER_DEV, mode="r", encoding="utf-8") as f:
        dev = json.load(f)

    connection = pyodbc.connect(CONNECTION_STRING)
    cursor = connection.cursor()
    cursor.execute("SET SHOWPLAN_XML ON")

    new_train, errors_1 = add_execution_plan(train, tables, cursor)
    new_dev, errors_2 = add_execution_plan(dev, tables, cursor)

    errors_df = pd.DataFrame(data=errors_1 + errors_2)
    errors_df.to_csv("errors.csv", index=False)

    with open("dataset/train_spider_with_ep.json", mode="w", encoding="utf-8") as f:
        json.dump(new_train, f, indent=2)
    with open("dataset/dev_spider_with_ep.json", mode="w", encoding="utf-8") as f:
        json.dump(new_dev, f, indent=2)

    print("Done!")


if __name__ == "__main__":
    # TODO Change column types, use this query: SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS
    # TODO Convert types from Spider to SQL Server
    main()
