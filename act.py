import pandas as pd

import sys
from datetime import datetime, timedelta

from shared_module2 import loadvp3
from shared_chouse import contc, save_file_data_ch, intoclickhouse


def examples():
    """
    хранение шаблонов
    """
    # df["Количество"] = df["Количество"].astype("Float64")

    # dft = df.select_dtypes("str")
    # df[dft.columns] = dft.apply(lambda x: x.fillna(""))

    # dft = df.select_dtypes("number")
    # df[dft.columns] = dft.apply(lambda x: x.fillna(0.0))

    # fill_value = pd.Timestamp("1970-01-01")
    # dft = df.select_dtypes("datetime")
    # df[dft.columns] = dft.apply(lambda x: x.fillna(fill_value))

    # print("--databases-----------------------")
    # result = client.query("SHOW DATABASES")
    # for row in result.result_set:
    #     print(row[0])
    # print("-------------------------")

    # print(f"--tables in database:{db_name}-----------------------")
    # result = client.query("SHOW TABLES")
    # for row in result.result_set:
    #     print(row[0])  # Print the table name from the first column of each row
    # print("-------------------------")

    return 0


def chtest2(client):
    # 1. Read the Parquet file to infer the schema
    # df = pd.read_parquet(r"data\Сборная_план2.parquet")
    df = pd.read_parquet(r"data\pickle_na.parquet", dtype_backend="pyarrow")
    # df = df.convert_dtypes(dtype_backend="pyarrow")

    # print(df)
    print(df.dtypes)
    # sys.exit()

    # print(df["Количество"].sum())
    # print(df["УчетнЦена"].sum())
    # print(df["Стоимость"].sum())

    table_name = "pickle_na"
    intoclickhouse(client, df, table_name)

    sql_query = f"SELECT * FROM {table_name}"
    df2 = client.query_df(sql_query)

    print(df2)
    print(df2.dtypes)

    # print(df2["Количество"].sum())
    # print(df2["УчетнЦена"].sum())
    # print(df2["Стоимость"].sum())

    # loadvp3(r"..\datas\настройки\загрузка ВП")

    # соединение


def ch_dict(client):

    # 1. Create a table in ClickHouse
    client.command("DROP TABLE IF EXISTS inventory")
    client.command(
        "CREATE TABLE IF NOT EXISTS new_table (key UInt32, value String, metric Float64) ENGINE MergeTree ORDER BY key"
    )

    print("table new_table created or exists already!\n")

    row1 = [1000, "String Value 1000", 5.233]
    row2 = [2000, "String Value 2000", -107.04]
    data = [row1, row2]
    client.insert("new_table", data, column_names=["key", "value", "metric"])

    print("written 2 rows to table new_table\n")
    print("Data inserted successfully using a list of dictionaries.")


if __name__ == "__main__":
    db_name = "db_ilja"
    # client = contc()

    # client.command(f"CREATE DATABASE IF NOT EXISTS {db_name}")

    client = contc(db_name)

    chtest2(client)
    # ch_dict(client)
    save_file_data_ch(
        client, datetime.today() + timedelta(days=1), r"C:\uv\chouse\act.py"
    )

    if client.ping():
        client.close()
        print("\nConnection to ClickHouse closed.")
