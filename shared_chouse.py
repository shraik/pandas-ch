# pip install sqlalchemy-cratedb==0.42.0.dev2
import clickhouse_connect
from clickhouse_connect.driver.exceptions import DatabaseError
import pandas as pd
import sys
from datetime import datetime

from os import environ as int_env
from sqlalchemy import inspect


if dict(int_env).get("AIRFLOW_HOME", None) is not None:
    from smbclient import open_file, register_session, stat, scandir as scandir_smb  # type: ignore


def save_file_data(engine, modified, ftl, gl_dagmode=True) -> bool:
    """записать дату файла в БД"""
    if gl_dagmode is False and engine is None:
        # локальный запуск и пустой engine - инициализируем локальную базу sqlite
        engine = initdb()

    conf_dict = pd.read_sql_table("config_vp", engine).set_index("index").to_dict()
    conf_dict["config"][ftl] = modified
    conf_df = pd.DataFrame(conf_dict)
    conf_df.to_sql("config_vp", engine, if_exists="replace")
    return True


def check_file_data(engine, modified, ftl) -> bool:
    """Возвращает false если дата сохраненного имени файла >= полученной на вход, иначе возвращает true.
    engine - sqlalchemy
    modified - дата файла
    ftl - имя файла
    """
    if engine is None:
        engine = initdb()

    inspector = inspect(engine)
    lst_t = inspector.get_table_names()
    if "config_vp" not in lst_t:
        # если нет таблицы, создать начальный конфиг и записать в БД
        datetime_object = datetime(2020, 1, 1, 0, 0, 0)
        values = {ftl: datetime_object}
        config_dct = {"config": values}
        conf_df = pd.DataFrame(config_dct)
        conf_df.to_sql("config_vp", engine, if_exists="replace")
    else:
        conf_dict = pd.read_sql_table("config_vp", engine).set_index("index").to_dict()
        if conf_dict["config"].get(ftl, None) is not None:
            if conf_dict["config"][ftl] >= modified:
                # сохранённая дата больше или = дате файла
                return False

    return True


def contc(
    dbname="default", hostip="192.168.203.128"
) -> clickhouse_connect.driver.client:
    try:
        client = clickhouse_connect.get_client(
            host=hostip,
            port=8123,
            username="default",
            password="",  # Your password, if any
            database=dbname,
        )
    except DatabaseError as e:
        print("Caught a ClickHouse DatabaseError:")
        print(f"Error Code: {e.args[0]}")
        if "Code: 81" in str(e):
            print(
                f"This is the specific 'Database {dbname} doesn\\'t exist' error (Code 81)."
            )
            client = clickhouse_connect.get_client(
                host=hostip,
                port=8123,
                username="default",
                password="",  # Your password, if any
                database="default",
            )
            client.command(f"CREATE DATABASE {dbname}")
            client = clickhouse_connect.get_client(
                host=hostip,
                port=8123,
                username="default",
                password="",  # Your password, if any
                database=dbname,
            )
        else:
            print("This is a different type of DatabaseError.")
            sys.exit(0)
    return client


def intoclickhouse(client, df, table_name):
    """Записать датафрейм в clickhouse.
    Если в БД будет существовать таблица с таким имененем, она будет перезаписна.

    Args:
        client (_type_): соединение к clickhouse
        df (_type_): датафрейм для записи в clickhouse
        table_name (_type_): имя таблицы в clickhouse
    """
    create_table_schema(client, df, table_name)
    # Optional: Insert the data using the client's insert method

    backend_np = False
    for dtype in df.dtypes:
        if str(dtype).endswith("[pyarrow]") is not True:
            backend_np = True
            break

    if backend_np:
        df2 = df.convert_dtypes(dtype_backend="pyarrow")
        client.insert_df_arrow(table_name, df2)
    else:
        client.insert_df_arrow(table_name, df)

    print(f"Data from dataframe inserted into clickhouse table '{table_name}'.")


def create_table_schema(client, df, table_name):
    # 2. Map Pandas/Parquet dtypes to ClickHouse types (this is a simplified mapping)
    # A more robust implementation would handle nested types and specific ClickHouse types
    dtype_mapping = {
        "int64": "Int64",
        "int64[pyarrow]": "Int64",
        "float64": "Float64",
        "Float64": "Float64",
        "double[pyarrow]": "Float64",
        "object": "String",
        "bool": "Bool",
        "datetime64[ns]": "DateTime64(3)",
        "timestamp[ns][pyarrow]": "DateTime64(3)",
        "str": "String",
        "string[pyarrow]": "String",
        "large_string[pyarrow]": "String",
    }
    # dft = df.select_dtypes("str")
    # df[dft.columns] = dft.apply(lambda x: x.fillna(""))

    # df = df.convert_dtypes(dtype_backend="pyarrow")
    print(df.dtypes)

    columns_sql = []
    for col_name, dtype in zip(df.columns, df.dtypes):
        ch_type = dtype_mapping.get(
            str(dtype), "String"
        )  # Default to String if not found
        columns_sql.append(f"`{col_name}` {ch_type}")

    schema_sql = ", ".join(columns_sql)

    create_table_sql = f"""
    CREATE TABLE {table_name} (
        {schema_sql}
    ) ENGINE = MergeTree()
    ORDER BY tuple(); -- Use an appropriate ORDER BY clause for your data
    """

    # 3. Connect to ClickHouse and execute the CREATE TABLE statement
    # Replace with your actual ClickHouse connection details

    # Drop the table if it exists for a clean run
    client.command(f"DROP TABLE IF EXISTS {table_name}")

    client.command(create_table_sql)

    print(f"Table '{table_name}' created with schema inferred from dataframe.")


def save_file_data_ch(client, modified, ftl, gl_dagmode=True) -> bool:
    """
    Записать дату файла в БД

    :param client: Клиент clickhouse
    :param modified: дата файла
    :param ftl: имя файла
    :param gl_dagmode: признак запуска из DAG, по умолчанию True
    :return: код возврата
    :rtype: bool
    """

    table_name = "config_vp"
    # проверка существования таблицы
    result = client.command(f"EXISTS {table_name}")

    if result != 1:
        #     print(f"The table '{table_name}' exists.")
        # else:
        print(f"The table '{table_name}' does not exist.")

        # если нет таблицы, создать начальный конфиг и записать в БД
        # datetime_object = datetime(2020, 1, 1, 0, 0, 0)
        datetime_object = modified

        values = {ftl: datetime_object}
        config_dct = {"config": values}

        conf_df1 = (
            pd.DataFrame(config_dct)
            .reset_index()
            .convert_dtypes(dtype_backend="pyarrow")
        )

        create_table_schema(client, conf_df1, table_name)
        client.insert_df_arrow(table_name, conf_df1)
    else:
        conf_dict = (
            client.query_df(f"SELECT * FROM {table_name}").set_index("index").to_dict()
        )
        conf_dict["config"][ftl] = modified
        conf_df = (
            pd.DataFrame(conf_dict)
            .reset_index()
            .convert_dtypes(dtype_backend="pyarrow")
        )

        # сброс содержимого таблицы и вставка нового
        client.command(f"TRUNCATE TABLE {table_name}")
        client.insert_df_arrow(table_name, conf_df)
    return True


# ----------start--------------
if __name__ == "__main__":
    print("модуль с общими процедурами")
