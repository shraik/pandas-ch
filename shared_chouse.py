# pip install sqlalchemy-cratedb==0.42.0.dev2
import pyarrow as pa
import clickhouse_connect
from clickhouse_connect.driver.exceptions import DatabaseError
import pandas as pd
import sys
from datetime import datetime
import re

# from os import environ as int_env
# from sqlalchemy import inspect


# if dict(int_env).get("AIRFLOW_HOME", None) is not None:
#     from smbclient import open_file, register_session, stat, scandir as scandir_smb  # type: ignore


# def save_file_data(engine, modified, ftl, gl_dagmode=True) -> bool:
#     """записать дату файла в БД"""
#     if gl_dagmode is False and engine is None:
#         # локальный запуск и пустой engine - инициализируем локальную базу sqlite
#         engine = initdb()

#     conf_dict = pd.read_sql_table("config_vp", engine).set_index("index").to_dict()
#     conf_dict["config"][ftl] = modified
#     conf_df = pd.DataFrame(conf_dict)
#     conf_df.to_sql("config_vp", engine, if_exists="replace")
#     return True


# def check_file_data(engine, modified, ftl) -> bool:
#     """Возвращает false если дата сохраненного имени файла >= полученной на вход, иначе возвращает true.
#     engine - sqlalchemy
#     modified - дата файла
#     ftl - имя файла
#     """
#     if engine is None:
#         engine = initdb()

#     inspector = inspect(engine)
#     lst_t = inspector.get_table_names()
#     if "config_vp" not in lst_t:
#         # если нет таблицы, создать начальный конфиг и записать в БД
#         datetime_object = datetime(2020, 1, 1, 0, 0, 0)
#         values = {ftl: datetime_object}
#         config_dct = {"config": values}
#         conf_df = pd.DataFrame(config_dct)
#         conf_df.to_sql("config_vp", engine, if_exists="replace")
#     else:
#         conf_dict = pd.read_sql_table("config_vp", engine).set_index("index").to_dict()
#         if conf_dict["config"].get(ftl, None) is not None:
#             if conf_dict["config"][ftl] >= modified:
#                 # сохранённая дата больше или = дате файла
#                 return False

#     return True


def timer(name: str, startTime=None):
    """_summary_

    Args:
        name (str): Наименование таймера для вывода на консоль.
        startTime (datetime, optional): Время запуска. Defaults to None.

    Returns:
        datetime: Время запуска или время прошедшее с полученной даты запуска
    """
    if startTime:
        elapsedt = datetime.now() - startTime
        print(f"Таймер: Прошло времени для [{name}]: {elapsedt}")
        return elapsedt
    else:
        startTime = datetime.now()
        print(f"Таймер: Запущен [{name}] at {startTime}")
        return startTime


def check_file_data_ch(
    client: clickhouse_connect.driver.client,  # type: ignore
    modified: datetime,
    ftl: str,
) -> bool:
    """Записать датафрейм в clickhouse.
    Если в БД будет существовать таблица с таким имененем, она будет перезаписна.

    Args:
        client (clickhouse_connect.driver.client): Клиент clickhouse
        modified (datetime): дата файла
        ftl (str): имя файла

    Returns:
        bool: ответ на: дата файла отправленного на проверку новее чем сохраненная в БД?
    """

    table_name = "config_vp"
    # проверка существования таблицы
    result = client.command(f"EXISTS {table_name}")

    if result != 1:
        datetime_object = datetime(2020, 1, 1, 0, 0, 0)
        save_file_data_ch(client, datetime_object, ftl)
    else:
        conf_dict = (
            client.query_df(f"SELECT * FROM {table_name}").set_index("index").to_dict()
        )
        if conf_dict["config"].get(ftl, None) is not None:
            if conf_dict["config"][ftl] >= modified:
                # сохранённая дата больше или = дате файла
                return False

    return True


def load_ch(
    client: clickhouse_connect.driver.client,  # type: ignore
    table_name: str,
    filter=None,
) -> pd.DataFrame:
    """Загрузить данные из clickhouse в датафрейм. Если указать фильтр, то загрузятся только данные по фильтру.
    Возвращает датафрейм с данными из clickhouse.

    Args:
        client (clickhouse_connect.driver.client): Соединение к clickhouse
        table_name (str): имя таблицы

        filter (str, optional): Строка фильтра. Defaults to None.

    Returns:
        pd.DataFrame: Считанный датафрейм с данными из clickhouse.
    """

    # проверка существования таблицы
    result = client.command(f"EXISTS {table_name}")
    if result != 1:
        print(f"The table '{table_name}' does not exist.")
        sys.exit(0)

    if filter is not None:
        res_df = client.query_df(f"SELECT * FROM {table_name} where {filter}")
    else:
        res_df = client.query_df(f"SELECT * FROM {table_name}")

    return res_df


def contc(
    dbname="default",
    hostip="192.168.203.128",
    port=8123,
    username="default",
    password="",
) -> clickhouse_connect.driver.httpclient.HttpClient:  # type: ignore
    try:
        client = clickhouse_connect.get_client(
            host=hostip,
            port=port,
            username=username,
            password=password,  # Your password, if any
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


def intoclickhouse(client, df: pd.DataFrame, table_name: str, append=False, dropc=""):
    """Записать датафрейм в clickhouse.
    Если в БД будет существовать таблица с таким имененем, она будет перезаписна.
    Нераспознанные типы, для хранения преобразуются в 'str'.

    Args:
        client (_type_): соединение к clickhouse
        df (pd.DataFrame): датафрейм для записи в clickhouse
        table_name (str): имя таблицы в clickhouse
        append(bool): признак добавления к таблице. Отключается очистка и создание схемы.
        dropc(str): колонка для очистки. Из нее выбирается минимальная и максимальная дата по входным данным и удаляется из БД перед вставкой.
    """
    if append is False:
        create_table_schema(client, df, table_name)
    elif len(dropc) > 0:
        # pd.options.display.max_rows = 130
        # print(df.dtypes)

        mind = df[dropc].min().to_pydatetime()
        maxd = df[dropc].max().to_pydatetime()

        parameters = {"v1": mind, "v2": maxd}
        client.query(
            'ALTER TABLE pandas.c1_ost DELETE WHERE "Версия2" BETWEEN %(v1)s AND %(v2)s',
            parameters=parameters,
        )

    backend_np = False
    for dtype in df.dtypes:
        if str(dtype).endswith("[pyarrow]") is not True:
            backend_np = True
            break

    if backend_np:
        df2 = df.convert_dtypes(dtype_backend="pyarrow")
        pd.set_option("display.max_rows", None)
        print("полуконвертация", df2.dtypes)
        # cols_str = df.select_dtypes(include=["object", "category"]).columns
        cols_str = df.select_dtypes(include=["category"]).columns

        df2[cols_str] = df2[cols_str].astype(pd.ArrowDtype(pa.string()))
        print("окончатальная конвертация", df2.dtypes)
        # Optional: Reset back to default settings later
        pd.reset_option("display.max_rows")
        client.insert_df_arrow(table_name, df2)
    else:
        client.insert_df_arrow(table_name, df)

    pd.set_option("display.max_rows", None)

    # print("полуконвертация", df.dtypes)
    # pd.reset_option("display.max_rows")

    # cols_str = df.select_dtypes(include=["datetime"]).columns
    # print(cols_str)
    # print(df[cols_str].dtypes)

    # mmax = pd.Timestamp.max.to_pydatetime()
    # df.loc[df[cols_str].gt(mmax).any(axis=1), cols_str] = mmax

    # df[cols_str].to_csv("out/test.csv")

    client.insert_df(table_name, df)

    print(
        f"intoclickhouse. Data from dataframe inserted into clickhouse table '{table_name}'."
    )


def create_table_schema(client, df, table_name):
    # 2. Map Pandas/Parquet dtypes to ClickHouse types (this is a simplified mapping)
    # A more robust implementation would handle nested types and specific ClickHouse types
    dtype_mapping = {
        "int64": "Nullable(Int64)",
        "int64[pyarrow]": "Nullable(Int64)",
        "float64": "Nullable(Float64)",
        "Float64": "Nullable(Float64)",
        "double[pyarrow]": "Nullable(Float64)",
        "object": "Nullable(String) CODEC(LZ4)",
        "category": "LowCardinality(String)",
        "bool": "Bool",
        "datetime64[ns]": "Nullable(DateTime64(3))",
        "timestamp[ns][pyarrow]": "Nullable(DateTime64(3))",
        "timestamp[us][pyarrow]": "Nullable(DateTime64(6))",
        "datetime64[us]": "Nullable(DateTime64(6))",
        "str": "Nullable(String) CODEC(LZ4)",
        "string[pyarrow]": "Nullable(String) CODEC(LZ4)",
        "large_string[pyarrow]": "Nullable(String) CODEC(LZ4)",
    }
    # dft = df.select_dtypes("str")
    # df[dft.columns] = dft.apply(lambda x: x.fillna(""))

    # df = df.convert_dtypes(dtype_backend="pyarrow")
    # pd.options.display.max_columns = 130
    # pd.options.display.max_rows = 130
    # print(df.dtypes)

    columns_sql = []
    for col_name, dtype in zip(df.columns, df.dtypes):
        ch_type = dtype_mapping.get(
            str(dtype), "Nullable(String) CODEC(LZ4)"
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

    # print(f"Table '{table_name}' created with schema inferred from dataframe.")


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


def load_mol_сh(
    client: clickhouse_connect.driver.client,
    clumns: dict,
    header_row: list,
    table_name: str,
    only_selected=True,
    drop_un=False,
) -> pd.DataFrame:
    """Cчитывает таблицу из clickhouse.
    Формирует наименования колонок из 2х указанных строк.
    удаляет дубликаты колонк с сохранением первой.
    Возвращает фрейм с найденными колонками

    Args:
        client (clickhouse_connect.driver.client): Клиент clickhouse
        clumns (dict): Словарь для фильтрации и переименования колонок.
        header_row (list): Список строк для формирования наименования колонок. Нумерация с 0.
        table_name (str): имя таблицы в clickhouse
        only_selected (bool, optional): Вернуть только выбранные колонки. Defaults to True.
        drop_un (bool, optional): Убрать из наименования колонок шаблон Unnamed + удалить дубликаты колонок если они получатся.

    Returns:
        pd.Dataframe: Считанный и преобразованный датафрейм
    """
    fname = "load_mol_сh"
    print(
        f"{fname}. Загрузка из Clickhouse. Таблица {table_name}. Только выбранные колонки: {only_selected}"
    )

    df_raw_c = load_ch(client, table_name)

    # копируем первые строки для поиска даты, перед трансформацией
    res2 = df_raw_c.head(header_row[0]).copy()

    # готовим наименования колонок
    # выбираем строки заголовка, транспонируем заполняем вниз дырки предыдущим значением
    summ = df_raw_c.iloc[header_row].T.ffill(axis="index")

    # суммируем колонки в серию для заголовка
    # для второй строки заголока для первых пустых значений добавляем Unnamed +индекс+ _level_1

    rs = (
        summ.iloc[:, 0]
        + "_"
        + summ.iloc[:, 1].fillna(
            "Unnamed: " + (summ.iloc[:, 1].isna().cumsum()).astype(str) + "_level_1"
        )
    )

    df_raw_c.columns = rs
    # сбросить первые строки в таблице до конца заголовка
    df_raw_c = df_raw_c.drop(df_raw_c.index[: max(header_row) + 1]).reset_index(
        drop=True
    )

    res = df_raw_c

    # логическая матрица поиска слова во фрейме
    res3 = res2.apply(lambda col: col.str.contains("Период", na=False), axis=1)
    # удалить пустые строки и колонки, взять первое значение
    res4 = (
        res2[res3]
        .dropna(axis="index", how="all")
        .dropna(axis="columns", how="all")
        .values[0][0]
    )

    # удалить пустые колонки
    # res = res.dropna(axis="columns", how="all")

    lisc = res.columns.to_list()

    print(
        f"{fname}. Список прочитанных колонок: {lisc}, \nколичество колонок: {len(lisc)}"
    )
    # список найденных имён колонок
    resl = []
    # дикт для переименования найденных колонок
    renmd = {}

    for li in clumns:
        dfit = next((x for x in lisc if x.find(li) > -1), "Not found")
        if dfit != "Not found":
            resl.append(dfit)
            renmd[dfit] = clumns[li]
        else:
            # print(f'Ошибка. Не нашел колонку "{li}" в списке колонок: {lisc} ')
            print(f'\nОшибка. Не нашел колонку "{li}" в списке колонок')
            sys.exit()

    if only_selected:
        res = res[resl]

    res.rename(
        columns=renmd,
        inplace=True,
    )
    # sys.exit(0)
    if drop_un:
        pattern = r"_[A-z:\d{1,2} ]+"
        print(f"{fname}. Очистка имени колонок по шаблону '{pattern}'")
        res = res.rename(columns=lambda x: re.sub(pattern, "", x))
        # сбросить дубликаты колонок по именам, сохранив первую
        res = res.loc[:, ~res.columns.duplicated()]

    res["Версия"] = res4
    res["Версия2"] = pd.to_datetime(str(res4).strip()[-10:], format="%d.%m.%Y")

    return res


# ----------start--------------
if __name__ == "__main__":
    print("модуль с общими процедурами")
