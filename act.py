import pandas as pd
from pathlib import Path
import sys
from datetime import datetime, timedelta

# from shared_module2 import loadvp3
from shared_chouse import contc, save_file_data_ch, intoclickhouse, check_file_data_ch
import configparser

# import pyarrow as pa
import re


def timer(name, startTime=None):
    """таймер"""
    if startTime:
        print(f"Таймер: Прошло времени для [{name}]: {datetime.now() - startTime}")
    else:
        startTime = datetime.now()
        print(f"Таймер: Запущен [{name}] at {startTime}")
        return startTime


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


def test3(client):
    # chtest2(client)
    # ch_dict(client)
    save_file_data_ch(
        client, datetime.today() + timedelta(days=1), r"C:\uv\chouse\act.py"
    )

    res = check_file_data_ch(
        # client, datetime.today() + timedelta(days=1), r"C:\uv\chouse\act.py"
        client,
        datetime.today() + timedelta(days=2),
        r"C:\uv\chouse\act.py",
    )
    print("проверка даты файла:", res)

    print(
        "проверка даты файла2:",
        check_file_data_ch(
            client,
            datetime.today(),
            r"C:\uv\chouse\act.py",
        ),
    )


def load_mol_excel(
    clumns: dict, header_row: list, filename: str, only_selected=True, drop_un=False
) -> pd.DataFrame:
    """считывает xlsx файл с поиском колонок в 2х этажном заголовке таблицы
    возвращает фрейм с найденными колонками
    """

    res = pd.read_excel(filename, dtype=str, header=header_row, engine="calamine")
    res2 = pd.read_excel(filename, nrows=header_row[0], engine="calamine")

    # затычка для ускорения отладки
    # res.to_parquet("C1_in/1с-2026-02-остатки.parquet")
    # res2.to_parquet("C1_in/1с-2026-02-остатки_res2.parquet")
    # res = pd.read_parquet("C1_in/1с-2026-02-остатки.parquet")
    # res2 = pd.read_parquet("C1_in/1с-2026-02-остатки_res2.parquet")

    # логическая матрица поиска слова во фрейме
    res3 = res2.apply(lambda col: col.str.contains("Период", na=False), axis=1)
    # удалить пустые строки и колонки, взять первое значение
    res4 = (
        res2[res3]
        .dropna(axis="index", how="all")
        .dropna(axis="columns", how="all")
        .values[0][0]
    )

    # схлопнуть многоэтажный заголовок
    res.columns = ["_".join(a) for a in res.columns.to_flat_index()]  # pyright: ignore[reportAttributeAccessIssue]
    # удалить пустые колонки
    res = res.dropna(axis="columns", how="all")

    lisc = res.columns.to_list()

    print(f"Список прочитанных колонок: {lisc}, \nколичество колонок: {len(lisc)}")
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
    if drop_un:
        # print("/n", res.columns.to_list())
        pattern = r"_[A-z:\d{1,2} ]+"
        print(rf"/nОчистка инени колонок по шаблону '{pattern}'")
        res = res.rename(columns=lambda x: re.sub(pattern, "", x))
        # print("/n", res.columns.to_list())

    res["Версия"] = res4

    return res


def find_latest_file(directory: str, pattern: str) -> str | None:
    """Находит самый последний измененный файл в каталоге по заданному шаблону."""
    try:
        dir_path = Path(directory)
        files = list(dir_path.glob(pattern))
        if not files:
            print(
                f"Ошибка: Не найдено файлов по шаблону '{pattern}' в каталоге '{directory}'."
            )
            return None

        latest_file = max(files, key=lambda p: p.stat().st_mtime)
        print(
            f"Найден самый новый файл по шаблону '{pattern}' в каталоге '{directory}':\n{latest_file}"
        )
        return str(latest_file)
    except FileNotFoundError:
        print(f"Ошибка: Каталог '{directory}' не найден.")
        return None
    except Exception as e:
        print(f"Произошла непредвиденная ошибка: {e}")
        return None


def loadinit() -> configparser.ConfigParser:
    config = configparser.ConfigParser()
    config_file_path = "act.ini"

    try:
        with open(config_file_path, "r") as f:
            config.read_file(f)
    except FileNotFoundError, configparser.MissingSectionHeaderError:
        config["DEFAULT"] = {
            "file1": r"R:\source\python\Python-xls\data\склады\2026-02-28\все мтр на_27.02.2026.xlsx",
            "file2": r"R:\source\python\Python-xls\data\склады\2026-02-28\Лист в ALVXXL01 (1).xlsx",
            "serverip": "192.168.5.17",
        }
        with open(config_file_path, "w") as configfile:
            config.write(configfile)
    except Exception as e:
        print(f"An unexpected error occurred while reading the file: {e}")
        sys.exit(1)

    return config


if __name__ == "__main__":
    # загрузить конфиг и проверить, что из него пришли переменные
    config_gl = loadinit()
    if (
        config_gl.has_option("DEFAULT", "file1")
        and config_gl.has_option("DEFAULT", "file2")
        and config_gl.has_option("DEFAULT", "serverip")
    ):
        file_sap1 = config_gl.get("DEFAULT", "file1")
        file_sap2 = config_gl.get("DEFAULT", "file2")
        serverip = config_gl.get("DEFAULT", "serverip")
        print(f"File1: {file_sap1}")
        print(f"file2: {file_sap2}")
        print(f"serverip: {serverip}")
    else:
        print("Не найден ключ 'file1' или 'file2' в секции 'DEFAULT'.")
        sys.exit()

    db_name = "pandas"
    client = contc(db_name, hostip=serverip)

    # каталоги для входных файлов
    DATA_SAP = "SAP_in"
    DATA_C1 = "C1_in"

    print("--выбираем файл с остатками SAP---")
    filesap = find_latest_file(DATA_SAP, "*.xlsx")
    print("--выбираем файл с остатками ОСВ---")
    mol_file = find_latest_file(DATA_C1, "*.xlsx")

    if not filesap or not mol_file:
        print("Не удалось найти необходимые файлы данных. Выход.")
        sys.exit(0)

    print(f"--читаем SAP файл:\n{filesap}")
    sap_ost = pd.read_excel(filesap, engine="calamine")

    print(f"--читаем ОСВ файл:\n{mol_file}")
    c1_ost = load_mol_excel(
        {
            "Счет_": "Счет",
            "КСМ_": "КСМ",
            "Код склада SAP_": "Код склада SAP",
            "Партия SAP_": "Партия SAP",
        },
        # [10, 11],
        [9, 10],
        mol_file,
        only_selected=False,
        drop_un=True,
    )

    # сбросить строки в которых не заполнен КСМ
    c1_ost = c1_ost.dropna(subset="КСМ")
    # обрезать нули слева
    c1_ost["КСМ"] = c1_ost["КСМ"].str.lstrip("0")

    # сформировать ключ для слияния
    c1_ost["key"] = (
        c1_ost["Код склада SAP"] + c1_ost["КСМ"].astype("string") + c1_ost["Партия SAP"]
    )
    sap_ost["key"] = (
        sap_ost["Склад"] + sap_ost["Материал"].astype("string") + sap_ost["Партия"]
    )

    # слияние
    c1_ost = c1_ost.merge(sap_ost, left_on="key", right_on="key")

    # вывести результат в файл
    startTime = timer(name="Начало записи в выходной файл")
    c1_ost.to_excel("out/c1_ost.xlsx", index=False)
    timer("Завершена запись в выходной файл", startTime)

    startTime = timer(name="Начало записи в clickhouse, таблица c1_ost")
    c1_ost.to_excel("out/c1_ost.xlsx", index=False)
    intoclickhouse(client, c1_ost, "c1_ost")
    timer("Завершена запись в clickhouse", startTime)

    if client.ping():
        client.close()
        print("\nConnection to ClickHouse closed.")
