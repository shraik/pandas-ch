import pandas as pd
from pathlib import Path
import sys
from datetime import datetime, timedelta

# from shared_module2 import loadvp3
from shared_chouse import contc, save_file_data_ch, intoclickhouse, check_file_data_ch
import configparser


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


def load_mol_excel(clumns: dict, header_row: list, filename: str) -> pd.DataFrame:
    """считывает xlsx файл с поиском колонок в 2х этажном заголовке таблицы
    возвращает фрейм с найденными колонками
    """

    # res = pd.read_excel(filename, dtype=str, header=header_row, engine="calamine")
    # res2 = pd.read_excel(filename, nrows=header_row[0], engine="calamine")

    # res.to_parquet("C1_in/1с-2026-02-остатки.parquet")
    # res2.to_parquet("C1_in/1с-2026-02-остатки_res2.parquet")

    res = pd.read_parquet("C1_in/1с-2026-02-остатки.parquet")
    res2 = pd.read_parquet("C1_in/1с-2026-02-остатки_res2.parquet")

    # логическая матрица поиска слова во фрейме
    res3 = res2.apply(lambda col: col.str.contains("Период", na=False), axis=1)
    # удалить пустые строки и колонки, взять первое значение
    res4 = (
        res2[res3]
        .dropna(axis="index", how="all")
        .dropna(axis="columns", how="all")
        .values[0][0]
    )

    # print(res.columns.to_flat_index())

    res.columns = ["_".join(a) for a in res.columns.to_flat_index()]

    lisc = res.columns.to_list()

    print("Список прочитанных колонок: ", lisc)
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

    res = res[resl]
    res.rename(
        columns=renmd,
        inplace=True,
    )
    res["Версия"] = res4

    return res


def makeclean_mol(filename: str, allcol=False):
    """Считывает файл остатков по МОЛ и возвращает фрейм"""
    global gl_writer

    mol_file = filename

    df = load_mol_excel(
        {
            "Статья_Статья": "Статья",
            "Статья_Счет": "Вид деят",
            "Статья_Номенклатура": "Номенклатура",
            "Статья_КСМ": "КСМ",
            "Статья_Склад/Контрагент/Работник": "Наименование подразделения",
            "Освоение_Количество списание": "Освоение. списание (шт)",
            "Освоение_Количество передача в экспл.": "Освоение. передача (шт)",
            "Освоение_Количество ввод в экспл.": "Освоение. ввод (шт)",
            "Освоение_Итого (без НДС)": "Освоение. Сумма (без НДС)",
            "Конечный остаток_Количество": "Остаток (шт)",
            # "Конечный остаток_Сумма (без НДС)": "Остаток Сумма без ТЗР (без НДС)",
            "Конечный остаток_Итого с ТЗР (без НДС)": "Остаток Сумма (без НДС)",
        },
        [7, 8],
        mol_file,
    )

    df.dropna(
        subset=["Вид деят", "Номенклатура", "Наименование подразделения"], inplace=True
    )

    # df["Расход. Сумма (без НДС)"] = df["Расход. Сумма (без НДС)"].fillna(0.0)

    # mask = df["Вид деят"].str.fullmatch(r"^10.+")
    # mask = (
    #     df["Вид деят"].str.match("10") & ~df["Вид деят"].str.match("10.12")
    # )
    # df.loc[mask, "Вид деят"] = "МТР"

    mask = df["Статья"].str.match("Амортизация малоценных ОС") | df["Статья"].str.match(
        "Подготовка к вводу в эксплуатацию"
    )
    df.loc[mask, "Вид деят"] = "ОНСС"

    df["Вид деят"] = df["Вид деят"].where(df["Вид деят"] == "ОНСС", "МТР")

    # обрезка пробелов слева и справа
    df["Наименование подразделения"] = df["Наименование подразделения"].str.strip()
    df["Наименование подразделения"] = df["Наименование подразделения"].replace(
        {
            "МОЛ ЦАП": "ОАСУТП",
            "МОЛ ЦАП УМАИТ": "ОТ",
            "МОЛ ОТ ": "ОТ",
            "МОЛ ОТ": "ОТ",
            "ЦАП связь аварийный": "ОТ",
            "МОЛ ЦАП  ИТ": "ОИТ",
            "Оргтехника офис": "ОИТ",
        },
        # na_action="ignore",
    )

    df[
        [
            "Освоение. Сумма (без НДС)",
            # "Расход (шт)",
            # "Приход (шт)",
            # "Расход. Сумма (без НДС)",
            "Остаток (шт)",
            # "Остаток Сумма без ТЗР (без НДС)",
            "Остаток Сумма (без НДС)",
        ]
    ] = df[
        [
            "Освоение. Сумма (без НДС)",
            # "Расход (шт)",
            # "Приход (шт)",
            # "Расход. Сумма (без НДС)",
            "Остаток (шт)",
            # "Остаток Сумма без ТЗР (без НДС)",
            "Остаток Сумма (без НДС)",
        ]
    ].apply(pd.to_numeric, errors="coerce")

    df[
        [
            "Освоение. Сумма (без НДС)",
            # "Расход. Сумма (без НДС)",
            # "Остаток Сумма без ТЗР (без НДС)",
            "Остаток Сумма (без НДС)",
        ]
    ] = df[
        [
            "Освоение. Сумма (без НДС)",
            # "Расход. Сумма (без НДС)",
            # "Остаток Сумма без ТЗР (без НДС)",
            "Остаток Сумма (без НДС)",
        ]
    ].multiply(0.001, axis="columns")

    return df


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
        }
        with open(config_file_path, "w") as configfile:
            config.write(configfile)
    except Exception as e:
        print(f"An unexpected error occurred while reading the file: {e}")
        sys.exit(1)
    return config


if __name__ == "__main__":
    config_gl = loadinit()

    # for key in config_gl["DEFAULT"]:
    #     print(key)

    if config_gl.has_option("DEFAULT", "file1") and config_gl.has_option(
        "DEFAULT", "file2"
    ):
        file_sap1 = config_gl.get("DEFAULT", "file1")
        file_sap2 = config_gl.get("DEFAULT", "file2")
        print(f"File1: {file_sap1}")
        print(f"file2: {file_sap2}")
    else:
        print("Не найден ключ 'file1' или 'file2' в секции 'DEFAULT'.")
        sys.exit()

    db_name = "pandas"
    # client = contc()
    client = contc(db_name, hostip="192.168.5.17")

    # filets = r"\\192.168.5.199\ilja\source\python\Python-xls\data\склады\2026-02-28\1c-file-2026-02-статья2.xlsx"
    # df_in = makeclean_mol(filets)
    # df_in.to_excel(r"data\loaded_f.xlsx")

    # df1 = pd.read_excel(file_sap1, engine="calamine", nrows=10, dtype_backend="pyarrow")
    # df2 = pd.read_excel(file_sap2, engine="calamine", nrows=10, dtype_backend="pyarrow")
    # # print(df1.info())

    # print(df1.columns.sort_values().to_list())
    # print(df2.columns.sort_values().to_list())

    # df1 = pd.read_excel(file_sap1, engine="calamine", dtype_backend="pyarrow")
    # df1.to_parquet("out/все мтр на_27.02.2026.parquet")

    # df1 = pd.read_excel(file_sap2, engine="calamine", dtype_backend="pyarrow")
    # df1.to_parquet("out/Лист в ALVXXL01 (1).parquet")

    # каталоги для входных файлов
    DATA_SAP = "SAP_in"
    DATA_C1 = "C1_in"

    print("--выбираем файл с остатками SAP---")
    filesap = find_latest_file(DATA_SAP, "*.parquet")
    print("--выбираем файл с остатками ОСВ---")
    mol_file = find_latest_file(DATA_C1, "*.xlsx")

    if not filesap or not mol_file:
        print("Не удалось найти необходимые файлы данных. Выход.")
        sys.exit(1)

    print(f"--читаем SAP файл:\n{filesap}")
    sap_ost = pd.read_parquet(filesap, dtype_backend="pyarrow")

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
    )

    c1_ost = c1_ost.dropna(subset="КСМ")
    # c1_ost["КСМ"] = c1_ost["КСМ"].astype("int32")
    c1_ost["КСМ"] = c1_ost["КСМ"].str.lstrip("0")
    c1_ost["key"] = (
        c1_ost["Код склада SAP"]
        + c1_ost["КСМ"].astype("string[pyarrow]")
        + c1_ost["Партия SAP"]
    )

    # pd.StringDtype(storage="pyarrow")
    sap_ost["Материал"] = (
        sap_ost["Материал"]
        .astype("string[pyarrow]")
        .convert_dtypes(dtype_backend="pyarrow")
    )

    print(sap_ost.info())
    sap_ost["key"] = sap_ost["Склад"] + sap_ost["Материал"] + sap_ost["Партия"]

    c1_ost = c1_ost.merge(sap_ost, left_on="key", right_on="key")

    c1_ost.to_excel("out/c1_ost.xlsx", index=False)

    # df1 = pd.read_parquet("out/все мтр на_27.02.2026.parquet", dtype_backend="pyarrow")
    # df2 = pd.read_parquet("out/Лист в ALVXXL01 (1).parquet", dtype_backend="pyarrow")

    # print(df1.info())
    # print(df2.info())

if client.ping():
    client.close()
    print("\nConnection to ClickHouse closed.")
