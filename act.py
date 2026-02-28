import pandas as pd

import sys
from datetime import datetime, timedelta

from shared_module2 import loadvp3
from shared_chouse import contc, save_file_data_ch, intoclickhouse, check_file_data_ch


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

    # res = pd.read_excel(filename, header=header_row, engine="calamine")
    res = pd.read_excel(filename, dtype=str, header=header_row, engine="calamine")
    res2 = pd.read_excel(filename, nrows=header_row[0], engine="calamine")

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


if __name__ == "__main__":
    db_name = "pandas"
    # client = contc()
    client = contc(db_name, hostip="192.168.5.17")

    # filets = r"\\192.168.5.199\ilja\source\python\Python-xls\data\склады\2026-02-28\1c-file-2026-02-статья2.xlsx"
    # df_in = makeclean_mol(filets)
    # df_in.to_excel(r"data\loaded_f.xlsx")

    file_sap1 = r"\\192.168.5.199\ilja\source\python\Python-xls\data\склады\2026-02-28\все мтр на_27.02.2026.xlsx"
    df1 = pd.read_excel(file_sap1, engine="calamine", nrows=10, dtype_backend="pyarrow")

    file_sap2 = r"\\192.168.5.199\ilja\source\python\Python-xls\data\склады\2026-02-28\Лист в ALVXXL01 (1).xlsx"
    df1 = pd.read_excel(file_sap1, engine="calamine", nrows=10, dtype_backend="pyarrow")
    df2 = pd.read_excel(file_sap2, engine="calamine", nrows=10, dtype_backend="pyarrow")
    print(df1.info())

    print(df1.columns.sort_values().to_list())
    print(df2.columns.sort_values().to_list())

    df1 = pd.read_excel(file_sap1, engine="calamine", nrows=10)


if client.ping():
    client.close()
    print("\nConnection to ClickHouse closed.")
