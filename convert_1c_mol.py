import sys
from pathlib import Path
import pandas as pd
from shared_chouse import contc, intoclickhouse


def makeclean_mol(molfile: str):
    """Считывает файл остатков по МОЛ и возвращает фрейм"""
    global gl_writer

    mol_file = molfile

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
            "Конечный остаток_Сумма (без НДС)": "Остаток Сумма без ТЗР (без НДС)",
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
            "Остаток Сумма без ТЗР (без НДС)",
            "Остаток Сумма (без НДС)",
        ]
    ] = df[
        [
            "Освоение. Сумма (без НДС)",
            # "Расход (шт)",
            # "Приход (шт)",
            # "Расход. Сумма (без НДС)",
            "Остаток (шт)",
            "Остаток Сумма без ТЗР (без НДС)",
            "Остаток Сумма (без НДС)",
        ]
    ].apply(pd.to_numeric, errors="coerce")

    df[
        [
            "Освоение. Сумма (без НДС)",
            # "Расход. Сумма (без НДС)",
            "Остаток Сумма без ТЗР (без НДС)",
            "Остаток Сумма (без НДС)",
        ]
    ] = df[
        [
            "Освоение. Сумма (без НДС)",
            # "Расход. Сумма (без НДС)",
            "Остаток Сумма без ТЗР (без НДС)",
            "Остаток Сумма (без НДС)",
        ]
    ].multiply(0.001, axis="columns")

    return df


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
            print(f'Ошибка. Не нашел колонку "{li}" в списке колонок: {lisc} ')
            sys.exit()

    res = res[resl]
    res.rename(
        columns=renmd,
        inplace=True,
    )
    res["Версия"] = res4

    return res


if __name__ == "__main__":
    df_mol = makeclean_mol(
        r"R:\source\python\Python-xls\data\склады\2026-02-02\январь\1c-file-2026-01-счета.xlsx"
    )
    print(df_mol.dtypes)
    print(df_mol)

    # проверить что выходной каталог существует и создать его если требуется
    folder_path = Path("./out")
    folder_path.mkdir(parents=True, exist_ok=True)

    df_mol.to_excel("out/flat_1c_mol.xlsx", index=False)

    client = contc("pandas", hostip="192.168.5.17")
    intoclickhouse(client, df_mol, "flat_1c_mol")

    if client.ping():
        client.close()
