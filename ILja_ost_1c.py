import configparser
import re
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Optional
from numpy import nan as NP_NAN

import pandas as pd
from clickhouse_connect import driver as ch_driver
from joblib import Parallel, delayed

from xlsxwriter.worksheet import Worksheet

from shared_chouse import (
    contc,
    # save_file_data_ch,
    intoclickhouse,
    # check_file_data_ch,
    load_ch,
    # load_mol_сh,
    timer,
)

gl_writer: pd.ExcelWriter
gl_link_format = None
gl_format1 = None
gl_format0 = None
gl_wrap_format = None
gl_back_addr = {}


def save_ws(ldf: pd.DataFrame, ws: Worksheet):
    """Построчный вывод датафрейма в эксель лист. Работает более быстро для больших фреймов (более ~300 строк).

    Args:
        ldf (pd.DataFrame): Фрейм для вывода
        ws (Worksheet): Страница для вывода. Страница должна быть создана заранее.
    """
    # ldf = ldf.fillna(np.nan).replace({np.nan: None})
    ldf = ldf.fillna(NP_NAN).replace({NP_NAN: None})

    index = 1
    ws.write_row(0, 0, ldf.columns.to_list())
    for row in ldf.itertuples(name=None):
        ws.write_row(index, 0, list(row)[1:])
        index += 1


def initexcel(pathtofile: str):
    """Инициализация excel файла для записи. Настройка стилей.

    Args:
        pathtofile (str): Путь к файлу для записи

    Returns:
        pd.ExcelWriter: Excel writer.
    """
    global gl_format0, gl_format1, gl_link_format, gl_wrap_format

    # проверка и создание выходного каталога
    folder_path = Path(pathtofile).parents[0]
    folder_path.mkdir(parents=True, exist_ok=True)

    writer = pd.ExcelWriter(
        pathtofile,
        mode="w",
        engine="xlsxwriter",
        date_format="dd/mm/yyyy",
        datetime_format="dd/mm/yyyy",
        # engine_kwargs={"options": {"strings_to_urls": True}},
    )
    gl_format1 = writer.book.add_format({"num_format": "#,##0.00;-#,##0.00;-"})
    gl_format0 = writer.book.add_format({"num_format": "#,##0;-#,##0;-"})
    gl_wrap_format = writer.book.add_format({"text_wrap": True, "valign": "top"})

    gl_link_format = writer.book.get_default_url_format()
    gl_link_format.set_align("center")
    gl_link_format.set_bold()

    return writer


def hyperlink(param):
    # добавление гиперссылок назад на листы
    wsts = param["writer"].book.worksheets()
    for sheet in wsts:
        sn = sheet.get_name()
        if sn[:5] in param["префиксл"]:
            sheet.write_url(
                "A1",
                "internal:'"
                + gl_back_addr.get(sn + "_p", param["страница"])
                + "'!"
                + gl_back_addr[sn],
                string=gl_back_addr.get(sn + "_p", param["страница"]),
            )

        else:
            continue


def make_clean(ldf: pd.DataFrame) -> pd.DataFrame:

    # ldf["ЗапасМен"] = ldf["ЗапасМен"].astype("str")

    # набор колонок для перобразования в числовой тип
    to_digit = [
        "Начальный остаток_Количество",
        "Начальный остаток_Сумма (без НДС)",
        "Начальный остаток_в т.ч. сумма доп. расходов",
        "Начальный остаток_Сумма ТЗР",
        "Начальный остаток_Цена (средняя)",
        "Начальный остаток_Итого с ТЗР (без НДС)",
        "Закупка_Количество",
        "Закупка_Сумма (без НДС)",
        "Закупка_Сумма ТЗР",
        "Закупка_Цена (средняя)",
        "Закупка_Итого с ТЗР (без НДС)",
        "Приход_Количество",
        "Приход_Сумма (без НДС)",
        "Приход_в т.ч. сумма доп. расходов",
        "Освоение_Количество",
        "Освоение_Сумма (без НДС)",
        "Освоение_Сумма ТЗР",
        "Освоение_Итого с ТЗР (без НДС)",
        "Расход_Количество",
        "Расход_Сумма (без НДС)",
        "Расход_в т.ч. сумма доп. расходов",
        "Конечный остаток_Количество",
        "Конечный остаток_Сумма (без НДС)",
        "Конечный остаток_в т.ч. сумма доп. расходов",
        "Конечный остаток_Сумма ТЗР",
        "Конечный остаток_Цена (средняя)",
        "Конечный остаток_Итого с ТЗР (без НДС)",
    ]
    ldf[to_digit] = ldf[to_digit].apply(
        pd.to_numeric, downcast="integer", errors="coerce"
    )

    # ldf[to_digit] = ldf[to_digit].replace(0.0, pd.NA)

    return ldf


def transform(
    sap_ost: pd.DataFrame, c1_ost: pd.DataFrame, sap_ost_ot: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Преобразование данных для отчета

    Args:
        sap_ost (pd.DataFrame): Фрейм с остатками SAP на дату "до"
        c1_ost (pd.DataFrame): Фрейм с остатками 1с
        sap_ost_ot (pd.DataFrame): Фрейм с остатками SAP на дату "от"

    Returns:
        tuple[pd.DataFrame, pd.DataFrame]: Возвращает 2 фрейма. соединенные остатки и потерянные строки
    """
    # сбросить строки в которых не заполнен КСМ
    c1_ost = c1_ost.dropna(subset="КСМ")

    # обрезать нули слева
    c1_ost["КСМ"] = c1_ost["КСМ"].str.lstrip("0")

    # обработка пустых кодов склада в 1с
    mask = (c1_ost["Склад / Контрагент / Работник"].isna()) | (
        c1_ost["Склад / Контрагент / Работник"] == "Материалы в пути"
    )
    c1_ost.loc[mask, "Код склада SAP"] = "####"

    # сформировать ключ для слияния
    c1_ost["key"] = (
        c1_ost["Код склада SAP"] + c1_ost["КСМ"].astype("string") + c1_ost["Партия SAP"]
    )
    sap_ost["key"] = (
        sap_ost["Склад"] + sap_ost["Материал"].astype("string") + sap_ost["Партия"]
    )
    sap_ost_ot["key"] = (
        sap_ost_ot["Склад"]
        + sap_ost_ot["Материал"].astype("string")
        + sap_ost_ot["Партия"]
    )
    # сортировка и сброс дубликатов по партиям
    # sap_ost = sap_ost.sort_values(by="ДатаПервПост", ascending=False).drop_duplicates(
    sap_ost = sap_ost.sort_values(by="ПервДатПр", ascending=False).drop_duplicates(
        subset="key", keep="first"
    )
    sap_ost_ot = sap_ost_ot.sort_values(
        by="ПервДатПр", ascending=False
    ).drop_duplicates(subset="key", keep="first")

    # из начальных sap остатков удаляем строки которые есть в конечных остатках.
    # останутся строки которые были израсходованы полностью
    # сливаем в один фрейм
    sap_ost_ot = sap_ost_ot[~sap_ost_ot["key"].isin(sap_ost["key"])]

    # sap_ost.to_excel("sap_ost.xlsx", engine="xlsxwriter")

    sap_ost = pd.concat([sap_ost, sap_ost_ot], ignore_index=True)

    # print("запись временного файла sap_ost")
    # # c1_ost.to_excel("c1_ost.xlsx", index=False)
    # sap_ost.to_excel("sap_ost2.xlsx", engine="xlsxwriter")

    # подготовка таблицы с выгружаемыми запасами к слиянию
    # сортировать по дате и оставлять только первую строку для каждого ключа
    # sap_ost2["key"] = (
    #     sap_ost2["КодСклада"]
    #     + sap_ost2["Код КСМ"].astype("string")
    #     + sap_ost2["Партия"]
    # )
    # print("запись временного файла sap_ost2")
    # c1_ost.to_excel("c1_ost.xlsx", index=False)
    # sap_ost2.to_excel("sap_ost2.xlsx", index=False)

    # sap_ost2 = (
    #     sap_ost2[["key", "ДатПервПст"]]  # pyright: ignore[reportCallIssue]
    #     .sort_values(by="ДатПервПст", ascending=False)
    #     .drop_duplicates(subset="key", keep="first")
    # )

    # слияние
    c1_ost = pd.merge(
        left=c1_ost,
        right=sap_ost,
        how="left",
        left_on="key",
        right_on="key",
    )

    # слияние с выгружаемыми запасами
    # c1_ost = pd.merge(
    #     left=c1_ost,
    #     right=sap_ost2,
    #     how="left",
    #     left_on="key",
    #     right_on="key",
    # )
    # c1_ost.to_parquet("c1_ost.parquet")

    # перенос данных из выгружаемых запасов "ДатПервПст" в выгрузку 1с
    # print(c1_ost.dtypes)
    # c1_ost["ДатаПервПост"] = pd.to_datetime(c1_ost["ДатаПервПост"], errors="coerce")
    c1_ost.rename(
        columns={
            "Характеристика.Документ поступления.Дата": "ДатаПервПост_1c",
            "Документ посутпления - Дата": "ДатаПервПост_1c",
            "Обозначение": "Обознач. склада",
            "Доступное кол-во": "Количество",
        },
        inplace=True,
        errors="ignore",
    )

    c1_ost["ДатаПервПост_1c"] = pd.to_datetime(
        c1_ost["ДатаПервПост_1c"], errors="coerce", dayfirst=True
    )
    c1_ost["ДатаПервПост"] = pd.to_datetime(c1_ost["ДатаПервПост"], errors="coerce")

    # для ускорения можно выполнить после фильтрации
    # заполнить пустые даты из 1с, сбросить время в 0:00, в позиции без даты заливаем 2020-01-01
    mask = c1_ost["ДатаПервПост"].isna()
    c1_ost.loc[mask, "ДатаПервПост"] = c1_ost["ДатаПервПост_1c"]
    c1_ost["ДатаПервПост"] = c1_ost["ДатаПервПост"].dt.normalize()
    mask = c1_ost["ДатаПервПост"].isna()
    c1_ost.loc[mask, "ДатаПервПост"] = pd.to_datetime(date(2020, 1, 1))

    # ==================для тестирования
    # print("запись временного файла 268")
    # c1_ost.to_excel("c1_ost.xlsx", engine="xlsxwriter")
    # sys.exit()
    # ==

    # преобразовать типы данных
    c1_ost = make_clean(c1_ost)

    # c1_ost["Счет"] = c1_ost["Счет"].astype("str")
    # сформировать маску и всё по маске в МТР, остальное в ОНСС
    mask = c1_ost["Счет"].str.match("10") & ~c1_ost["Счет"].str.match("10.12")
    c1_ost.loc[mask, "Вид деят 1с"] = "МТР"
    c1_ost["Вид деят 1с"] = c1_ost["Вид деят 1с"].where(
        c1_ost["Вид деят 1с"] == "МТР", "ОНСС"
    )

    mask = c1_ost["Наименование подразделения"].isna()
    c1_ost.loc[mask, "Наименование подразделения"] = c1_ost[
        "Склад / Контрагент / Работник"
    ]

    c1_ost["Наименование подразделения"] = (
        c1_ost["Наименование подразделения"]
        .str.strip()
        .replace(
            {
                "МОЛ ЦАП": "ОАСУТП",
                "Отдел автоматизированных систем управления технологическим процессом": "ОАСУТП",
                "Управление метрологии, автоматизации и информационных технологий и телекоммуникаций (не использовать)": "ОАСУТП",
                "МОЛ ЦАП УМАИТ": "ОТ",
                "МОЛ ОТ": "ОТ",
                "Отдел телекоммуникаций": "ОТ",
                "ЦАП связь аварийный": "ОТ",
                "МОЛ ЦАП  ИТ": "ОИТ",
                "Отдел информационных технологий": "ОИТ",
                "Оргтехника офис": "ОИТ",
            },
            # na_action="ignore",
        )
    )

    mask = c1_ost["Наименование категории запаса"].isna()
    c1_ost.loc[mask, "Наименование категории запаса"] = (
        "Запасы под потребность текущего периода"
    )

    # заполнить пустые значения в "Наименование категории запаса" (из 1с) на "Запасы под потребность текущего периода"
    c1_ost["Наименование категории запаса"] = c1_ost[
        "Наименование категории запаса"
    ].fillna("Запасы под потребность текущего периода")

    # добавление
    date3y = pd.to_datetime(date(datetime.now().year - 3, 12, 31))
    c1_ost.loc[
        c1_ost["ДатаПервПост"] <= date3y, "Конечная сумма более 3х лет (без НДС)"
    ] = c1_ost["Конечный остаток_Сумма (без НДС)"]
    c1_ost.loc[
        c1_ost["ДатаПервПост"] <= date3y, "Начальная сумма более 3х лет (без НДС)"
    ] = c1_ost["Начальный остаток_Сумма (без НДС)"]

    param_sum = [
        "Конечный остаток_Сумма (без НДС)",
        "Конечный остаток_Итого с ТЗР (без НДС)",
        "Начальная сумма более 3х лет (без НДС)",
        "Конечная сумма более 3х лет (без НДС)",
    ]
    # в колонках суммирования заменить na на нули
    c1_ost[param_sum] = c1_ost[param_sum].fillna(0.0)

    c1_ost["Изм 3х леток"] = (
        c1_ost["Конечная сумма более 3х лет (без НДС)"]
        - c1_ost["Начальная сумма более 3х лет (без НДС)"]
    )

    # вывести результат в файл
    if __debug__:
        print("Датафрейм для записи в выходной файл:")
        c1_ost.info()

    # потеряшки это строки с остатком которых не нашли в sap
    # "Конечный остаток_Итого с ТЗР (без НДС)"
    lost = c1_ost[
        (c1_ost["Конечный остаток_Итого с ТЗР (без НДС)"] > 0.0) & (c1_ost["БЕ"].isna())
    ]

    # фильтр по "Наименование подразделения"
    podr_sap = [
        "Отдел автоматизированных систем управления технологическим процессом",
        "Отдел телекоммуникаций",
        "Отдел информационных технологий",
        "Управление метрологии, автоматизации и информационных технологий и телекоммуникаций (не использовать)",
        "ОАСУТП",
        "ОТ",
        "ОИТ",
    ]

    # выборка по фильтрам для не найденных в sap позиций
    lost_warn = lost[
        (lost["Наименование подразделения"].isin(podr_sap))
        & (lost["ДатаПервПост"].isna())
    ]

    return c1_ost, lost_warn


def load_mol_excel(
    clumns: dict, header_row: list, filename: str, only_selected=True, drop_un=False
) -> pd.DataFrame:
    """считывает xlsx файл с поиском колонок в 2х этажном заголовке таблицы
    возвращает фрейм с найденными колонками
    """

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

    # схлопнуть многоэтажный заголовок
    res.columns = ["_".join(a) for a in res.columns.to_flat_index()]  # pyright: ignore[reportAttributeAccessIssue]
    # удалить пустые колонки
    # res = res.dropna(axis="columns", how="all")

    lisc = res.columns.to_list()

    # print(
    #     f"--ОСВ список прочитанных колонок в файле {str(Path(filename).resolve())}: {lisc}, \nколичество колонок: {len(lisc)}"
    # )
    print(
        f"--ОСВ прочитанных колонок в файле {str(Path(filename).resolve())}: {len(lisc)}"
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
            print(f'\n--ОСВ Ошибка. Не нашел колонку "{li}" в списке колонок')
            sys.exit()

    if only_selected:
        res = res[resl]

    res.rename(
        columns=renmd,
        inplace=True,
    )
    if drop_un:
        pattern = r"_[A-z:\d{1,2} ]+"
        print(rf"--ОСВ удаление из имени колонок по шаблону '{pattern}'")
        res = res.rename(columns=lambda x: re.sub(pattern, "", x))
        # сбросить дубликаты колонок по именам, сохранив первую
        res = res.loc[:, ~res.columns.duplicated()]

    res["Версия"] = res4
    res["Версия2"] = pd.to_datetime(str(res4).strip()[-10:], format="%d.%m.%Y")

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
        # print(
        #     f"\nНайден самый новый файл по шаблону '{pattern}' в каталоге '{directory}':\n{str(Path(latest_file).resolve())}"
        # )
        return str(latest_file)
    except FileNotFoundError:
        print(f"Ошибка: Каталог '{directory}' не найден.")
        return None
    except Exception as e:
        print(f"Произошла непредвиденная ошибка: {e}")
        return None


def find_latest2_file(directory: str, pattern: str) -> list[str] | None:
    """Находит два последних измененных файла в каталоге по заданному шаблону.
    Первый в списке должен быть файл с суффиксом "до". Если первый заканчивается на "до", поменяет их местами.

    Args:
        directory (str): _Каталог для поиска файла_
        pattern (str): _Шаблон для поиска файла_

    Returns:
        list[str] | None: _Найденные файлы по шаблону_
    """

    try:
        dir_path = Path(directory)
        files = list(dir_path.glob(pattern))
        if not files:
            print(
                f"Ошибка: Не найдено файлов по шаблону '{pattern}' в каталоге '{directory}'."
            )
            return None

        # сортируть файлы по времени изменения и взять два последних
        latest_files = sorted(files, key=lambda p: p.stat().st_mtime)[:2]
        # первый в списке должен быть файл с суффиксом "до". Если первый заканчивается на "до", поменять их местами.
        if len(latest_files) == 2 and latest_files[0].stem[-3:-1] == "до":
            latest_files[0], latest_files[1] = latest_files[1], latest_files[0]

        # print(
        #     f"\nНайдены два последних файла по шаблону '{pattern}' в каталоге '{directory}':\n{[str(Path(f).resolve()) for f in latest_files]}"
        # )
        return [str(f) for f in latest_files]
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
            "port": "80",
        }
        with open(config_file_path, "w") as configfile:
            config.write(configfile)
    except Exception as e:
        print(f"An unexpected error occurred while reading the file: {e}")
        sys.exit(1)

    return config


def pdread_sap(filesap: str) -> tuple[pd.DataFrame, str]:
    """Для использования с joblib. Загрузка файла SAP"""

    # if filesap[-3:-1] == "до":
    if filesap.find("{от}") > -1:
        ftype = "начальный"
    elif filesap.find("{до}") > -1:
        ftype = "конечный"
    else:
        ftype = "неизвестный"

    print(f"--читаем {ftype} SAP файл: {filesap}")
    res = pd.read_excel(filesap, engine="calamine")
    print(f"--{ftype} SAP файл прочитан: {filesap}")

    return res, filesap


def pdread_c1(DATA_C1: str) -> tuple[pd.DataFrame, str]:
    """Для использования с joblib. Загрузка файла ОСВ"""
    print("--выбираем файл с остатками ОСВ---")
    mol_file = find_latest_file(DATA_C1, "*.xlsx")

    print(f"--читаем ОСВ файл: {mol_file}")

    if not mol_file:
        print("Не удалось найти необходимые файлы данных. Выход.")
        sys.exit(0)

    res = (
        load_mol_excel(
            {
                "Счет_": "Счет",
                "КСМ_": "КСМ",
                "Код склада SAP_": "Код склада SAP",
                "Партия SAP_": "Партия SAP",
            },
            [9, 10],
            mol_file,
            only_selected=False,
            drop_un=True,
        ),
        mol_file,
    )
    print(f"--ОСВ файл прочитан: {mol_file}")
    return res


def toe(mol_pd: pd.DataFrame, param: dict) -> int:
    """
    Формирование сводной таблицы и вывод в excel файл с расшифровками по уровням.

    Args:
        mol_pd (pd.DataFrame): Фрейм с данными

        param (dict): Cловарь с параметрами. формат= {
            "writer": gl_writer,
            "страница": "Суммы",
            "начстрока": downrows,
            "начколонка": 0,
            "pivot_ind": ["Наименование подразделения", "Вид деят"],
            "pivot_sum": ["Расход. Сумма (без НДС)", "Остаток (без НДС)"],
            #параметр display - перечень колонок для вывода в отчет. Если будет пустой выведет все колонки.
            "display": [
                "Наименование подразделения",
                "Вид деят",
                "Расход (шт)",
                "Расход. Сумма (без НДС)",
                "Остаток (шт)",
                "Остаток (без НДС)",
            ],
            "префиксл": "Расх",
            "tablename": "итог",  # имя добавляемой таблицы
            "linkcol": "Наименование подразделения",  # колонка для которой делать гиперссылки
        }
    Возвращает высоту выведённой таблицы
    """
    global gl_format1, gl_link_format, gl_back_addr
    # счетчик страниц для уникальности ссылок
    sh_count = 0

    # проверить что страница для вывода существует, если нет - создать
    worksheet = param["writer"].book.get_worksheet_by_name(param["страница"])
    if worksheet is None:
        worksheet = param["writer"].book.add_worksheet(param["страница"])

    # свертка по параметрам из словаря, формирование сводной
    table = pd.pivot_table(
        mol_pd,
        values=param["pivot_sum"],
        # "pivot_sum": ["Расход. Сумма (без НДС)", "Остаток (без НДС)"],
        index=param["pivot_ind"],
        # "pivot_ind": ["Наименование подразделения", "Вид деят"],
        aggfunc="sum",
    )

    # переиндексация для сохранения порядка колонок в сводной, сброс индекса для вывода в виде таблицы
    table = table.reindex(param["pivot_sum"], axis=1)
    table.reset_index(inplace=True)

    # формирование таблицы итогов
    itogt = pd.pivot_table(
        table,
        values=param["pivot_sum"],
        # index=param["pivot_ind"][0],
        index=param["pivot_ind"][:2],
        aggfunc="sum",
    ).reindex(param["pivot_sum"], axis=1)
    itogt.reset_index(inplace=True)

    itogt2 = pd.pivot_table(
        table,
        values=param["pivot_sum"],
        # index=param["pivot_ind"][0],
        index=param["pivot_ind"][1:2],
        aggfunc="sum",
    ).reindex(param["pivot_sum"], axis=1)
    itogt2.reset_index(inplace=True)

    for row in table.itertuples():
        # сборка фильтров для вывода фреймов расшифровок
        ffilter = mol_pd[param["pivot_ind"][0]] == row[1]
        for mm in range(1, len(param["pivot_ind"])):
            ffilter = ffilter & (mol_pd[param["pivot_ind"][mm]] == row[mm + 1])

        # dff = mol_pd[ffilter]
        if param.get("display", None):
            dff: pd.DataFrame = mol_pd[param["display"]]
        else:
            dff: pd.DataFrame = mol_pd[ffilter]

        sheetname = param["префиксл"] + "_" + str(sh_count)
        sh_count += 1

        # удалить пустые колонки из расшифровок
        dff = dff.dropna(axis="columns", how="all")
        dff.to_excel(
            param["writer"],
            sheet_name=sheetname,
            index=False,
        )

        # форматирование листов расшифровок отключено
        # wsl = gl_writer.book.get_worksheet_by_name(sheetname)  # type: ignore
        # wsl.set_column(3, 4, 18, gl_format1)  # type: ignore
        # wsl.autofit()  # type: ignore

        table.at[row.Index, param["pivot_ind"][0]] = (
            "=HYPERLINK(\"#'"
            + sheetname
            + '\'!A1", "'
            + str(row[1]).replace('"', '""')
            + '")'
        )
        gl_back_addr[sheetname] = chr(ord("A") + param["начколонка"]) + str(
            param["начстрока"] + row[0] + 2
        )

    namet = "Table" + param["префиксл"]

    # формирование шаблона вывода для колонок, установка итоговой функции суммирования
    # для колонки с ссылками отдельный формат синим и без функции итога

    column_settings = [
        {
            "header": column,
            "total_function": "sum",
            "format": gl_format0,
            "header_format": gl_wrap_format,
        }
        if column
        != param.get("linkcol", "--строка которая не попадется в наименованиях--")
        else {
            "header": column,
            "format": gl_link_format,
            "header_format": gl_wrap_format,
        }
        for column in table.columns
    ]

    cur_row = param["начстрока"]
    cur_col = param["начколонка"]

    worksheet.add_table(
        cur_row,
        cur_col,
        cur_row + len(table.index) + 1,
        cur_col + table.shape[1] - 1,
        {
            "data": table.values,
            "columns": column_settings,
            "style": "Table Style Medium 9",
            "name": namet,
            "total_row": True,
        },
    )

    # вывод таблицы подитога
    cur_row = cur_row + len(table.index) + 1

    param["writer"].book.get_worksheet_by_name(param["страница"]).write_string(
        cur_row + 1,
        cur_col + table.shape[1] - itogt.shape[1],
        "Итоги по отделам: ",
    )

    column_settings = [
        {
            "header": column,
            "total_function": "sum",
            "format": gl_format0,
            "header_format": gl_wrap_format,
        }
        for column in itogt.columns
    ]

    cur_row += 2
    # сдвиг левой границы на разницу в ширине таблиц
    cur_col = cur_col + table.shape[1] - itogt.shape[1]

    worksheet.add_table(
        cur_row,
        cur_col,
        cur_row + len(itogt.index) + 1,
        cur_col + itogt.shape[1] - 1,
        {
            "data": itogt.values,
            "columns": column_settings,
            "style": "Table Style Medium 9",
            "name": namet + "total",
            "total_row": True,
        },
    )

    # вывод таблицы подитога2
    cur_row = cur_row + len(itogt.index) + 2
    cur_col += 1

    param["writer"].book.get_worksheet_by_name(param["страница"]).write_string(
        cur_row,
        cur_col,
        "Итоги по виду деятельности:",
    )

    cur_row += 1

    column_settings = [
        {
            "header": column,
            "total_function": "sum",
            "format": gl_format0,
            "header_format": gl_wrap_format,
        }
        for column in itogt2.columns
    ]

    worksheet.add_table(
        cur_row,
        cur_col,
        cur_row + len(itogt2.index) + 1,
        cur_col + itogt2.shape[1] - 1,
        {
            "data": itogt2.values,
            "columns": column_settings,
            "style": "Table Style Medium 9",
            "name": namet + "total2",
            "total_row": True,
        },
    )

    return cur_row + len(itogt2.index) + 2


def report(
    dfl: pd.DataFrame,
    files: tuple,
    toch=False,
    client: Optional[ch_driver.Client] = None,
    tablename="c1_ost_filter",
):
    """Формирование выходного отчета. Запись промежуточной таблицы в Clickhouse

    Args:
        pathtofile (str): Путь к файлу для записи
        dfl (pd.DataFrame): датафрейм
        toch (bool, optional): Флаг записи в БД. Defaults to False.
        client (ch_driver.Client, optional): Клиент соединения с БД. Defaults to None.
        tablename (str, optional): Имя таблицы для записи в БД. Defaults to "c1_ost_filter".
    """
    global gl_writer

    workbook = gl_writer.book
    wssumm = workbook.add_worksheet("Суммы")  # type: ignore
    wsfilter = workbook.add_worksheet("filter")  # type: ignore
    wsbase = workbook.add_worksheet("base")  # type: ignore

    # записать в excel имеющиеся колонок
    # выключить для ускорения вывода
    # обычный вывод
    # dfl.to_excel(gl_writer, sheet_name="base", index=False)
    # оптимизированный вывод
    save_ws(dfl, wsbase)  # type: ignore

    goodlist = [
        "Счет",
        "Номенклатура / ОС",
        "КСМ",
        "Единица измерения",
        "Склад / Контрагент / Работник",
        "Начальный остаток_Количество",
        "Начальный остаток_Сумма (без НДС)",
        "Приход_Количество",
        "Приход_Сумма (без НДС)",
        "Приход_в т.ч. сумма доп. расходов",
        "Расход_Количество",
        "Расход_Сумма (без НДС)",
        "Освоение_Сумма (без НДС)",
        "Освоение_Итого с ТЗР (без НДС)",
        "Расход_в т.ч. сумма доп. расходов",
        "Конечный остаток_Количество",
        "Конечный остаток_Сумма (без НДС)",
        "Конечный остаток_в т.ч. сумма доп. расходов",
        "Конечный остаток_Сумма ТЗР",
        "Конечный остаток_Цена (средняя)",
        "Конечный остаток_Итого с ТЗР (без НДС)",
        "Версия2",
        "Материал",
        "ФИО менеджера",
        "Наименование подразделения",
        "Обознач. склада",
        "КатегЗапас",
        "Наименование категории запаса",
        "НомЗаяв",
        "Позиция",
        "Цена",
        "Количество",
        "Стоимость",
        "ПрзСрВвлЗп",
        "ДатаПервПост",
        "Вид деят 1с",
        "Начальная сумма более 3х лет (без НДС)",
        "Конечная сумма более 3х лет (без НДС)",
        "Изм 3х леток",
    ]
    dfl_s = dfl[goodlist]

    # фильтр по "Наименование подразделения"
    podr_sap = [
        "Отдел автоматизированных систем управления технологическим процессом",
        "Отдел телекоммуникаций",
        "Отдел информационных технологий",
        "Управление метрологии, автоматизации и информационных технологий и телекоммуникаций (не использовать)",
        "ОАСУТП",
        "ОТ",
        "ОИТ",
    ]

    # выборка складов МОЛ "Склад / Контрагент / Работник"
    # mol_1c = [
    #     "МОЛ ЦАП",
    #     "МОЛ ЦАП УМАИТ",
    #     "Оргтехника офис",
    # ]
    # выборка по фильтрам
    dfl_s = dfl_s[
        dfl_s["Наименование подразделения"].isin(podr_sap)
        # тк нужные данные внесены в наим. подр. фильтр по складу можно сбросить
        # | dfl_s["Склад / Контрагент / Работник"].isin(mol_1c)
    ]

    # записать в excel выборку колонок
    # dfl_s.to_excel(gl_writer, sheet_name="filter", index=False)
    save_ws(dfl_s, wsfilter)

    if toch and client is not None:
        intoclickhouse(
            client,
            dfl_s,
            tablename,
        )
        print(
            f"Report. Запись отфильтрованной таблицы:{tablename} в clickhouse выполнена."
        )
    else:
        print("Report. Запись отфильтрованной таблицы в clickhouse отключена.")

    # генерация и вывод сводной
    param_sum = [
        "Освоение_Итого с ТЗР (без НДС)",
        # "Расход_в т.ч. сумма доп. расходов",
        "Конечный остаток_Сумма (без НДС)",
        "Конечный остаток_Итого с ТЗР (без НДС)",
        "Начальная сумма более 3х лет (без НДС)",
        "Конечная сумма более 3х лет (без НДС)",
        "Изм 3х леток",
    ]

    param_ind = [
        "Наименование подразделения",
        "Вид деят 1с",
        "Склад / Контрагент / Работник",
        # "ФИО менеджера",
        "Наименование категории запаса",
    ]

    params = {
        "writer": gl_writer,
        "страница": "Суммы",
        "начстрока": 5,
        "начколонка": 0,
        "pivot_ind": param_ind,
        "pivot_sum": param_sum,
        "префиксл": "Расх",
        "linkcol": "Наименование подразделения",
    }

    listmessage = (
        f"Остатки из файла: {files[1]}",
        f"Дата прихода и распределение центральных складов из файла: {files[0]}",
        # f"Дата прихода по складам МОЛ из файла: {files[2]}",
        "Дата первой поставки взята из остатков SAP, оставшиеся пустые заполнены из остатков 1С, оставшиеся пустые заполнены константой "
        "2020-01-01"
        "",
        f"Всё что пришло {date(datetime.now().year - 3, 12, 31).strftime('%d.%m.%Y')} и раньше, считается 3х летками.",
    )

    wssumm.write_column(0, 0, listmessage)
    # автоподбор ширины колонок
    # вывод таблиц с расшифровками
    toe(dfl_s, params)
    wssumm.autofit()
    wssumm.set_column(0, 0, 14)
    wssumm.set_column(4, 9, 24)

    # расстановка обратных ссылок на листы с расшифровкой
    params["префиксл"] = ["Расх_"]
    hyperlink(params)


def start_parellel():
    global gl_writer
    global gl_client

    # load = False
    load = True

    Main_startTime = timer(name="============Запуск скрипта===============")

    # загрузить конфиг и проверить, что из него пришли переменные
    config_gl = loadinit()

    if config_gl.has_option("DEFAULT", "serverip") and config_gl.has_option(
        "DEFAULT", "port"
    ):
        serverip = config_gl.get("DEFAULT", "serverip")
        serverport = config_gl.get("DEFAULT", "port")
        print(f"serverip: {serverip}")
        print(f"serverip: {serverport}")
    else:
        print("Не найден ключ 'serverip'/'port' в секции 'DEFAULT'.")
        sys.exit()

    db_name = "db_pandas"
    gl_client = contc(db_name, hostip=serverip, port=int(serverport))

    # выгрузка sap. 2 последних файла с суффиксами {от} и {до}
    DATA_SAP = "SAP_in"
    # выгрузка 1С
    DATA_C1 = "C1_in"

    if load:
        startTime = timer(name="Начало чтения входных файлов")

        if (latest_sap_file := find_latest2_file("SAP_in", "*.xlsx")) is None or len(
            latest_sap_file
        ) != 2:
            print("Не удалось найти необходимые файлы данных. Выход.")
            sys.exit(0)
        print(f"Найденные файлы в папке {DATA_SAP}: {latest_sap_file}")

        # запуск параллельного считывания файлов excel
        tasks = [
            delayed(pdread_sap)(latest_sap_file[1]),
            delayed(pdread_c1)(DATA_C1),
            delayed(pdread_sap)(latest_sap_file[0]),
        ]
        results = Parallel(n_jobs=-1)(tasks)
        timer("Чтение завершено.", startTime)

        # слияние и поиск строк не найденных в 1С
        c1_ost, lost_warn = transform(results[0][0], results[1][0], results[2][0])  # type: ignore

        # ==================для тестирования
        # print("запись временного файла")
        # c1_ost.to_excel("c1_ost.xlsx", index=False)
        # sys.exit()
        # ==

        # ===блок вывода промежуточных файлов для отладки
        # mol_file = results[1][1]  # type: ignore
        # startTime = timer(name="Начало записи промежуточного файла")
        # filenametosave = "out/" + Path(mol_file).stem + ".xlsx"
        # tmp_writer = initexcel(filenametosave)
        # c1_ost.to_excel(
        #     tmp_writer, sheet_name="mol_file", index=False, engine="xlsxwriter"
        # )
        # c1_ost.dtypes.to_excel(
        #     tmp_writer, sheet_name="info", index=True, engine="xlsxwriter"
        # )
        # lostrows.to_excel(
        #     tmp_writer, sheet_name="lostrows", index=False, engine="xlsxwriter"
        # )
        # lostrows.dtypes.to_excel(
        #     tmp_writer, sheet_name="info_lostrows", index=True, engine="xlsxwriter"
        # )
        # sap_df: pd.DataFrame = results[0]
        # c1_df: pd.DataFrame = results[1][0]
        # sap_df.to_excel("out/sap_df.xlsx", index=True, engine="xlsxwriter")
        # c1_df.to_excel("out/c1_df.xlsx", index=True, engine="xlsxwriter")
        # tmp_writer.close()
        # timer("Завершена запись в промежуточный файл", startTime)
        # print(f"Промежуточный файл сохранен: '{str(Path(filenametosave).resolve())}'")
        # ===\блок вывода промежуточных файлов для отладки

        startTime = timer(name="Начало записи в clickhouse, таблица c1_sap_ost_flat")
        intoclickhouse(
            gl_client,
            c1_ost,
            "c1_sap_ost_flat",
            append=False,
            # dropc="Версия2",
        )
        timer("Завершена запись в clickhouse", startTime)

    print("\n===============Чтение из clickhouse====================")
    startTime = timer(name="Читаем из clickhouse, таблицу c1_ost")

    # вариант с фильтром на считывание
    # c2_df = load_ch(gl_client, "c1_sap_ost_flat", "\"Версия2\"='2026-02-28'")
    c2_df = load_ch(gl_client, "c1_sap_ost_flat")
    timer("Читаем из clickhouse, таблицу c1_sap_ost_flat", startTime)

    if __debug__:
        print("__debug__:", __debug__)
        print("====считанная таблица====")
        c2_df.info()

    repfile = "out/report.xlsx"
    gl_writer = initexcel(repfile)
    if load:
        lost_warn.to_excel(
            gl_writer, sheet_name="lost_warn", index=False, engine="xlsxwriter"
        )

    startTime = timer(name="Формирование выборки")
    # формирование выходного файла
    if load:
        gotfiles = (results[0][1], results[1][1])  # type: ignore
    else:
        gotfiles = ("Clickhouse_mode1", "Clickhouse_mode2")
    report(
        c2_df, files=gotfiles, toch=True, client=gl_client, tablename="c1_ost_filter"
    )
    timer("Формирование выборки", startTime)

    if gl_client.ping():
        gl_client.close()
        # print("\nConnection to ClickHouse closed.")

    if gl_writer is not None:
        gl_writer.close()

    print(f"Отчет сформирован в файле: '{str(Path(repfile).resolve())}'")
    timer("Итого времени выполнения скрипта", Main_startTime)
    print("Завершено.")


if __name__ == "__main__":
    print(
        "================================================================Запуск скрипта===="
    )
    start_parellel()
