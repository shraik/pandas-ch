import configparser
import re
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import pandas as pd
from clickhouse_connect import driver as ch_driver
from joblib import Parallel, delayed

# from xlsxwriter import workbook
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
gl_back_addr = {}


def initexcel(pathtofile: str):
    """Инициализация excel файла для записи. Настройка стилей.

    Args:
        pathtofile (str): Путь к файлу для записи

    Returns:
        pd.ExcelWriter: Excel writer.
    """
    global gl_format0, gl_format1, gl_link_format

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
    gl_format1 = writer.book.add_format({"num_format": "#,##0.00;-#,##0.00;-"})  # type: ignore
    gl_format0 = writer.book.add_format({"num_format": "#,##0;-#,##0;-"})  # type: ignore

    gl_link_format = writer.book.get_default_url_format()  # type: ignore
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

    ldf["ЗапасМен"] = ldf["ЗапасМен"].astype("str")

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
    sap_ost: pd.DataFrame, c1_ost: pd.DataFrame, sap_ost2: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Преобразование данных для отчета

    Args:
        sap_ost (pd.DataFrame): Фрейм с остатками SAP
        c1_ost (pd.DataFrame): Фрейм с остатками 1с
        sap_ost2 (pd.DataFrame): Фрейм с остатками SAP выгружаемые запасы

    Returns:
        tuple[pd.DataFrame, pd.DataFrame]: Возвращает 2 фрейма. соединенные остатки и потерянные строки
    """
    # сбросить строки в которых не заполнен КСМ
    c1_ost = c1_ost.dropna(subset="КСМ")

    # обрезать нули слева
    c1_ost["КСМ"] = c1_ost["КСМ"].str.lstrip("0")

    # добавление кода склада в выгрузку остатков 1с
    c1_ost.loc[c1_ost["Склад / Контрагент / Работник"].isna(), "Код склада SAP"] = (
        "####"
    )

    # сформировать ключ для слияния
    c1_ost["key"] = (
        c1_ost["Код склада SAP"] + c1_ost["КСМ"].astype("string") + c1_ost["Партия SAP"]
    )
    sap_ost["key"] = (
        sap_ost["Склад"] + sap_ost["Материал"].astype("string") + sap_ost["Партия"]
    )

    # подготовка таблицы с выгружаемыми запасами к слиянию
    # сортировать по дате и оставлять только первую строку для каждого ключа
    sap_ost2["key"] = (
        sap_ost2["КодСклада"]
        + sap_ost2["Код КСМ"].astype("string")
        + sap_ost2["Партия"]
    )
    sap_ost2 = (
        sap_ost2[["key", "ДатПервПст"]]  # pyright: ignore[reportCallIssue]
        .sort_values(by="ДатПервПст", ascending=False)
        .drop_duplicates(subset="key", keep="first")
    )

    # слияние
    c1_ost = pd.merge(
        left=c1_ost,
        right=sap_ost,
        how="left",
        left_on="key",
        right_on="key",
    )

    # слияние с выгружаемыми запасами
    c1_ost = pd.merge(
        left=c1_ost,
        right=sap_ost2,
        how="left",
        left_on="key",
        right_on="key",
    )
    # c1_ost.to_parquet("c1_ost.parquet")
    print("запись временного файла")
    # c1_ost.to_excel("c1_ost.xlsx", index=False)
    sap_ost.to_excel("sap_ost.xlsx", index=False)

    # перенос данных из выгружаемых запасов "ДатПервПст" в выгрузку 1с
    # print(c1_ost.dtypes)
    mask = c1_ost["ДатаПервПост"].isna()
    c1_ost.loc[mask, "ДатаПервПост"] = c1_ost["ДатПервПст"]

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
    print("Датафрейм для записи в выходной файл:")
    print(c1_ost.info())

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

    print(
        f"Список прочитанных колонок в файле {str(Path(filename).resolve())}: {lisc}, \nколичество колонок: {len(lisc)}"
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
    if drop_un:
        pattern = r"_[A-z:\d{1,2} ]+"
        print("\n", rf"Очистка имени колонок по шаблону '{pattern}'")
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
        print(
            f"Найден самый новый файл по шаблону '{pattern}' в каталоге '{directory}':\n{str(Path(latest_file).resolve())}"
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
            "port": "80",
        }
        with open(config_file_path, "w") as configfile:
            config.write(configfile)
    except Exception as e:
        print(f"An unexpected error occurred while reading the file: {e}")
        sys.exit(1)

    return config


def pdread_sap(DATA_SAP) -> pd.DataFrame:
    """Для использования с joblib. Загрузка файла SAP"""
    print("--выбираем файл с остатками SAP---")
    filesap = find_latest_file(DATA_SAP, "*.xlsx")

    print(f"--читаем SAP файл:\n{filesap}")
    return pd.read_excel(filesap, engine="calamine")


def pdread_c1(DATA_C1) -> tuple[pd.DataFrame, str]:
    """Для использования с joblib. Загрузка файла ОСВ"""
    print("--выбираем файл с остатками ОСВ---")
    mol_file = find_latest_file(DATA_C1, "*.xlsx")

    print(f"--читаем ОСВ файл:\n{mol_file}")

    if not mol_file:
        print("Не удалось найти необходимые файлы данных. Выход.")
        sys.exit(0)

    return load_mol_excel(
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
    ), mol_file


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
    # itogt
    itogt.reset_index(inplace=True)

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

        wsl = gl_writer.book.get_worksheet_by_name(sheetname)  # type: ignore
        wsl.set_column(3, 4, 18, gl_format1)  # type: ignore
        wsl.autofit()  # type: ignore

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
    # column_settings = [{"header": column} for column in table.columns]

    # формирование шаблона вывода для колонок, установка итоговой функции суммирования
    # для колонки с ссылками отдельный формат синим и без функции итога
    column_settings = [
        {"header": column, "total_function": "sum", "format": gl_format0}
        if column
        != param.get("linkcol", "--строка которая не попадется в наименованиях--")
        else {"header": column, "format": gl_link_format}
        for column in table.columns
    ]

    worksheet.add_table(
        param["начстрока"],
        param["начколонка"],
        param["начстрока"] + len(table.index) + 1,
        param["начколонка"] + table.shape[1] - 1,
        {
            "data": table.values,
            "columns": column_settings,
            "style": "Table Style Medium 9",
            "name": namet,
            "total_row": True,
        },
    )

    # вывод таблицы подитога
    param["writer"].book.get_worksheet_by_name(param["страница"]).write_string(
        param["начстрока"] + len(table.index) + 2,
        param["начколонка"] + table.shape[1] - itogt.shape[1],
        "Итоги: ",
    )

    column_settings = [
        {"header": column, "total_function": "sum", "format": gl_format0}
        for column in itogt.columns
    ]

    worksheet.add_table(
        param["начстрока"] + len(table.index) + 3,
        param["начколонка"] + table.shape[1] - itogt.shape[1],
        param["начстрока"] + len(table.index) + 3 + len(itogt.index) + 1,
        param["начколонка"] + table.shape[1] - itogt.shape[1] + itogt.shape[1] - 1,
        {
            "data": itogt.values,
            "columns": column_settings,
            "style": "Table Style Medium 9",
            "name": namet + "total",
            "total_row": True,
        },
    )
    # itogt.to_excel(
    #     param["writer"],
    #     sheet_name=param["страница"],
    #     startrow=param["начстрока"] + len(table.index) + 3,
    #     startcol=param["начколонка"] + table.shape[1] - itogt.shape[1],
    #     index=False,
    # )

    return len(table.index) + len(itogt.index) + 3


def report(
    dfl: pd.DataFrame,
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
    workbook.add_worksheet("base")  # type: ignore
    workbook.add_worksheet("filter")  # type: ignore
    workbook.add_worksheet("Суммы")  # type: ignore

    # записать в excel имеющиеся колонок
    dfl.to_excel(gl_writer, sheet_name="base", index=False)

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
    mol_1c = [
        "МОЛ ЦАП",
        "МОЛ ЦАП УМАИТ",
        "Оргтехника офис",
    ]
    # выборка по фильтрам
    dfl_s = dfl_s[
        dfl_s["Наименование подразделения"].isin(podr_sap)
        | dfl_s["Склад / Контрагент / Работник"].isin(mol_1c)
    ]

    # записать в excel выборку колонок
    dfl_s.to_excel(gl_writer, sheet_name="filter", index=False)

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

    # Освоение_Итого с ТЗР (без НДС)
    # Расход_в т.ч. сумма доп. расходов

    param_sum = [
        "Освоение_Итого с ТЗР (без НДС)",
        "Расход_в т.ч. сумма доп. расходов",
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
        "начстрока": 0,
        "начколонка": 0,
        "pivot_ind": param_ind,
        "pivot_sum": param_sum,
        "префиксл": "Расх",
        "linkcol": "Наименование подразделения",
    }

    # вывод таблиц с расшифровками
    # TODO добавить третью таблицу без разбивки по отделам

    toe(dfl_s, params)

    # автоподбор ширины колонок
    gl_writer.book.get_worksheet_by_name(params["страница"]).autofit()  # type: ignore

    # расстановка обратных ссылок на листы с расшифровкой
    params["префиксл"] = ["Расх_"]
    hyperlink(params)

    # ====добавление листа с не синхронизированными строками


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

    DATA_SAP = "SAP_in"
    # данные по месяцам
    DATA_SAP2 = "дпм"
    DATA_C1 = "C1_in"

    if load:
        startTime = timer(name="Начало чтения входных файлов")

        # запуск параллельного считывания файлов excel
        tasks = [
            delayed(pdread_sap)(DATA_SAP),
            delayed(pdread_c1)(DATA_C1),
            delayed(pdread_sap)(DATA_SAP2),
        ]
        results = Parallel(n_jobs=-1)(tasks)
        timer("Чтение завершено.", startTime)

        # слияние и поиск строк не найденных в 1С
        c1_ost, lost_warn = transform(results[0], results[1][0], results[2])  # type: ignore

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

    print("====считанная таблица====")
    c2_df.info()

    repfile = "out/report.xlsx"
    gl_writer = initexcel(repfile)
    lost_warn.to_excel(
        gl_writer, sheet_name="lost_warn", index=False, engine="xlsxwriter"
    )

    startTime = timer(name="Формирование выборки")
    # формирование выходного файла
    report(c2_df, toch=True, client=gl_client, tablename="c1_ost_filter")
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
