# pyinstaller --onefile --noconfirm --upx-dir C:/python/upx/upx-5.1.1-win64 --hidden-import=numpy.core.multiarray --hidden-import babel.numbers .\3year_v33.py
# pyinstaller --onefile --noconfirm --hidden-import=numpy.core.multiarray --hidden-import babel.numbers .\3year_v33.py
# C:/python/upx/upx-5.1.1-win64
# --onefile
from datetime import date, datetime, timedelta

# from dateutil.relativedelta import relativedelta
from pathlib import Path, PureWindowsPath
import tkinter as tk
from tkinter import filedialog as fd
from tkcalendar import DateEntry
import pandas as pd
import configparser
from math import ceil
from sys import version as pyversion
from importlib.metadata import version
import os
import sys

from shared_module import loadsettings3, timer

try:
    from mp import gl_factfile as gl_factfile_mp  # type: ignore
    from mp import gl_settings as gl_settings_mp  # type: ignore

    print(
        f"применен monkey patch import\ngl_factfile_mp={gl_factfile_mp}\ngl_settings_mp={gl_settings_mp}"
    )
except ModuleNotFoundError:
    print("Не найден mp.py")


left_file = r"Y:\ilja\source\python\Python-xls\data\склады\Январь\Выгрузка МТР на 31.01.2024.pkl"
right_file = (
    r"Y:\ilja\source\python\Python-xls\data\склады\март\Выгрузка МТР на 31.03.2024.pkl"
)
mol_file = r"Y:\ilja\source\python\Python-xls\data\склады\март\02.04.2024.xlsx"
molsap_file = ""
molsap_file3 = ""
factf_folder = ""
gl_conf_list = ""
gl_prpath = (
    r"R:\03. ЗГД_ГИ\07. УМАИТТ\06. Общая\!БП\отчеты вп\настройки\Приоритетные КСМ.xlsx"
)

# ost_1c_fname = r"R:\03. ЗГД_ГИ\07. УМАИТТ\06. Общая\!Склады\2025\ЦС\Август\план-Б\1с\запас-1с-2025-08-18.xlsx"
ost_1c_fname = r"R:\source\python\Python-xls\data\склады\Июль\запас-1с-2025-08-18.xlsx"


# mol_file = r"\\rosneft.ru\SNK\DiskR\03. ЗГД_ГИ\07. УМАИТТ\06. Общая\!Склады\2024\Склады МОЛ\февраль\12.03.24.xlsx"
# left_file = r"\\rosneft.ru\SNK\DiskR\03. ЗГД_ГИ\07. УМАИТТ\06. Общая\!Склады\2023\ЦС\Ноябрь\Выгрузка МТР на 01.12.2023.pkl"
# right_file = r"\\rosneft.ru\SNK\DiskR\03. ЗГД_ГИ\07. УМАИТТ\06. Общая\!Склады\2024\ЦС\Март\Выгрузка МТР на 31.03.2024.pkl"

config = configparser.ConfigParser()
gl_format1 = None
gl_format0 = None
gl_link_format = None
gl_back_addr = {}
df_molsap = pd.DataFrame()
gl_df_cmtr: pd.DataFrame

# признак запуска из DAGa
gl_dagmode = False


def saveconf(vn: str, val: str):
    global config

    fname = os.path.basename(__file__)
    conffile = PureWindowsPath(fname).with_suffix(".ini")

    defc = config["default"]
    defc[vn] = str(val)

    with open(conffile, "w") as configfile:
        config.write(configfile)


def open_file_d(num: int, gedit: tk.Text):
    """Открытие диалогов и выбор файлов
    1- левый файл
    2- правый файл
    3- остатки по мол-1c
    4- остатки по мол-sap
    5- каталог для поиска факта поставки text_factf
    6- выбор файлов настроек
    7- выбор файла с выгрузкой счетов
    8- остатки по мол-sap3
    """
    ind = gedit.get("1.0", tk.END).strip()
    if num == 5:
        filepath = fd.askdirectory(initialdir=PureWindowsPath(ind))
    elif num == 6:
        filepath = fd.askopenfilenames(initialdir=PureWindowsPath(ind))
    else:
        if str(PureWindowsPath(ind)) != ".":
            filepath = fd.askopenfilename(initialdir=PureWindowsPath(ind).parents[0])
        else:
            filepath = fd.askopenfilename()

    # R:\source\python\Python-xls\data\факт\помесячно
    if filepath != "":
        if type(filepath) is not tuple:
            filepath = PureWindowsPath(filepath)
        else:
            filepath = list(filepath)

        match num:
            case 8 | 7 | 5 | 4 | 3 | 2 | 1:
                gedit.delete("1.0", tk.END)
                gedit.insert("1.0", filepath)
                saveconf(gedit._name, filepath)

            case 6:
                gedit.delete("1.0", tk.END)
                gedit.insert(tk.END, "[")
                for x in filepath:
                    gedit.insert(
                        tk.END,
                        "'" + x + "', ",
                    )
                gedit.delete("end-2c")
                gedit.delete("end-2c")
                gedit.insert(tk.END, "]")
                saveconf("gedit._name", filepath)
            case _:
                print("Ошибка вызова open_file")
                return 0

    return 0


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

    res.columns = ["_".join(a) for a in res.columns.to_flat_index()]  # pyright: ignore[reportAttributeAccessIssue]

    lisc = res.columns.to_list()

    print(
        f"Список прочитанных колонок из файла {str(Path(filename).resolve())}: ", lisc
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
            print(f'Ошибка. Не нашел колонку "{li}" в списке колонок: {lisc} ')
            sys.exit()

    res = res[resl]
    res.rename(
        columns=renmd,
        inplace=True,
    )
    res["Версия"] = res4

    return res


def makeclean_mol():
    """Считывает файл остатков по МОЛ и возвращает фрейм"""
    global gl_writer

    mol_file = text_mol.get("1.0", tk.END).strip()

    # df = load_mol_excel(
    #     {
    #         "Счет_": "Вид деят",
    #         "Номенклатура_": "Номенклатура",
    #         "Склад/Контрагент/Работник_": "Наименование подразделения",
    #         "Расход_Количество": "Расход (шт)",
    #         "Расход_Сумма (без НДС)": "Расход. Сумма (без НДС)",
    #         "Конечный остаток_Количество": "Остаток (шт)",
    #         "Конечный остаток_Сумма (без НДС)": "Остаток Сумма без ТЗР (без НДС)",
    #         "Конечный остаток_Итого с ТЗР (без НДС)": "Остаток Сумма (без НДС)",
    #     },
    #     [7, 8],
    #     mol_file,
    # )

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


def makeclean(df: pd.DataFrame, ost_c1: pd.DataFrame, mode=""):
    """процедура очистки данных, возвращает очищенный фрейм"""

    # удалить лишние строки
    if mode == "":
        df.drop(df[df["ВидКоммОбм"] == "E"].index, inplace=True)

    df.drop(df[(df["Количество"] == 0) & (df["Стоимость"] == 0)].index, inplace=True)

    # переименование под общий шаблон, ошибки в игнор
    df.rename(
        columns={
            "ДатаПервПост": "ДатПервПст",
            "Наим вида деят": "Наименование вида деятельности",
            "ЕИ": "БЕИ",
            "Учетная цена": "УчетнЦена",
        },
        inplace=True,
        errors="ignore",
    )

    # Заполнить менеджера
    mask = (df["Наименование подразделения"] == "Отдел информационных технологий") & (
        df["ФИО менеджера"].isnull()
    )
    df.loc[mask, "ФИО менеджера"] = "Менеджер УМАИТиТ ОИТ"

    mask = (df["Наименование подразделения"] == "Отдел телекоммуникаций") & (
        df["ФИО менеджера"].isnull()
    )
    df.loc[mask, "ФИО менеджера"] = "Менеджер УМАИТиТ ОТ"

    mask = (
        df["Наименование подразделения"]
        == "Отдел автоматизированных систем управления технологическим процессом"
    ) & (df["ФИО менеджера"].isnull())
    df.loc[mask, "ФИО менеджера"] = "Менеджер УМАИТиТ ОАСУТП"

    # распил старого запаса
    mask = (
        (
            df["Наименование подразделения"]
            == "Управление метрологии, автоматизации и информационных технологий и телекоммуникаций (не использовать)"
        )
        # & (df["ФИО менеджера"].isnull())
        & (df["Склад"] == "Оргтехника офис")
    )
    df.loc[mask, "ФИО менеджера"] = "Менеджер УМАИТиТ ОИТ"
    df.loc[mask, "Наименование подразделения"] = "Отдел информационных технологий"

    mask = (df["Наименование подразделения"].str.contains("не использовать")) & (
        df["Склад"] == "МОЛ ЦАП"
    )
    df.loc[mask, "ФИО менеджера"] = "Менеджер УМАИТиТ ОАСУТП"
    df.loc[mask, "Наименование подразделения"] = (
        "Отдел автоматизированных систем управления технологическим процессом"
    )

    mask = (df["Наименование подразделения"].str.contains("не использовать")) & (
        df["Склад"] == "МОЛ ЦАП УМАИТ"
    )
    df.loc[mask, "ФИО менеджера"] = "Менеджер УМАИТиТ ОТ"
    df.loc[mask, "Наименование подразделения"] = "Отдел телекоммуникаций"

    # сброс остатков по складу МОЛ на подразделение владелец
    mask = df["Склад"] == "МОЛ ЦАП УМАИТ"
    df.loc[mask, "Наименование подразделения"] = "Отдел телекоммуникаций"

    # переименование колонок под шаблон
    if "Наименование категории запаса" in df.columns:
        df.rename(
            columns={"Наименование категории запаса": "НаимКатегорииЗапаса"},
            inplace=True,
        )

    if "Вид деят" not in df.columns:
        print("Колонка 'Вид деят' не найдена, пробую использовать 'ВидДеят'")
        if "ВидДеят" in df.columns:
            df.rename(
                columns={"ВидДеят": "Вид деят"},
                inplace=True,
            )
            print("Переименовали 'ВидДеят' -> 'Вид деят'")

    #     if "Вид МТР" in df.columns:
    #         df.rename(
    #             columns={"Вид МТР": "Вид деят"},
    #             inplace=True,
    #         )
    #     print("Колонка 'Вид МТР' не найдена, пробую использовать 'ВидДеят'")

    df["Наименование подразделения"] = df["Наименование подразделения"].replace(
        {
            "Отдел информационных технологий": "ОИТ",
            "Отдел телекоммуникаций": "ОТ",
            "Отдел автоматизированных систем управления технологическим процессом": "ОАСУТП",
            # "Управление метрологии, автоматизации и информационных технологий и телекоммуникаций (не использовать)": "ОАСУТП",
            "Сектор метрологии": "ОАСУТП",
            # "Отдел автоматизации, метрологии и связи (не использовать)": "ОАСУТП",
        },
        # na_action="ignore",
    )

    # рекласс в МТР по совокупности параметра
    # pd.options.display.width = 0
    if "БухКодСчет" in df.columns:
        # with pd.option_context("display.max_rows", None, "display.max_columns", None):
        #     print(df.dtypes)
        bks_list = [100500000, 1005010000]

        mask1 = df["Вид деят"] == "Q"
        mask2 = df["БухКодСчет"].isin(bks_list)
        msk = pd.concat((mask1, mask2), axis=1)
        slct = msk.all(axis=1)
        df.loc[slct, "Вид деят"] = "МТР"

    df["Вид деят"] = df["Вид деят"].replace(
        {
            "O": "МТР",
            "M": "ОНСС",
            "Q": "ОНСС",
            "F": "МТР",
        },
        # na_action="ignore",
    )

    vallst = [
        "ОИТ",
        "ОТ",
        "ОАСУТП",
    ]

    # сортировка по количеству в партиях
    df.sort_values(["Партия", "Количество"], ascending=[True, False], inplace=True)

    if mode == "ost":
        fiom = [
            "Зарезервировано под продажу ДО",
            "Невостребованные ликвидные МТР (НВЛ)",
        ]
        # для стыка с 1с
        df["КодСклада"] = df["Склад"]
        if "Обозначение склада" in df.columns:
            df["Склад"] = df["Обозначение склада"]
        elif "Обознач. склада" in df.columns:
            df["Склад"] = df["Обознач. склада"]
        else:
            print("Не нашел колонки 'Обознач. склада' или 'Обозначение склада' ")

        if "Цена" in df.columns:
            df["УчетнЦена"] = df["Цена"]

        df["sap"] = (
            df["Материал"].astype(str)
            + " / "
            + df["Партия"].astype(str)
            + " / "
            + df["Склад"].astype(str)
            + " / "
            + df["КатегЗапас"].astype(str)
        )
    else:
        fiom = [
            "Менеджер УМАИТиТ ОАСУТП",
            "Менеджер УМАИТиТ ОИТ",
            "Менеджер УМАИТиТ ОТ",
            "Свободные запасы МТР",
            "Аварийный запас",
        ]
        df["sap"] = (
            df["Код КСМ"].astype(str)
            + " / "
            + df["Партия"].astype(str)
            + " / "
            + df["Склад"].astype(str)
            + " / "
            + df["КатегЗапас"].astype(str)
        )
        df["cc"] = df.groupby(["sap"]).cumcount()
    df = df[
        (df["Наименование подразделения"].isin(vallst))
        & (df["ФИО менеджера"].isin(fiom))
        # & (df["ПервДатПр"].dt.year <= 2021)
    ].copy()

    df[["НомЗаяв", "Позиция"]] = df[["НомЗаяв", "Позиция"]].apply(
        pd.to_numeric, downcast="integer", errors="coerce"
    )

    df["НомЗаяв"] = df["НомЗаяв"].fillna(0.0).astype("Int64")
    df["Позиция"] = df["Позиция"].fillna(0.0).astype("Int64")
    df["Перв"] = df["НомЗаяв"].astype("str") + " / " + df["Позиция"].astype("str")

    droplist = [
        "Доступное кол-во",
        "Зарезерв",
        "Гарантийный срок",
        "СрХрн/МСГ",
        "Объект КС",
        "Обозначение",
        "КлОцн",
        "НомЗаяв",
        "Позиция",
        "Уникальный идентификатор запаса",
        "Код инвест. проекта",
        "СтрПроисх",
        "СтрПроисх",
        "ОпрЛист",
        "СтрПроисх.1",
        "КодСППЭлем",
        "Наименование",
        "СПП-элемента 1ур",
        "КодСППЭлем.1",
        "Наименование СПП-элемента ПКВ",
        "Краткий текст",
        "ГрпВидДеят",
        "Наименование СПП-элемента 1урНаим груп. вида деят",
        "Статус сог",
        "РасСтатуса",
        "Создал",
        "Время",
        "ВремяАктлз",
        "Комментарии к корректировке",
        "ВидКоммОбм",
        "ДатаПерем",
        "ДатаРезерв",
        "Приход",
        "КодСППЭлем",
        "Наименован",
        "КодСППЭлем.1",
        "Наименование СПП-элемента 1ур",
        "Наименован.1",
        "КраткТекст",
        "ГрпВидДеят",
        "Наим груп. вида деят",
        "Статус сог",
        "РасСтатуса",
        "Время",
        "ВремяАктлз",
        # "Уникальный ИД запаса",
        "КодИнвПр",
        "Влт",
        "ДатаАктуал",
        "ДатаОткреп",
        "ГарантСрок",
        "Назначенный срок хранения",
    ]
    availcolset = set(df.columns.to_list())
    candel = [x for x in droplist if x in availcolset]

    df.drop(
        labels=candel,
        axis="columns",
        inplace=True,
    )

    # выборка колонок с суммой для деления из разных вариантов
    availcolset = set(df.columns.to_list())
    print(availcolset)

    st_list = ["УчетнЦена", "Стоимость", "ЦРеалБ/НДС", "СтоимРеал"]
    canmul = [x for x in st_list if x in availcolset]
    df[canmul] = df[canmul].multiply(0.001, axis="columns")

    # модификация счетов на основе считанных из 1с
    if mode == "ost":
        df["key"] = df["КодСклада"] + df["Материал"].astype("str") + df["Партия"]
    else:
        df["key"] = df["КодСклада"] + df["Код КСМ"].astype("str") + df["Партия"]

    # добавление номера счета из выгрузки 1с
    df = df.merge(
        ost_c1[["Счет", "key"]],
        how="left",
        left_on="key",
        right_on="key",
    )
    # .drop(
    #     labels="key",
    #     axis="columns",
    # )

    df["Счет"] = df["Счет"].astype("str")
    mask = (
        df["Счет"].str.match("10") & ~df["Счет"].str.match("10.12")
        # 10.08.2 относим к ОНСС
        # & ~df_right["Счет"].str.match("10.08.2")
    )
    df.loc[mask, "Вид деят 1с"] = "МТР"
    df["Вид деят 1с"] = df["Вид деят 1с"].where(df["Вид деят 1с"] == "МТР", "ОНСС")

    # df.loc[df["Счет"] == "nan", "Вид деят 1с"] = "nan"
    df.loc[df["Счет"].isna(), "Вид деят 1с"] = "nan"
    # print(df["Счет"])

    df.loc[df["Вид деят 1с"] != "nan", "Вид деят"] = df["Вид деят 1с"]

    # df_right.rename(
    #     columns={"Вид деят": "Вид деят-SAP"},
    #     inplace=True,
    # )

    # df_right.rename(
    #     columns={"Вид деят 1с": "Вид деят"},
    #     inplace=True,
    # )

    """
    # суммирование одинаковых партий
    coltoa = df.columns.to_list()
    removel = [
        "ПрзСрВвлЗп",
        # "КатегЗапас",
        # "НаимКатегорииЗапаса",
        "Код менедж",
        # "ФИО менеджера",
        "Количество",
        "Стоимость",
    ]
    coltoa = [x for x in coltoa if x not in removel]
    df = df.groupby(by=coltoa, as_index=False, dropna=False).agg(
        {"Количество": "sum", "Стоимость": "sum"}
    )
    """

    return df


def toe(mol_pd: pd.DataFrame, param: dict) -> int:
    """
    формирование сводной таблицы и вывод в excel файл с расшифровками по уровням.
    Перечень параметров:
    params = {
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
        # dff = mol_pd[
        #     (mol_pd[param["pivot_ind"][0]] == row[1])
        #     & (mol_pd[param["pivot_ind"][1]] == row[2])
        # ]

        ffilter = mol_pd[param["pivot_ind"][0]] == row[1]
        for mm in range(1, len(param["pivot_ind"])):
            ffilter = ffilter & (mol_pd[param["pivot_ind"][mm]] == row[mm + 1])

        # dff = mol_pd[ffilter]
        if param.get("display", None):
            dff = mol_pd[param["display"]]
        else:
            dff = mol_pd[ffilter]

        # sheetname = param["префиксл"] + "_" + str(row[1]) + "_" + str(row[2])
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


def conv_to_pkl(editor, fname) -> pd.DataFrame:
    """Загрузка файла .xlsx"""
    df_l = pd.read_excel(editor.get("1.0", tk.END).strip(), engine="calamine")
    print(f"Файл прочитан {PureWindowsPath(editor.get('1.0', tk.END).strip()).name}")
    # filepath = PureWindowsPath(editor.get("1.0", tk.END).strip()).with_suffix(".pkl")
    # print(f"Преобразован и записан в {PureWindowsPath(filepath).name}")
    # df_l.to_pickle(filepath)
    # editor.delete("1.0", tk.END)
    # editor.insert("1.0", filepath)
    # saveconf(fname, str(filepath))
    return df_l


def mfunc1(inpt: pd.Series, l_today) -> pd.DataFrame:
    """вычисление и добавление на фрейм плана по списанию"""
    if True:
        # if inpt["НаимКатегорииЗапаса"] not in {
        #     "Аварийные запасы МТР",
        #     "Запасы под потребность других ОГ",
        #     "Невостребованные ликвидные МТР (НВЛ)",
        # }:
        m1 = inpt["m1"]
        # l_today = pd.to_datetime("today")

        raspr = inpt["Количество"]
        # dsp = pd.to_datetime("today").strftime("%Y-%m")
        dsp = l_today.strftime("%Y-%m")
        mon = 0

        if inpt["ДатПервПст"] > l_today:
            mnth_shift = inpt["ДатПервПст"]
        else:
            mnth_shift = l_today

        # если разовое распределение, то всё кол-во откидываем на след месяц
        if inpt.get("spv") == "разовое":
            cum = raspr
            dsp = "м_" + (mnth_shift + pd.offsets.DateOffset(months=mon + 1)).strftime(
                "%Y_%m"
            )
            inpt[dsp] = cum * inpt["УчетнЦена"]
            inpt[dsp + "_шт"] = cum

        else:
            while raspr > 0:
                if m1 > 0:
                    cum = ceil(raspr / (m1 - mon))
                else:
                    cum = raspr
                dsp = "м_" + (mnth_shift + pd.offsets.DateOffset(months=mon)).strftime(
                    "%Y_%m"
                )
                inpt[dsp] = cum * inpt["УчетнЦена"]
                inpt[dsp + "_шт"] = cum

                raspr -= cum
                mon += 1

    return inpt


def out_planspis(df_lin: pd.DataFrame) -> bool:
    """вывод плана по списанию"""
    goodcol = [
        "Код КСМ",
        "Наименование КСМ",
        "Склад",
        "НаимКатегорииЗапаса",
        "Вид деят",
        "Наименование подразделения",
        "ФИО менеджера",
        "БЕИ",
        "Количество",
        "УчетнЦена",
        "Стоимость",
        "ДатОстатка",
        "ДатПервПст",
    ]
    df_l = df_lin[goodcol].copy()

    df_l["КрайДат"] = df_l["ДатПервПст"] + pd.offsets.DateOffset(years=1)
    df_l["ТекДат"] = pd.to_datetime("today").date()

    df_l["m1"] = df_l["КрайДат"].dt.to_period("M").astype("int64") - pd.to_datetime(
        ["today"]
    ).to_period("M").astype("int64")

    cols_to_move_l = df_l.columns.tolist()
    ll_today = pd.to_datetime("today")
    df_l = df_l.apply(mfunc1, args=(ll_today,), axis=1)
    df_l = df_l[cols_to_move_l + [x for x in df_l.columns if x not in cols_to_move_l]]

    df_l.to_excel(gl_writer, sheet_name="План_до_года", index=False)
    return True


def monkey_path2():
    """Исправление путей в настройках для локального запуска"""
    global gl_factfile, gl_settings
    print("Запуск Monkey path")

    if "gl_factfile_mp" in globals():
        gl_factfile = gl_factfile_mp
        print("Применен Monkey path gl_factfile")
    else:
        print("Ошибка применения Monkey path gl_factfile_mp не найден")

    if "gl_settings_mp" in globals():
        if gl_settings_mp.get("классификатор"):
            gl_settings["классификатор"] = gl_settings_mp["классификатор"]
        if gl_settings_mp.get("путь"):
            gl_settings["путь"] = gl_settings_mp["путь"]
        if gl_settings_mp.get("корректировки"):
            gl_settings["корректировки"] = gl_settings_mp["корректировки"]
        print("Применен Monkey path gl_settings")
    else:
        print("Ошибка применения Monkey path gl_settings_mp не найден")


def out_sbor(df_lin: pd.DataFrame) -> bool:
    """формирование сборной таблицы из фактического остатка и плана прихода
    df_lin - фрейм с остатками на складах
    """

    goodcol = [
        "Перв",
        "Код КСМ",
        "Наименование КСМ",
        "Склад",
        "Класс",
        "НаимКатегорииЗапаса",
        "Вид деят",
        "Наименование подразделения",
        "ФИО менеджера",
        "БЕИ",
        "Количество",
        "УчетнЦена",
        "Стоимость",
        "ДатОстатка",
        "ДатПервПст",
    ]
    df_l = df_lin[goodcol].copy()

    # добавление к остаткам на складе информации по планированию
    df_l = df_l.merge(
        gl_filtersdf[["sap", "bgt", "otdel", "vidtmc", "spv"]],
        how="left",
        left_on="Перв",
        right_on="sap",
    ).drop(
        labels="sap",
        axis="columns",
    )

    # формирование списка заявок для построения плана поставки
    # Загрузка кэша ВП заявок с полем "срок"
    patho = Path(gl_settings["путь"], "вп_corr.prqt")
    if not patho.is_file():
        print(f"Нет базы данных {patho}, необходимо запустить 'refresh'")
        sys.exit(0)
    print(f"Загрузка кэша возвратного плана из {str(patho.resolve())}")
    dfb = pd.read_parquet(patho)

    # выборка из ВП заявок упомянутых в настройках с датой поставки больше начала текущего года
    # сбросить заявки у которых стоит статус поставлено, если их нет в остатке значит "съели"
    values = gl_filtersdf[gl_filtersdf["item"] == "заявка"]["sap"].tolist()  # noqa: F841
    start_date = date(int(date.today().year), 1, 1)  # noqa: F841
    dfb = dfb.query(
        "`/ САП`==@values & `СРОК`>=@start_date & `Статус`!='13. Поставлено'"
    )

    # сбросить заявки перв номера которых уже есть в остатке
    # TODO добавить сброс заявок упомянутых в факте поставки
    dfb = dfb[~dfb["/ САП"].isin(df_l["Перв"])]

    # накатить корректировки
    # dfb = load_cor_file(dfb, gl_settings, False)

    # убрать НДС из заявок загруженных с ВП, пересчитать в тыс руб
    dfb["Сумма"] = dfb["Сумма"] / 1.22 / 1000

    # удаление заявок из плана найденных в факте поставки
    # df_fact = loadfact()

    # скорректировать вид деятельности в ВП по плану из заявок или по суммам
    # dfb = cor_data(dfb, gl_filters, gl_settings)

    # заполнение данных в ВП для слияния с остатками
    dfb["Вид деятельности"] = dfb["Вид деятельности"].replace(
        {
            "Основная деятельность": "МТР",
            "Оборудование не входящее в смету строек": "ОНСС",
        },
    )

    dfb["Склад"] = "В_закупке"
    dfb["НаимКатегорииЗапаса"] = "Запасы под потребность текущего периода"
    dfb["УчетнЦена"] = dfb["Сумма"] / dfb["Кол-во в ЕИ"]

    coltd = [
        "Наименование службы заказчика",
        "sap",
        "Номер НПП",
        "Позиция НПП",
        # "Сокр оператор",
        "Оператор закупки",
        "Доп. данные о материале",
        "Дата Утверждения  НПП",
        "Номер дог. документа",
        "Договор 1С",
        # "Полное имя материала",
        "Номер лота из отчета по лотам",
    ]
    dfb = (
        dfb.merge(
            gl_filtersdf[["sap", "bgt", "otdel", "vidtmc", "spv"]],
            how="left",
            left_on="/ САП",
            right_on="sap",
        )
        .drop(
            labels=coltd,
            axis="columns",
        )
        .rename(
            columns={
                "Сумма": "Стоимость",
                "СРОК": "ДатПервПст",
                "Вид деят_x": "Вид деят",
                "otdel": "Наименование подразделения",
                "/ САП": "Перв",
                "Материал": "Код КСМ",
                "ЕИ ввода": "БЕИ",
                "Кол-во в ЕИ": "Количество",
                "Наименование факт": "Наименование КСМ",
                "Вид деятельности": "Вид деят",
            },
        )
    )

    dfb["Наименование подразделения"] = dfb["Наименование подразделения"].replace(
        {
            "ИТ": "ОИТ",
            "АСУТП": "ОАСУТП",
        },
    )

    df_res = pd.concat([df_l, dfb])

    # замена содержимого в колонке по нахождению слова
    df_res.loc[
        df_res["vidtmc"].str.contains("расходники", na=False, case=False), "vidtmc"
    ] = "расходники"

    df_res["bgt"] = df_res["bgt"].fillna("не_задан")

    # dfb.to_excel(gl_writer, sheet_name="Сборная_вп", index=False)
    # df_l.to_excel(gl_writer, sheet_name="Сборная", index=False)
    df_res.to_excel(gl_writer, sheet_name="Сборная_итог", index=False)

    df_plan = out_planspis2(df_res, "Сборная_план2")

    df_plan = out_planspis2(df_res, "Сборная_план3", True)

    col1 = df_res.columns.to_list() + [
        "КрайДат",
        "ДатПервПст_ориг",
        "ТекДат",
        "m1",
        "dd_corr",
        "базис",
        "level",
        "стратегия_вовл",
        "Стоимость_бн",
        "УчетнЦена_бн",
    ]
    col2 = df_plan.columns.to_list()

    cold = [x for x in col2 if x not in col1 and x[-3:] != "_шт"]

    # ---вывод сводной по фактическим остаткам
    table2 = pd.pivot_table(
        df_plan[~df_plan["ДатОстатка"].isna()],
        values=cold,
        index=[
            "Наименование подразделения",
            "Вид деят",
            "bgt",
            "НаимКатегорииЗапаса",
            "Склад",
        ],
        aggfunc="sum",
    ).reset_index()

    params = {
        "writer": gl_writer,
        "страница": "Сводная_план2",
        "начстрока": 1,
        "начколонка": 1,
        "pivot_ind": [
            "Наименование подразделения",
            "Вид деят",
            "bgt",
            "НаимКатегорииЗапаса",
            "Склад",
        ],
        "pivot_sum": cold,
        "префиксл": "СписС",
        "режим": "суммы",  # режим расшифровки - суммы
        "table": True,  # режим добавить таблицы
        "tablename": "факт",  # имя добавляемой таблицы
    }

    table2[cold] = table2[cold].astype(int).astype("string")
    wsl = gl_writer.book.get_worksheet_by_name(params["страница"])

    wsl.write_string(0, 1, "Вывод плана списания по фактическим остаткам")
    endline = df_toexcel_desc(table2, df_plan[~df_plan["ДатОстатка"].isna()], params)

    # отладочная выгрузка. удалить
    # df_plan[df_plan["ДатОстатка"].isna()].to_excel(
    #     gl_writer, sheet_name="Сводная_план2", index=False
    # )

    # ---вывод сводной по ВП
    table2 = pd.pivot_table(
        df_plan[df_plan["ДатОстатка"].isna()],
        values=cold,
        index=[
            "Наименование подразделения",
            "Вид деят",
            "bgt",
            "НаимКатегорииЗапаса",
            "Склад",
        ],
        aggfunc="sum",
    ).reset_index()

    # предыдущие параметры с перезаписанными значениями
    params["начстрока"] = endline + 3
    params["префиксл"] = "СписП"

    table2[cold] = table2[cold].astype(int).astype("string")

    wsl.write_string(endline + 2, 1, "Вывод плана списания по плану поставки")
    params["tablename"] = "ВП"
    endline = df_toexcel_desc(table2, df_plan[df_plan["ДатОстатка"].isna()], params)

    # ---вывод суммарной
    table2 = pd.pivot_table(
        df_plan,
        values=cold,
        index=[
            "Наименование подразделения",
            "Вид деят",
            "bgt",
            "НаимКатегорииЗапаса",
            "Склад",
        ],
        aggfunc="sum",
    ).reset_index()

    # предыдущие параметры с перезаписанными значениями
    params["начстрока"] = endline + 3
    params["префиксл"] = "СписТ"

    table2[cold] = table2[cold].astype(int).astype("string")
    wsl.write_string(
        endline + 2, 1, "Вывод суммарного плана списания остатки + план поставки"
    )
    params["tablename"] = "факт_и_ВП"
    df_toexcel_desc(table2, df_plan, params)

    # форматирование страницы
    fcol = params["начколонка"] + len(params["pivot_ind"])
    lcol = params["начколонка"] + table2.shape[1]
    wsl.set_column(fcol, lcol, 10, gl_format0)
    wsl.autofit()

    return 0


def out_planspis2(df_lin: pd.DataFrame, sheet_name: str, endyear=False) -> pd.DataFrame:
    """вывод плана по списанию с расчетом на 1 год
    df_lin - входной фрейм
    sheet_name - имя листа для вывода
    """
    goodcol = [
        "Перв",
        "Код КСМ",
        "Наименование КСМ",
        "Полное имя материала",
        "Склад",
        "Класс",
        "НаимКатегорииЗапаса",
        "Вид деят",
        "Наименование подразделения",
        "ФИО менеджера",
        "БЕИ",
        "Количество",
        "УчетнЦена",
        "Стоимость",
        "ДатОстатка",
        "ДатПервПст",
        "spv",
        "vidtmc",
        "bgt",
    ]
    # print(df_lin.columns)
    df_l = df_lin[goodcol].copy()
    df_l["Код КСМ"] = df_l["Код КСМ"].astype("string")

    # Добавление НДС!
    df_l = df_l.rename(
        columns={"Стоимость": "Стоимость_бн", "УчетнЦена": "УчетнЦена_бн"},
    )

    df_l["Стоимость"] = df_l["Стоимость_бн"] * 1.22
    df_l["УчетнЦена"] = df_l["УчетнЦена_бн"] * 1.22

    # TODO удалить
    # print("1158-9, Вывод в файл Сборная_план2.parquet\n")
    # df_l.to_parquet("Сборная_план2.parquet")

    df_l["ДатПервПст_ориг"] = df_l["ДатПервПст"]

    d1_y = pd.to_datetime(date(datetime.now().year, 1, 1))
    mask = df_l["ДатПервПст"] < d1_y
    df_l.loc[mask, "ДатПервПст"] = d1_y

    if endyear:
        df_l["КрайДат"] = pd.to_datetime(date(datetime.today().year, 12, 31))
    else:
        df_l["КрайДат"] = df_l["ДатПервПст"] + pd.offsets.DateOffset(years=1)

    df_l["ТекДат"] = pd.to_datetime("today").date()

    # загрузить перечень исключений, поставить по перечню "КрайДат" = 30.07.тек_года

    if gl_prpath is not None:
        df_prioritet = pd.read_excel(gl_prpath, engine="calamine")
        mask = df_l["Наименование КСМ"].isin(df_prioritet["Наименование КСМ"])

        df_l.loc[mask, "КрайДат"] = pd.to_datetime(
            df_l.loc[mask, "ДатПервПст"].dt.year.astype("str") + "-07-30",
            format="%Y-%m-%d",
        )

    df_l["m1"] = df_l["КрайДат"].dt.to_period("M").astype("int64") - pd.to_datetime(
        ["today"]
    ).to_period("M").astype("int64")

    mask = df_l["m1"] > 12
    df_l.loc[mask, "m1"] = df_l.loc[mask, "КрайДат"].dt.to_period("M").astype(
        "int64"
    ) - df_l.loc[mask, "ДатПервПст"].dt.to_period("M").astype("int64")

    cols_to_move_l = df_l.columns.tolist()
    ll_today = pd.to_datetime("today")
    df_l = df_l.apply(mfunc1, args=(ll_today,), axis=1)
    df_l = df_l[cols_to_move_l + [x for x in df_l.columns if x not in cols_to_move_l]]

    # добавка Алексея ---

    df_l["стратегия_вовл"] = df_l["Склад"]
    df_l["стратегия_вовл"] = (
        df_l["стратегия_вовл"].map(
            {
                "МОЛ ОТ": "мин",
                "МОЛ ЦАП УМАИТ": "мин",
                "ЦАП связь аварийный": "мин",
                "Оргтехника офис": "мин",
                "МОЛ ЦАП ИТ": "мин",
                "МОЛ ЦАП": "мин",
                "ПлощадкаЦПС БСкл": "сред",
                "К-219 БазовыйСкл": "сред",
                "К-10 Базовый скл": "сред",
                "Офис ПередНаУИС": "сред",
            }
        )
        # .fillna("макс")
    )

    # df_l["стратегия_вовл"].fillna("макс", inplace=True)
    df_l["стратегия_вовл"] = df_l["стратегия_вовл"].fillna("макс")

    # переместить колонки влево таблицы
    lcl = [
        "стратегия_вовл",
        "Перв",
    ]
    columns = [x for x in df_l.columns if x not in lcl]
    new_order = lcl + columns
    df_l = df_l[new_order]

    # выборка в отдельный фрейм заявок из настроек
    zai = gl_filtersdf[gl_filtersdf["item"] == "заявка"]

    # слияние на базис
    df_l = pd.merge(
        left=df_l,
        right=zai[["sap", "базис"]],
        left_on="Перв",
        right_on="sap",
        how="left",
    ).drop("sap", axis="columns")  # сбросить лишнюю колонку после слияния

    # слияние класс - уровень
    df_l = pd.merge(
        left=df_l,
        right=gl_df_cmtr[["class", "level"]],
        left_on="Класс",
        right_on="class",
        how="left",
    ).drop("class", axis="columns")  # сбросить лишнюю колонку после слияния

    # преобразование уровня
    df_l["level"] = df_l["level"].astype("str")
    condition = (df_l["level"] == "1") | (df_l["level"] == "2")
    df_l["level"] = df_l["level"].mask(condition, "Р-21")
    df_l["level"] = df_l["level"].where(condition, "Прочие")

    if sheet_name != "":
        df_l.to_excel(gl_writer, sheet_name=sheet_name, index=False)

    return df_l


def df_toexcel_desc(table: pd.DataFrame, raw_data: pd.DataFrame, param: dict) -> int:
    """Вывод фрейма на страницу с выводом листов расшифровок.
    Возвращает номер строки после вывода фрейма
    table - фрейм для расшифровки
    raw_data - фрейм для фильтрации
    param - словарь с параметрами
    params = {
        "writer": gl_writer,
        "страница": "Суммы_изм2",
        "начстрока": 3,
        "начколонка": 1,
        "pivot_ind": [
            "Отдел",
            "Вид деят",
            "Склад",
            "НаимКатегорииЗапаса",
        ],
        "префиксл": "CИзм_",
        "tablename": "итог",  # имя добавляемой таблицы
        "linkcol": "Наименование подразделения",  # колонка для которой делать гиперссылки
    }

    """
    global gl_back_addr
    endline = param["начстрока"]
    # счетчик страниц для уникальности
    sh_count = 0

    # поискать query by dict of values

    for row in table.itertuples():
        ffilter = raw_data[param["pivot_ind"][0]] == row[1]
        for mm in range(1, len(param["pivot_ind"])):
            ffilter = ffilter & (raw_data[param["pivot_ind"][mm]] == row[mm + 1])

        if param.get("режим", None) == "суммы":
            for dopf in range(0, len(param["pivot_sum"])):
                colnametf = param["pivot_sum"][dopf]
                idx_col = table.columns.get_loc(colnametf) + 1
                valueintbl = row[idx_col]
                if float(valueintbl) != 0.0:
                    ffilter2 = ffilter & (~raw_data[colnametf].isnull())
                    dff = raw_data[ffilter2]
                    sheetname = param["префиксл"] + "_" + str(sh_count)
                    sh_count += 1

                    # срезать лишние колонки
                    gcol = dff.columns.tolist()
                    badcl = ["Стоимость_бн"]
                    rcol = [
                        x
                        for x in gcol
                        if x not in param["pivot_sum"]
                        and x[-3:] != "_шт"
                        and x not in badcl
                    ]
                    rcol.append(colnametf)
                    rcol.append(colnametf + "_шт")

                    dff[rcol].to_excel(
                        param["writer"],
                        sheet_name=sheetname,
                        index=False,
                    )

                    # wsl = gl_writer.book.get_worksheet_by_name(sheetname)
                    # wsl.set_column(3, 4, 18, gl_format1)
                    # wsl.autofit()

                    table.at[row.Index, colnametf] = (
                        "=HYPERLINK(\"#'"
                        + sheetname
                        + "'!A1\","
                        + str(valueintbl).replace('"', '""')
                        + ")"
                    )
                    gl_back_addr[sheetname] = chr(
                        ord("A") + param["начколонка"] + idx_col - 1
                    ) + str(param["начстрока"] + row[0] + 2)
                    gl_back_addr[sheetname + "_p"] = param["страница"]

        else:
            dff = raw_data[ffilter]

            sheetname = param["префиксл"] + "_" + str(sh_count)
            sh_count += 1
            dff.to_excel(
                param["writer"],
                sheet_name=sheetname,
                index=False,
            )

            # wsl = gl_writer.book.get_worksheet_by_name(sheetname)
            # wsl.set_column(3, 4, 18, gl_format1)
            # wsl.autofit()

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
            gl_back_addr[sheetname + "_p"] = param["страница"]

    wsl = param["writer"].book.get_worksheet_by_name(param["страница"])

    column_settings = [
        {"header": column, "total_function": "sum", "format": gl_format0}
        if column
        != param.get("linkcol", "--строка которая не попадется в наименованиях--")
        else {"header": column, "format": gl_link_format}
        for column in table.columns
    ]

    wsl.add_table(
        param["начстрока"],
        param["начколонка"],
        param["начстрока"] + len(table.index) + 1,
        param["начколонка"] + table.shape[1] - 1,
        {
            "data": table.values,
            "columns": column_settings,
            "style": "Table Style Medium 9",
            # "name": sheetname,
            "name": param["tablename"],
            "total_row": True,
        },
    )

    endline += len(table.index)
    if param.get("table", None):
        endline += 1

    return endline


def load_osv():
    """загрузка из остатков 1с"""
    global ost_1c_fname
    ost_1c_fname = text_ost_1c_fname.get("1.0", tk.END).strip()

    print("--читаем ОСВ файл:")
    df = load_mol_excel(
        {
            "Счет_": "Счет",
            "Номенклатура / ОС_": "Номенклатура",
            "КСМ_": "КСМ",
            "Код склада SAP_": "Код склада SAP",
            "Партия SAP_": "Партия SAP",
            "Конечный остаток_Количество": "Коност",
        },
        # [10, 11],
        [9, 10],
        ost_1c_fname,
    )
    df["Коност"] = df["Коност"].astype("Float64")
    # df = df.dropna(subset="Коност")

    df.fillna({"Код склада SAP": "####"}, inplace=True)
    # df["КСМ"] = df["КСМ"].astype("int32")
    df["КСМ"] = df["КСМ"].str.lstrip("0")
    df["key"] = df["Код склада SAP"] + df["КСМ"].astype("str") + df["Партия SAP"]
    df = df.sort_values(by=["key", "Коност"], na_position="last").drop_duplicates(
        subset=["key"], keep="first"
    )

    df.to_excel(gl_writer, sheet_name="Счета_1с", index=False)

    return df


def start():
    global \
        gl_writer, \
        gl_format0, \
        gl_format1, \
        gl_link_format, \
        gl_format1_bold, \
        gl_settings, \
        gl_filters, \
        gl_filtersdf, \
        gl_df_cmtr

    print("=======Запуск=======")
    startTime = timer(name="Таймер запущен.")
    # проверка наличия выходного каталога и создание выходного файла
    if not os.path.isdir(PureWindowsPath("./склад")):
        os.mkdir(PureWindowsPath("./склад"))

    cmonth = datetime.today().strftime("%Y-%m")
    path = (
        str(PureWindowsPath("./склад"))
        + "/"
        + "_pandas_склад_v3-ндс-"
        + cmonth
        + ".xlsx"
    )

    gl_writer = pd.ExcelWriter(
        path,
        mode="w",
        engine="xlsxwriter",
        date_format="dd/mm/yyyy",
        datetime_format="dd/mm/yyyy",
        # engine_kwargs={"options": {"strings_to_urls": True}},
    )

    # загрузка фалов настроек
    # loadlist = [
    #     "R:/03. ЗГД_ГИ/07. УМАИТТ/06. Общая/!БП/отчеты вп/настройки/ИТ_настройки2.xlsx",
    #     "R:/03. ЗГД_ГИ/07. УМАИТТ/06. Общая/!БП/отчеты вп/настройки/ОТ_настройки2.xlsx",
    #     "R:/03. ЗГД_ГИ/07. УМАИТТ/06. Общая/!БП/отчеты вп/настройки/АСУТП_настройки2.xlsx",
    # ]

    sstr = text_conflist.get("1.0", tk.END)
    loadlist = list(eval(sstr))

    gl_settings, gl_filters, gl_filtersdf = loadsettings3(
        loadlist, gl_dagmode, defcolstoload=False
    )
    monkey_path2()

    # загрузка справочника классов МТР
    my_db = Path(Path(gl_settings["классификатор"]).parent, "classmtr.pkl")
    # my_db = Path(r"R:\source\python\Python-xls\data\настройки", "classmtr.pkl")

    if my_db.is_file():
        gl_df_cmtr = pd.read_pickle(my_db)
    else:
        print(f"Нет базы данных классов МТР= {my_db}")
        sys.exit(-1)

    gl_format1 = gl_writer.book.add_format({"num_format": "#,##0.00;-#,##0.00;-"})
    gl_format0 = gl_writer.book.add_format({"num_format": "#,##0;-#,##0;-"})
    gl_format1_bold = gl_writer.book.add_format(
        {"num_format": "#,##0.00;-#,##0.00;-", "bold": True}
    )

    gl_link_format = gl_writer.book.get_default_url_format()
    gl_link_format.set_align("center")
    gl_link_format.set_bold()

    workbook = gl_writer.book
    workbook.add_worksheet("Суммы")
    workbook.add_worksheet("Суммы_изм")
    workbook.add_worksheet("Карта_устаревания")
    workbook.add_worksheet("Под_продажу_SAP")
    workbook.add_worksheet("Под_продажу_SAP2")
    workbook.add_worksheet("План_до_года")
    # workbook.add_worksheet("Стало_план2")
    # workbook.add_worksheet("Сборная")
    # workbook.add_worksheet("Сборная_вп")
    workbook.add_worksheet("Счета_1с")
    workbook.add_worksheet("Сборная_итог")
    workbook.add_worksheet("Сборная_план2")
    workbook.add_worksheet("Сборная_план3")
    workbook.add_worksheet("Сводная_план2")
    # workbook.add_worksheet("l2")
    # workbook.add_worksheet("l3")
    # workbook.add_worksheet("l4")
    # workbook.add_worksheet("left")
    # workbook.add_worksheet("right1")
    workbook.add_worksheet("right")
    workbook.add_worksheet("Лог загрузки")

    df_mol = makeclean_mol()

    downrows = 3

    params = {
        "writer": gl_writer,
        "страница": "Суммы",
        "начстрока": downrows,
        "начколонка": 0,
        "pivot_ind": ["Наименование подразделения", "Вид деят"],
        "pivot_sum": [
            # "Расход. Сумма (без НДС)",
            "Освоение. Сумма (без НДС)",
            "Остаток Сумма без ТЗР (без НДС)",
            "Остаток Сумма (без НДС)",
        ],
        "префиксл": "Расх",
        "linkcol": "Наименование подразделения",
    }
    df_mol.to_excel(gl_writer, sheet_name="mol_плоский", index=False)

    htable = toe(df_mol, params)

    downrows_sm = 3 + htable + 5

    ws = gl_writer.book.get_worksheet_by_name("Суммы")
    cell_format = workbook.add_format({"align": "left", "text_wrap": True})
    ws.set_row(downrows - 1, 29)  # type: ignore

    ws.merge_range(  # type: ignore
        downrows - 1,
        1,
        downrows - 1,
        3,
        "Сводная таблица по сумме расхода/остатка на складе МОЛ за месяц из 1С:ERP. По типу и отделам из файла: "
        + text_mol.get("1.0", tk.END).strip(),
        cell_format,
    )

    # формирование листов по 3х летним запасам
    print("Обработка файла ", left_editor2.get("1.0", tk.END).strip())
    if PureWindowsPath(left_editor2.get("1.0", tk.END).strip()).suffix == ".xlsx":
        df_left = conv_to_pkl(left_editor2, "left_file")
    # else:
    #     df_left = pd.read_pickle(left_editor2.get("1.0", tk.END).strip())

    # считать остатки из выгрузки 1с
    c1_ost = load_osv()

    df_it = makeclean(df_left, c1_ost)
    # print("Вывод тестового файла1")
    # df_it.to_excel("косяки/df_it.xlsx")

    print("Обработка файла ", right_editor2.get("1.0", tk.END).strip())
    if PureWindowsPath(right_editor2.get("1.0", tk.END).strip()).suffix == ".xlsx":
        df_right = conv_to_pkl(right_editor2, "right_file")
    # else:
    #     df_right = pd.read_pickle(right_editor2.get("1.0", tk.END).strip())

    df_it2 = makeclean(df_right, c1_ost)
    # print("Вывод тестового файла1")
    # df_it2.to_excel("косяки/df_it2.xlsx")

    print("Обработка файла ", ms_editor2.get("1.0", tk.END).strip())
    if PureWindowsPath(ms_editor2.get("1.0", tk.END).strip()).suffix == ".xlsx":
        df_rez = conv_to_pkl(ms_editor2, "molsap_file")
    # else:
    #     df_rez = pd.read_pickle(ms_editor2.get("1.0", tk.END).strip())

    print(df_rez.columns)
    df_it3 = makeclean(df_rez, c1_ost, "ost")
    print("\ndf_it3", df_it3.columns)
    # df_it_1_3 = makeclean(df_rez, c1_ost, "ost")

    goodcolset = set(df_it2.columns.to_list())
    collist = set(df_it3.columns.to_list())
    candel = [x for x in collist if x not in goodcolset]

    print("candel", candel)
    df_it3.to_excel(gl_writer, sheet_name="Под_продажу_SAP", index=False)
    df_it3.drop(
        labels=candel,
        axis="columns",
        inplace=True,
    )

    df_it3.to_excel(gl_writer, sheet_name="Под_продажу_SAP2", index=False)
    if len(df_it3.index) > 0:
        df_it22 = pd.concat([df_it2, df_it3])
    else:
        df_it22 = df_it2

    # заполнение пустых ячеек в выгрузке OSV sap
    df_it22["Код КСМ"] = df_it22["Код КСМ"].astype("string")
    df_it22["Материал"] = df_it22["Материал"].astype("string")

    mask = df_it22["Наименование КСМ"].isna()
    df_it22.loc[mask, "Наименование КСМ"] = df_it22.loc[mask, "Краткий текст материала"]
    df_it22.loc[mask, "Код КСМ"] = df_it22.loc[mask, "Материал"]

    df_it22.to_excel(gl_writer, sheet_name="right", index=False)

    # out_planspis(df_it2)
    # out_sbor(df_it2)
    out_planspis(df_it22)
    out_sbor(df_it22)

    downrows = 3
    # y3 = datetime.now().year - 3
    # datef = pd.to_datetime(datef.replace(month=datef.month, day=1) - timedelta(days=1))
    y3 = date_right3.get_date().year

    # вывод сумм по остаткам на лист Суммы
    ws = gl_writer.book.get_worksheet_by_name("Суммы")

    ws.set_row(downrows_sm - 1, 58)  # type: ignore
    ws.merge_range(  # type: ignore
        downrows_sm - 1,
        1,
        downrows_sm - 1,
        4,
        "Сумма отстатков по типу запасов из SAP. Из файла: "
        + str(
            PureWindowsPath(right_editor2.get("1.0", tk.END).strip()).with_suffix(
                ".xlsx"
            )
        )
        + "\n + выборка строк по правилу 'ФИО менеджера'='Зарезервировано под продажу ДО' или 'Невостребованные ликвидные МТР (НВЛ)' из :\n"
        + str(PureWindowsPath(ms_editor2.get("1.0", tk.END).strip())),
        cell_format,
    )

    params = {
        "writer": gl_writer,
        "страница": "Суммы",
        "начстрока": downrows_sm,
        "начколонка": 0,
        "pivot_ind": [
            "Наименование подразделения",
            "Вид деят",
            "Склад",
            "НаимКатегорииЗапаса",
        ],
        "pivot_sum": ["Стоимость"],
        "префиксл": "ОстЦ",
        "linkcol": "Наименование подразделения",
    }
    htable = toe(df_it22, params)
    downrows_sm += htable + 5

    # вывод сумм по остаткам ранее 3х лет на лист Суммы
    ws.write_string(  # type: ignore
        downrows_sm - 1,
        1,
        f"Сумма 3х летних, по типу запасов пришедших в {str(y3)} году и ранее. Из файла:"
        + str(
            PureWindowsPath(right_editor2.get("1.0", tk.END).strip()).with_suffix(
                ".xlsx"
            )
        ),
    )

    # df_right_f = df_it2[df_it2["ДатПервПст"].dt.year <= y3]
    df_right_f = df_it22[df_it22["ДатПервПст"].dt.year <= y3]

    params = {
        "writer": gl_writer,
        "страница": "Суммы",
        "начстрока": downrows_sm,
        "начколонка": 0,
        "pivot_ind": [
            "Наименование подразделения",
            "Вид деят",
            "Склад",
            "НаимКатегорииЗапаса",
        ],
        "pivot_sum": ["Стоимость"],
        "префиксл": "OcЦ3",
        "linkcol": "Наименование подразделения",
    }
    htable = toe(df_right_f, params)
    downrows_sm += htable + 5

    # вывод сумм по остаткам ранее 3х лет (до месяца) на лист Суммы
    ws.write_string(  # type: ignore
        downrows_sm - 1,
        1,
        "Сумма по типу запасов пришедших "
        + str(date_right3.get_date())
        + " и ранее. Из файла: "
        + str(
            PureWindowsPath(right_editor2.get("1.0", tk.END).strip()).with_suffix(
                ".xlsx"
            )
        ),
    )

    # выборка 3х леток или старше из правого файла
    # df_right_f = df_it2[
    #     df_it2["ДатПервПст"] <= pd.to_datetime(date_right3.get_date())
    # ].copy()
    df_right_f = df_it22[
        df_it22["ДатПервПст"] <= pd.to_datetime(date_right3.get_date())
    ].copy()

    params = {
        "writer": gl_writer,
        "страница": "Суммы",
        "начстрока": downrows_sm,
        "начколонка": 0,
        "pivot_ind": [
            "Наименование подразделения",
            "Вид деят",
            "Склад",
            "НаимКатегорииЗапаса",
        ],
        "pivot_sum": ["Стоимость"],
        "префиксл": "OcЦ3m",
        "linkcol": "Наименование подразделения",
    }
    htable = toe(df_right_f, params)

    # ----вывод листа с картой устаревания
    df_right_fm = df_it22.copy()

    # всё пришедшее ранее 3х лет приравнимаем к 3 годам
    dtm = pd.to_datetime(date_right3.get_date())
    mask = df_right_fm["ДатПервПст"] <= dtm
    df_right_fm.loc[mask, "ДатПервПст"] = dtm

    df_right_fm["ДатПервПст"] = df_right_fm["ДатПервПст"].dt.strftime("%Y-%m")
    pivot_ind = [
        "Наименование подразделения",
        "Вид деят",
        "Склад",
        "НаимКатегорииЗапаса",
    ]
    df_fsv = pd.pivot_table(
        df_right_fm,
        index=pivot_ind,
        columns="ДатПервПст",
        values="Стоимость",
        aggfunc="sum",
    ).reset_index()

    df_fsv.to_excel(
        gl_writer,
        sheet_name="Карта_устаревания",
        index=False,
    )

    gl_writer.book.get_worksheet_by_name("Карта_устаревания").set_column(  # type: ignore
        4, df_fsv.shape[1], 11, gl_format1
    )
    gl_writer.book.get_worksheet_by_name("Карта_устаревания").autofit()  # type: ignore

    df_right_fm = None
    df_fsv = None
    # ---\вывод листа с картой устаревания

    params["префиксл"] = ["OcЦ3m", "OcЦ3_", "ОстЦ_", "Расх_", "СписП", "СписС", "СписТ"]
    hyperlink(params)

    # ---формирование дельты по запасам
    # df_right_f = df_it2.copy()
    df_right_f = df_it22.copy()
    df_left_f = df_it.copy()

    # добавить в левый фрейм НВЛ и НЛ
    df_mol3 = conv_to_pkl(ms_editor3, "molsap_file")
    df_it33 = makeclean(df_mol3, c1_ost, "ost")
    df_it33.drop(
        labels=candel,
        axis="columns",
        inplace=True,
    )

    if len(df_it33.index) > 0:
        df_left_f = pd.concat([df_left_f, df_it33])

    # добавление колонки с расчетом устаревания
    edate = pd.to_datetime(date_right3.get_date())
    sdate = datetime(edate.year, edate.month, 1)
    mask = df_right_f["ДатПервПст"].between(sdate, edate)
    df_right_f.loc[mask, "Постарело"] = df_right_f.loc[mask, "Стоимость"]

    # выделение сумм 3х летних и ранее запасов в правом и левом файле
    mask = df_right_f["ДатПервПст"] <= pd.to_datetime(date_right3.get_date())
    df_right_f.loc[mask, "Стоим_стало_3y"] = df_right_f.loc[mask, "Стоимость"]

    mask = df_left_f["ДатПервПст"] <= pd.to_datetime(date_left31.get_date())
    df_left_f.loc[mask, "Стоим_было_3y"] = df_left_f.loc[mask, "Стоимость"]

    # выделение сумм прошлогодних и ранее запасов в правом и левом файле
    lasty = datetime(date_year.get_date().year - 1, 12, 31)
    # mask = df_right_f["ДатПервПст"] <= lasty
    mask = df_right_f["ДатПервПст"] <= lasty
    df_right_f.loc[mask, "Стоим_стало_1y"] = df_right_f.loc[mask, "Стоимость"]

    mask = df_left_f["ДатПервПст"] <= lasty
    df_left_f.loc[mask, "Стоим_было_1y"] = df_left_f.loc[mask, "Стоимость"]

    # выделение сумм по текущему году в правом и левом файле
    mask = df_right_f["ДатПервПст"] >= datetime(date_year.get_date().year, 1, 1)
    cur_year = str(date_year.get_date().year)
    st_st_cur_year = "Стоим_стало_" + cur_year
    df_right_f.loc[mask, st_st_cur_year] = df_right_f.loc[mask, "Стоимость"]
    st_b_cur_year = "Стоим_было_" + cur_year
    mask = df_left_f["ДатПервПст"] >= datetime(date_year.get_date().year, 1, 1)
    df_left_f.loc[mask, st_b_cur_year] = df_left_f.loc[mask, "Стоимость"]

    df_right_f = df_right_f.rename(
        columns={"Стоимость": "Стоимость_стало", "Наименование подразделения": "Отдел"},
    )
    df_left_f = df_left_f.rename(
        columns={"Стоимость": "Стоимость_было", "Наименование подразделения": "Отдел"},
    )

    # слияние левого и правого файла
    df_conc = pd.concat([df_left_f, df_right_f])

    gcol = [
        "Отдел",
        "Вид деят",
        "Склад",
        "НаимКатегорииЗапаса",
        "sap",
        "Наименование КСМ",
        "Количество",
        "УчетнЦена",
        "Стоимость_было",
        "Стоимость_стало",
        "Постарело",
        "Стоим_было_3y",
        "Стоим_стало_3y",
        "Стоим_было_1y",
        "Стоим_стало_1y",
        "ДатПервПст",
        st_b_cur_year,
        st_st_cur_year,
        "cc",
    ]
    columns_l = [x for x in df_left_f.columns if x in gcol]
    columns_r = [x for x in df_right_f.columns if x in gcol]

    # при объединении запас прыгает по складам. надо добавить в "sap" наименование склада
    df_desc = df_left_f[columns_l].merge(
        df_right_f[columns_r], on=["sap", "cc"], how="outer"
    )
    df_desc[["Количество_x", "Количество_y"]] = df_desc[
        ["Количество_x", "Количество_y"]
    ].fillna(0)
    df_desc["Изм_колво"] = df_desc["Количество_y"] - df_desc["Количество_x"]

    mask = df_desc["Склад_x"].isna()

    # попробовать решение отсюда
    # https://stackoverflow.com/questions/37400246/pandas-update-multiple-columns-at-once
    df_desc.loc[mask, "Склад_x"] = df_desc.loc[mask, "Склад_y"]
    df_desc.loc[mask, "Отдел_x"] = df_desc.loc[mask, "Отдел_y"]
    df_desc.loc[mask, "Вид деят_x"] = df_desc.loc[mask, "Вид деят_y"]
    df_desc.loc[mask, "Наименование КСМ_x"] = df_desc.loc[mask, "Наименование КСМ_y"]
    df_desc.loc[mask, "НаимКатегорииЗапаса_x"] = df_desc.loc[
        mask, "НаимКатегорииЗапаса_y"
    ]

    df_desc.drop(
        [
            "Отдел_y",
            "Склад_y",
            "Вид деят_y",
            "НаимКатегорииЗапаса_y",
            "Наименование КСМ_y",
        ],
        inplace=True,
        axis="columns",
    )
    df_desc = df_desc.rename(
        columns={
            "Склад_x": "Склад",
            "Отдел_x": "Отдел",
            "Вид деят_x": "Вид деят",
            "НаимКатегорииЗапаса_x": "НаимКатегорииЗапаса",
        },
    )

    params["pivot_ind"][0] = "Отдел"
    params["linkcol"] = "Отдел"
    params["pivot_sum"] = [
        "Стоимость_было",
        "Стоимость_стало",
        "Постарело",
        "Стоим_было_3y",
        "Стоим_стало_3y",
        "Стоим_было_1y",
        "Стоим_стало_1y",
        st_b_cur_year,
        st_st_cur_year,
    ]

    table2 = pd.pivot_table(
        df_conc,
        values=params["pivot_sum"],
        index=params["pivot_ind"],
        aggfunc="sum",
    )

    table2.reset_index(inplace=True)
    table2["Изменение"] = table2["Стоимость_стало"] - table2["Стоимость_было"]
    table2["Изм_3y"] = table2["Стоим_стало_3y"] - table2["Стоим_было_3y"]
    table2["Изм_1y"] = table2["Стоим_стало_1y"] - table2["Стоим_было_1y"]
    izm_cy = "Изм_" + cur_year
    table2[izm_cy] = table2[st_st_cur_year] - table2[st_b_cur_year]
    table2["Вовлечение"] = (
        table2["Стоимость_было"] - table2["Стоимость_стало"] - table2["Постарело"]
    )

    rcl = [
        "Стоимость_было",
        "Стоимость_стало",
        "Изменение",
        "Постарело",
        "Вовлечение",
        st_b_cur_year,
        st_st_cur_year,
        izm_cy,
        "Стоим_было_1y",
        "Стоим_стало_1y",
        "Изм_1y",
        "Стоим_было_3y",
        "Стоим_стало_3y",
        "Изм_3y",
    ]

    columns = [x for x in table2.columns if x not in rcl]
    new_order = columns + rcl
    table2 = table2[new_order]

    params["префиксл"] = "CИзм_"
    params["начстрока"] = 5
    params["начколонка"] = 1
    params["страница"] = "Суммы_изм"
    params["tablename"] = "Суммы_изм"

    # сохранение оригинальной таблицы до добавления гиперссылок на листы
    table2_original = table2.copy()
    # df_toexcel_desc(table2, df_conc, params)
    df_toexcel_desc(table2, df_desc, params)

    table2 = table2_original
    table2_original = None

    itogt = pd.pivot_table(
        table2,
        values=params["pivot_sum"]
        + ["Изменение", "Вовлечение", "Изм_3y", "Изм_1y", izm_cy],
        index=params["pivot_ind"][0],
        aggfunc="sum",
    )
    itogt.reset_index(inplace=True)
    # itogt["Изменение"] = itogt["Стоимость_стало"] - itogt["Стоимость_было"]

    # columns = [x for x in itogt.columns if x not in rcl]
    # new_order = columns + rcl
    new_order = [
        "Отдел",
        "Стоимость_было",
        "Стоимость_стало",
        "Изменение",
        "Постарело",
        "Вовлечение",
        st_b_cur_year,
        st_st_cur_year,
        izm_cy,
        "Стоим_было_1y",
        "Стоим_стало_1y",
        "Изм_1y",
        "Стоим_было_3y",
        "Стоим_стало_3y",
        "Изм_3y",
    ]

    itogt = itogt[new_order]

    # sums = itogt[
    #     params["pivot_sum"] + ["Изменение", "Вовлечение", "Изм_3y", "Изм_1y"]
    # ].sum()

    sums = itogt.sum()
    # print(sums)

    itogt.to_excel(
        params["writer"],
        sheet_name="Суммы_изм",
        startrow=len(table2.index) + 8,
        startcol=table2.shape[1] - itogt.shape[1] + 1,
        index=False,
    )

    ws2 = gl_writer.book.get_worksheet_by_name("Суммы_изм")
    ws2.write_string(  # type: ignore
        len(table2.index) + 8 + len(itogt.index) + 1,
        4,
        "Итого:",
        gl_format1_bold,
    )

    ws2.write_row(  # type: ignore
        len(table2.index) + 8 + len(itogt.index) + 1,
        5,
        sums[1:].to_list(),
        gl_format1_bold,
    )

    ws2.set_column(5, 18, 20, gl_format1)  # type: ignore
    ws2.autofit()  # type: ignore
    ws2.set_column(1, 1, 8)  # type: ignore

    ws2.write_string(  # type: ignore
        0,
        1,
        "Изменение объёма  3х-летних запасов",
        gl_format1,
    )

    ws2.write_string(  # type: ignore
        1,
        1,
        "Стоимость_было = Сумма по типу запасов пришедших "
        + str(date_left31.get_date())
        + " и ранее. Из файла: "
        + str(
            PureWindowsPath(left_editor2.get("1.0", tk.END).strip()).with_suffix(
                ".xlsx"
            )
        ),
        gl_format1,
    )

    ws2.write_string(  # type: ignore
        2,
        1,
        "Стоимость_стало = Сумма по типу запасов пришедших "
        + str(date_right3.get_date())
        + " и ранее. Из файла: "
        + str(
            PureWindowsPath(right_editor2.get("1.0", tk.END).strip()).with_suffix(
                ".xlsx"
            )
        ),
        gl_format1,
    )

    # форматирование колонок листа "Суммы"
    # ws.set_column(0, 0, 29, gl_link_format)  # type: ignore
    ws.set_column(0, 0, 29)  # type: ignore
    ws.set_column(1, 1, 30, gl_format1)  # type: ignore
    ws.set_column(2, 2, 21, gl_format1)  # type: ignore
    ws.set_column(3, 3, 39, gl_format1)  # type: ignore
    ws.set_column(4, 4, 14, gl_format1)  # type: ignore

    params["префиксл"] = ["CИзм_"]
    hyperlink(params)

    wsts = workbook.get_worksheet_by_name("Лог загрузки")
    elt = timer("Готово.", startTime)
    statstr = f"Генерация отчета выполнена: {str(datetime.now())} за {str(elt)}, листов в отчете: {len(workbook.worksheets())}"
    wsts.write("B2", statstr)  # type: ignore
    print(statstr, "\n записываем на диск")
    gl_writer.close()
    print(f"Готово! Записано в : {str(Path(path).resolve())}")

    return 0


def loadconf():
    """
    Загрузка из .ini файла или его создание при отсутствии
    """
    global \
        left_file, \
        right_file, \
        mol_file, \
        config, \
        molsap_file, \
        molsap_file3, \
        factf_folder, \
        gl_conf_list, \
        gl_prpath, \
        ost_1c_fname
    fname = os.path.basename(__file__)
    # fname=sys.argv[0]
    conffile = PureWindowsPath(fname).with_suffix(".ini")
    # print(conffile)

    if os.path.isfile(conffile):
        print(f"Загрузка найденного конфиг-файл {conffile}")
        config.read(conffile)
        # print("config.sections()")
        # print(config.sections())
        left_file = config["default"]["left_file"]
        right_file = config["default"]["right_file"]
        mol_file = config["default"]["mol_file"]
        molsap_file = config["default"]["molsap_file"]
        molsap_file3 = config["default"].get("molsap_file3", "")
        factf_folder = config["default"].get("text_factf", "")
        gl_conf_list = list(eval(config["default"].get("text_conflist", "")))
        gl_prpath = config["default"].get("text_gl_prpath")
        ost_1c_fname = config["default"].get("text_ost_1c_fname", "")

    else:
        config["default"] = {}
        # в имя секции зашить разные пути
        defc = config["default"]
        defc["left_file"] = left_file
        defc["right_file"] = right_file
        defc["mol_file"] = mol_file
        defc["molsap_file"] = molsap_file
        defc["molsap_file3"] = molsap_file3
        defc["text_factf"] = factf_folder
        defc["text_gl_prpath"] = gl_prpath
        defc["text_gl_prpath"] = ost_1c_fname

        with open(conffile, "w") as configfile:
            config.write(configfile)
            print(f"Настройки сохранены в конфиг-файл {conffile}")
    return 0


# def print_selection():
#     print(var_factf.get())
#     return 0


if __name__ == "__main__":
    print(f"Версия python: {pyversion}")
    print(f"Версия XlsxWriter: {version('XlsxWriter')}")
    # print(f"Версия openpyxl: {version('openpyxl')}")
    print(f"Версия Pandas: {version('pandas')}")
    print(f"Версия Python-calamine: {version('python-calamine')}")

    # TODO если загрузку конфига производить после создания формы, можно убрать глобальные переменные
    loadconf()

    root = tk.Tk()  # создаем корневой объект - окно
    root.title("Расчет статистики по остаткам")  # устанавливаем заголовок окна
    root.geometry("800x440")  # устанавливаем размеры окна

    tabControl = tk.ttk.Notebook(root)  # type: ignore
    tab1 = tk.ttk.Frame(tabControl)  # type: ignore
    tab2 = tk.ttk.Frame(tabControl)  # type: ignore
    tab3 = tk.ttk.Frame(tabControl)  # type: ignore
    tabControl.add(tab1, text="Основная")
    tabControl.add(tab2, text="Инструкция")
    tabControl.add(tab3, text="Дополнительно")

    tabControl.pack(expand=1, fill="both")

    tk.ttk.Label(  # type: ignore
        tab2,
        text="Инструкция по выбору файлов для генерации отчета:\n\
    1. Файл выгрузки из 1С. Отчет Оборотно сальдовая ведомость.\n\
    2. Файл выгрузки из SAP с остатками на складах МОЛ -2 месяца.\n\
    3. Файл выгрузки из SAP с остатками на складах МОЛ за предыдущий месяц.\n\
    4. Файл выгрузки из SAP с остатками на ЦС за предыдущий месяц.\n\
       Для выгрузки остатков на продажу в другие ДО.",
    ).grid(column=0, row=0, padx=30, pady=30)

    tab1.grid_rowconfigure(index=0, weight=1)
    tab1.grid_rowconfigure(index=1, weight=1)
    tab1.grid_rowconfigure(index=2, weight=1)
    tab1.grid_rowconfigure(index=3, weight=1)
    tab1.grid_rowconfigure(index=4, weight=1)
    tab1.grid_rowconfigure(index=5, weight=1)

    tab1.grid_columnconfigure(index=0, minsize=10)
    tab1.grid_columnconfigure(index=1, weight=1)
    tab1.grid_columnconfigure(index=2, weight=1)
    tab1.grid_columnconfigure(index=3, weight=1)
    tab1.grid_columnconfigure(index=4, weight=1)
    tab1.grid_columnconfigure(index=5, minsize=40)

    for ii in range(0, 6):
        tab3.grid_rowconfigure(index=ii, weight=1)
        tab3.grid_columnconfigure(index=ii, weight=1)

    for ii in range(1, 6):
        tk.Label(tab1, text=str(ii) + ".").grid(column=0, row=ii, padx=0, pady=0)

    # ----наполнение вкладки №3
    tk.Label(
        tab3, text="Дополнительные параметры. Путь к файлам с фактом поставки"
    ).grid(column=0, row=0, columnspan=3, sticky=tk.NSEW)

    for ii in range(6, 9):
        tk.Label(tab3, text=str(ii) + ".").grid(column=0, row=ii - 5, padx=0, pady=0)

    text_factf = tk.Text(tab3, height=3, name="text_factf")
    text_factf.grid(column=1, columnspan=3, row=1, sticky=tk.NSEW)
    text_factf.insert("1.0", factf_folder)
    button_factf = tk.Button(
        tab3,
        text="Выбрать каталог с \nфайлами факта поставки",
        command=lambda: open_file_d(5, text_factf),
    )
    button_factf.grid(column=5, row=1, sticky=tk.NSEW, padx=10)

    # выгрузка со счетами
    # ost_1c_fname

    text_ost_1c_fname = tk.Text(tab3, height=3, name="text_ost_1c_fname")
    text_ost_1c_fname.grid(column=1, columnspan=3, row=2, sticky=tk.NSEW)
    text_ost_1c_fname.insert("1.0", ost_1c_fname)
    button_ost_1c_fname = tk.Button(
        tab3,
        text="Выбрать файл с \nвыгрузкой 1с по счетам",
        command=lambda: open_file_d(7, text_ost_1c_fname),
    )

    button_ost_1c_fname.grid(column=5, row=2, sticky=tk.NSEW, padx=10)

    # var_factf = tk.IntVar()
    # var_factf.set(0)

    # c1 = tk.Checkbutton(
    #     tab3,
    #     text="Проверка факта",
    #     variable=var_factf,
    #     onvalue=1,
    #     offvalue=0,
    #     # command=print_selection,
    # ).grid(column=1, columnspan=3, row=2, sticky=tk.NSEW)

    text_conflist = tk.Text(tab3, height=3, name="text_conflist")
    text_conflist.grid(column=1, columnspan=3, row=3, sticky=tk.NSEW)

    text_conflist.insert(tk.END, "[")
    for x in gl_conf_list:
        text_conflist.insert(
            tk.END,
            "'" + x + "', ",
        )
    # text_conflist.insert("1.0", gl_conf_list)
    text_conflist.delete("end-2c")
    text_conflist.delete("end-2c")
    text_conflist.insert(tk.END, "]")

    button_conflist = tk.Button(
        tab3,
        text="Выбрать файлы\n настроек",
        command=lambda: open_file_d(6, text_conflist),
    )
    button_conflist.grid(column=5, row=3, sticky=tk.NSEW, padx=10)

    # -------наполнение остальных вкладок
    text_mol = tk.Text(tab1, height=3, name="mol_file")
    date_year = DateEntry(
        tab1,
        locale="ru_RU.UTF-8",
        date_pattern="dd.mm.yyyy",
        bg="darkblue",
        fg="white",
        width=30,
    )

    date_year.grid(column=4, row=0, sticky=tk.EW, padx=10)
    tk.Label(tab1, text="Изменить текущий год для отчета->").grid(
        column=3, row=0, padx=0, pady=0, sticky="e"
    )

    text_mol.grid(column=1, columnspan=3, row=1, sticky=tk.NSEW)
    text_mol.insert("1.0", mol_file)
    mol_button = tk.Button(
        tab1,
        text="Выбрать файл \nОСВ из 1С",
        command=lambda: open_file_d(3, text_mol),
    )
    mol_button.grid(column=5, row=1, sticky=tk.NSEW, padx=10)

    left_editor2 = tk.Text(tab1, height=3, name="left_file")
    left_editor2.grid(column=1, columnspan=3, row=2, sticky=tk.NSEW)
    left_editor2.insert("1.0", left_file)

    right_editor2 = tk.Text(tab1, height=3, name="right_file")
    right_editor2.grid(column=1, columnspan=3, row=3, sticky=tk.NSEW)
    right_editor2.insert("1.0", right_file)

    ms_editor2 = tk.Text(tab1, height=3, name="molsap_file")
    ms_editor2.grid(column=1, columnspan=3, row=4, sticky=tk.NSEW)
    ms_editor2.insert("1.0", molsap_file)

    ms_editor3 = tk.Text(tab1, height=3, name="molsap_file3")
    ms_editor3.grid(column=1, columnspan=3, row=5, sticky=tk.NSEW)
    ms_editor3.insert("1.0", molsap_file3)

    date_left31 = DateEntry(
        tab1,
        locale="ru_RU.UTF-8",
        date_pattern="dd.MM.yyyy",
        bg="darkblue",
        fg="white",
        width=30,
    )
    dates = date(datetime.now().year - 3, datetime.now().month, 1)
    dates = dates.replace(month=dates.month, day=1) - timedelta(days=1)
    date_left31.set_date(dates.replace(month=dates.month, day=1) - timedelta(days=1))

    date_left31.grid(column=4, row=2, sticky=tk.EW, padx=10)
    date_right3 = DateEntry(
        tab1,
        locale="ru_RU",
        date_pattern="dd.mm.yyyy",
        bg="darkblue",
        fg="white",
        width=30,
    )

    dates = date(datetime.now().year - 3, datetime.now().month, 1)
    date_right3.set_date(dates.replace(month=dates.month, day=1) - timedelta(days=1))

    date_right3.grid(column=4, row=3, sticky=tk.EW, padx=10)

    open_button = tk.Button(
        tab1,
        text="Выбрать \nначальный \nфайл",
        command=lambda: open_file_d(1, left_editor2),
    )
    open_button.grid(column=5, row=2, sticky=tk.NSEW, padx=10)
    open_button2 = tk.Button(
        tab1,
        text="Выбрать \nконечный \nфайл",
        command=lambda: open_file_d(2, right_editor2),
    )
    open_button2.grid(column=5, row=3, sticky=tk.NSEW, padx=10)
    open_button3 = tk.Button(
        tab1,
        text="Выбрать файл \nвыгрузки ЦС\n конечный",
        command=lambda: open_file_d(4, ms_editor2),
    )
    open_button3.grid(column=5, row=4, sticky=tk.NSEW, padx=10)

    open_button4 = tk.Button(
        tab1,
        text="Выбрать файл \nвыгрузки ЦС\n начальный",
        command=lambda: open_file_d(8, ms_editor3),
    )
    open_button4.grid(column=5, row=5, sticky=tk.NSEW, padx=10)

    start_button = tk.Button(tab1, text="Сформировать", command=start)
    start_button.grid(column=0, row=6, columnspan=6, sticky=tk.NSEW, padx=10, pady=10)

    root.mainloop()
