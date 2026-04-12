import sys
from datetime import date, datetime, timedelta
from importlib.metadata import version
from math import ceil

# from os import remove, system, environ as int_env, path as os_path
from os import system, environ as int_env, stat as stat_file
from pathlib import Path, PureWindowsPath
from sys import version as pyversion

from sqlalchemy import create_engine

if dict(int_env).get("AIRFLOW_HOME", None) is not None:
    from smbclient import open_file, register_session, stat, scandir

# else:
#     import win32com.client
#     from win32com.client import constants as xlconst

import pandas as pd


if __name__ == "__main__":
    from shared_module import (
        loadsettings3,
        loadvp3,
        save_file_data,
        check_file_data,
        set_xl_styles,
        convert_fakt_prqt,
        get_files,
        timer,
    )

    try:
        from mp import gl_factfile as gl_factfile_mp  # type: ignore
        from mp import gl_settings as gl_settings_mp  # type: ignore

        print("main imp mp=", gl_factfile_mp, gl_settings_mp)
    except ModuleNotFoundError:
        print("Не найден mp.py")

else:
    from .shared_module import (
        loadsettings3,
        loadvp3,
        save_file_data,
        check_file_data,
        set_xl_styles,
        get_files,
        timer,
    )

    try:
        from .mp import gl_factfile as gl_factfile_mp  # type: ignore
        from .mp import gl_settings as gl_settings_mp  # type: ignore

        print("rel imp mp=", gl_factfile_mp, gl_settings_mp)
    except ModuleNotFoundError:
        print("Не найден mp.py")


# from python_calamine import CalamineWorkbook

from xlsxwriter.utility import xl_col_to_name

if not __debug__:
    import warnings

    warnings.filterwarnings("ignore", category=DeprecationWarning)

# путь для вывода отчета
gl_out_path = ""
# префикс сервера SMB для DAG
gl_prefix_smb = ""
# признак запуска из DAGa
gl_dagmode = False
# переменные среды при запуске из DAG
gl_dagmode_env = {}

gl_load_cor_res = None

gl_filters = {}
gl_settings = {}
gl_back_addr = {}
gl_df_cmtr = pd.DataFrame(
    columns=["class", "classname", "level", "direction", "addreq"]
)
gl_filtersdf = pd.DataFrame(
    columns=[
        "item",
        "sap",
        "year",
        "toonss",
        "bgt",
        "protcl",
        "protcl_d",
        "year_d",
        "otdel",
    ]
)
gl_df_control = pd.DataFrame(columns=["item", "sap", "year", "toonss", "bgt"])
gl_df_status = pd.DataFrame(
    [
        "1. Консолидация потребности",
        "2. Формирование НМЦ",
        "3. Согласование закупочной документации",
        "4. Размещено на сайте. Сбор оферт",
        "5. Техническая оценка",
        "6. Коммерческая оценка",
        "7. Переторжка",
        "8. Утверждение победителя",
        "9. Закупочные процедуры завершены",
        "10. Заключение договора",
        "11. Законтрактовано",
        "12. Частично поставлено",
        "13. Поставлено",
    ],
    columns=["Статус"],
)
gl_df = pd.DataFrame()
gl_df_project = pd.DataFrame()
# (columns=['item', 'sap', 'year', 'toonss', 'bgt']) # фрейм для контролируемых заявок
gl_writer = ""
# ссылки на стили оформления
gl_style = {}
gl_factfile = r"R:\03. ЗГД_ГИ\07. УМАИТТ\06. Общая\!БП\отчеты вп\факт\помесячно"

# списки заявок дубликатов
startlog, dupelist_k, dupelist_z, dupelist_zd = None, None, None, None


def dupecontrol() -> int:
    """удаление дубликатов из перечня заявок"""
    global gl_filtersdf
    global dupelist_k, dupelist_z, dupelist_zd
    # dupe=gl_filtersdf[(gl_filtersdf["item"]=="заявка") | (gl_filtersdf["item"]=="контроль")].duplicated()
    # dupe=gl_filtersdf[(gl_filtersdf["sap"].duplicated(keep=False)) & ((gl_filtersdf["item"]=="заявка")|(gl_filtersdf["item"]=="контроль"))]

    # список дубликатов типа контроль
    gl_filtersdf.reset_index(inplace=True, drop=True)
    dupe = gl_filtersdf[
        (
            (gl_filtersdf["sap"].duplicated(keep=False))
            & (gl_filtersdf["item"] == "контроль")
        )
    ]

    if len(dupe.index) > 0:
        dupelist_k = dupe.to_csv(
            columns=[
                "item",
                "sap",
                "year",
                "bgt",
                "otdel",
            ],
            index=False,
        ).splitlines()

    # удаление дубликатов котроль
    gl_filtersdf.drop(
        gl_filtersdf[
            (gl_filtersdf["sap"].duplicated(keep=False))
            & (gl_filtersdf["item"] == "контроль")
        ].index,
        inplace=True,
    )
    gl_filtersdf.reset_index(inplace=True, drop=True)

    # список дубликатов типа заявка
    dupe = gl_filtersdf[
        (gl_filtersdf["sap"].duplicated(keep=False))
        & (gl_filtersdf["item"] == "заявка")
    ]
    if len(dupe.index) > 0:
        dupelist_z = dupe.to_csv(
            columns=[
                "item",
                "sap",
                "year",
                "bgt",
                "otdel",
            ],
            index=False,
        ).splitlines()

        dupe = gl_filtersdf[
            (gl_filtersdf["sap"].duplicated(keep="first"))
            & (gl_filtersdf["item"] == "заявка")
        ]
        dupelist_zd = dupe.to_csv(
            columns=[
                "item",
                "sap",
                "year",
                "bgt",
                "otdel",
            ],
            index=False,
        ).splitlines()

    # удаление дубликатов типа заявка
    gl_filtersdf.drop(dupe.index, inplace=True)
    gl_filtersdf.reset_index(inplace=True, drop=True)
    return 0


def cor_data() -> int:
    """Процедура внесения корректировок в данные из возвратного плана.
    корректировка видов деятельности в заявках ВП по суммам и принудительным настройкам
    корректировка отражения сумм по лицензиям."""
    global gl_df, startlog

    mask = gl_df["Кол-во АС расч."] > 0
    gl_df.loc[mask, "ismtr"] = (
        gl_df.loc[mask, "Сумма"] / gl_df.loc[mask, "Кол-во АС расч."]
    )

    date1 = date(int(gl_settings["Период"][0]), 1, 1)  # noqa: F841
    # дата до которой действовало ОНСС=40к без НДС
    date4 = date(2023, 12, 31)  # noqa: F841
    # дата для применения ОНСС=100к без НДС
    date100 = date(2024, 1, 1)  # noqa: F841

    # рекласс на мтр по цене поставки
    query = '`Вид деятельности` == "Оборудование не входящее в смету строек" & СРОК >= @date1 & СРОК <= @date4 & ismtr < 48000'
    gl_df.loc[gl_df.eval(query), "Вид деятельности"] = "Основная деятельность"

    # рекласс на мтр по цене поставки (100к без НДС (120к с НДС) с 01.01.2024 )
    query = '`Вид деятельности` == "Оборудование не входящее в смету строек" & СРОК >= @date100 & (ismtr < 120000 | `Цена, руб. с НДС` < 120000)'
    gl_df.loc[gl_df.eval(query), "Вид деятельности"] = "Основная деятельность"

    # рекласс на онсс по списку из настроек
    if gl_filters.get("онсс", None) is not None:
        makeonss = gl_filters["онсс"]  # noqa: F841
        query = '`Вид деятельности` == "Основная деятельность" & `/ САП`==@makeonss'
        gl_df.loc[gl_df.eval(query), "Вид деятельности"] = (
            "Оборудование не входящее в смету строек"
        )
        if not gl_df_project.empty:
            gl_df_project.loc[gl_df_project.eval(query), "Вид деятельности"] = (
                "Оборудование не входящее в смету строек"
            )

    # рекласс на мтр по списку из настроек
    if gl_filters.get("мтр", None) is not None:
        makemtr = gl_filters["мтр"]  # noqa: F841
        query = '`Вид деятельности` == "Оборудование не входящее в смету строек" & `/ САП`==@makemtr'
        gl_df.loc[gl_df.eval(query), "Вид деятельности"] = "Основная деятельность"
        if not gl_df_project.empty:
            gl_df_project.loc[gl_df_project.eval(query), "Вид деятельности"] = (
                "Основная деятельность"
            )

    # корректировка сумм по лицензиям
    # mask = (gl_df["Сокр оператор"] == "СИБ ЦП") & (
    mask = (gl_df["Оператор закупки"] == "СИБ ЦП") & (
        (gl_df["Сумма"].isna()) | (gl_df["Сумма"] == 0)
    )

    if len(gl_df[mask].index) > 0:
        if startlog is None:
            startlog = []
        startlog.append(
            "Нулевые итоговые суммы заполнены плановыми суммами по следующим строкам ВП:"
        )
        startlog += (
            gl_df[mask]
            .to_csv(
                columns=[
                    "/ САП",
                    "Наименование заявки",
                    "Полное имя материала",
                    "Наименование заявителя",
                    "СРОК",
                    "Статус",
                    "Сокр оператор",
                ],
                index=False,
            )
            .splitlines()
        )
        startlog.append("")
        gl_df.loc[mask, "Сумма"] = gl_df.loc[mask, "Сумма, руб. с НДС"]
    return 0


def reportblock2(worksheet2, cfg) -> int:
    """
    Вывод блока отчета Итоги в excel
    """
    start_row = cfg["strn"]
    perv_god = int(gl_settings["Период"][0])

    aa = 0
    if (
        cfg["period"] != perv_god
        and (not gl_df_project.empty)
        and cfg["тип"] != "КОНТРОЛЬ"
    ):
        aa = 1  # добавка для блоков с проектами
    worksheet2.merge_range(
        "A" + str(start_row) + ":A" + str(start_row + 5 + aa),
        cfg["тип"] + " " + str(cfg["period"]),
        gl_style["gl_style_text_wrap"],
    )

    worksheet2.write("B" + str(start_row), "Всего заявок в " + str(cfg["period"]))
    worksheet2.write("B" + str(start_row + 1), "БП " + str(cfg["period"]))

    if cfg["тип"] != "КОНТРОЛЬ":
        worksheet2.write_url(
            "B" + str(start_row + 2),
            "internal:'Из_" + str(cfg["period"] - 1) + "_" + cfg["тип"] + "'!A1",
            string="Перенесено с " + str(cfg["period"] - 1),
        )
        worksheet2.write_url(
            "B" + str(start_row + 3),
            "internal:'Поставлено_" + str(cfg["period"]) + "_" + cfg["тип"] + "'!A1",
            string="Поставлено_" + cfg["тип"],
        )
        worksheet2.write_url(
            "B" + str(start_row + 4),
            "internal:'Ожидается_" + str(cfg["period"]) + "_" + cfg["тип"] + "'!A1",
            string="Ожидается_" + cfg["тип"],
        )
        worksheet2.write_url(
            "B" + str(start_row + 5),
            "internal:'Перенос_" + str(cfg["period"] + 1) + "_" + cfg["тип"] + "'!A1",
            string="Перенесено " + cfg["тип"] + " на " + str(cfg["period"] + 1),
        )
    else:
        worksheet2.write("B" + str(start_row + 2), "")
        worksheet2.write("B" + str(start_row + 3), "")
        worksheet2.write_url(
            "B" + str(start_row + 4),
            "internal:'Ожидается_" + str(cfg["period"]) + "_" + cfg["тип"] + "'!A1",
            string="Ожидается_" + cfg["тип"],
        )
        worksheet2.write("B" + str(start_row + 5), "")

    # всего заявок
    worksheet2.write_formula(
        "C" + str(start_row),
        "=SUM(C"
        + str(start_row + 3)
        + ",C"
        + str(start_row + 4)
        + ",C"
        + str(start_row + 5)
        + ")",
        gl_style["gl_style_moneyB"],
    )

    # бюджет
    bgt = gl_filtersdf[
        (gl_filtersdf["item"] == "бюджетс")
        & (gl_filtersdf["year"] == cfg["period"])
        & (gl_filtersdf["toonss"] == cfg["тип"])
    ]["sap"].sum()
    # worksheet2.write('C'+str(start_row+1),
    #                  gl_settings[cfg['period']][cfg['тип']], money)
    worksheet2.write(
        "C" + str(start_row + 1), "=" + str(bgt) + "/1000", gl_style["gl_style_money"]
    )

    colS = cfg["колСУММном"]

    # сумма пришло слева
    if cfg["тип"] != "КОНТРОЛЬ":
        rowS = cfg["из"]
        worksheet2.write_formula(
            "C" + str(start_row + 2),
            "=SUM(Из_"
            + str(cfg["period"] - 1)
            + "_"
            + cfg["тип"]
            + "!"
            + colS
            + "2:"
            + colS
            + rowS
            + ")/1000",
            gl_style["gl_style_money"],
        )

        # сумма поставленного
        rowS = cfg["пост_стр"]
        colSp = cfg["колСУММплан"]
        worksheet2.write_formula(
            "C" + str(start_row + 3),
            "=(SUM(Поставлено_"
            + str(cfg["period"])
            + "_"
            + cfg["тип"]
            + "!"
            + colS
            + "2:"
            + colS
            + rowS
            + ")+"
            + str(cfg["dop_post"])
            + ")/1000",
            gl_style["gl_style_money"],
        )
        worksheet2.write_formula(
            "I" + str(start_row + 3),
            "=(SUM(Поставлено_"
            + str(cfg["period"])
            + "_"
            + cfg["тип"]
            + "!"
            + colSp
            + "2:"
            + colSp
            + rowS
            + ")+"
            + str(cfg["dop_post"])
            + ")/1000"
            + "-C"
            + str(start_row + 3),
            gl_style["gl_style_money"],
        )

        # сумма переноса
        rowS = cfg["перенос_стр"]
        worksheet2.write_formula(
            "C" + str(start_row + 5),
            "=SUM(Перенос_"
            + str(cfg["period"] + 1)
            + "_"
            + cfg["тип"]
            + "!"
            + colS
            + "2:"
            + colS
            + rowS
            + ")/1000",
            gl_style["gl_style_money"],
        )

        # сумма проектов
        if cfg["period"] != perv_god and (not gl_df_project.empty):
            worksheet2.write_url(
                "B" + str(start_row + 6),
                "internal:'Проекты_" + str(cfg["period"]) + "_" + cfg["тип"] + "'!A1",
                string="Проекты_" + cfg["тип"],
            )

            rowS = cfg["проекты_стр"]
            worksheet2.write_formula(
                "C" + str(start_row + 6),
                "=SUM(Проекты_"
                + str(cfg["period"])
                + "_"
                + cfg["тип"]
                + "!"
                + colS
                + "2:"
                + colS
                + rowS
                + ")/1000",
                gl_style["gl_style_money"],
            )
            worksheet2.write_formula(
                "G" + str(start_row + 6),
                "=F" + str(start_row + 5) + "-C" + str(start_row + 6),
                gl_style["gl_style_money"],
            )

    # сумма ожидаемого
    rowS = cfg["ожид_стр"]
    worksheet2.write_formula(
        "C" + str(start_row + 4),
        "=SUM(Ожидается_"
        + str(cfg["period"])
        + "_"
        + cfg["тип"]
        + "!"
        + colS
        + "2:"
        + colS
        + rowS
        + ")/1000",
        gl_style["gl_style_money"],
    )

    # Остаток сейчас
    worksheet2.write_formula(
        "D" + str(start_row + 3),
        "=SUM(C" + str(start_row + 1) + "-C" + str(start_row + 3) + ")",
        gl_style["gl_style_money"],
    )

    # остаток ожидается
    worksheet2.write_formula(
        "E" + str(start_row + 4),
        "=C"
        + str(start_row + 1)
        + "-C"
        + str(start_row + 3)
        + "-C"
        + str(start_row + 4),
        gl_style["gl_style_money"],
    )

    # остаток если всё прийдёт
    worksheet2.write_formula(
        "F" + str(start_row + 5),
        "=C"
        + str(start_row + 1)
        + "-C"
        + str(start_row + 3)
        + "-C"
        + str(start_row + 4)
        + "-C"
        + str(start_row + 5),
        gl_style["gl_style_money"],
    )

    if (
        cfg["period"] != perv_god
        and (not gl_df_project.empty)
        and cfg["тип"] != "КОНТРОЛЬ"
    ):
        drow = 1
    else:
        drow = 0

    for col in range(0, 7):
        # при выводе последнего блока не рисуем верхнюю границу следующего блока
        if cfg["тип"] != "КОНТРОЛЬ":
            worksheet2.write(
                start_row + 5 + drow, col, "", gl_style["gl_style_border_td"]
            )
        else:
            worksheet2.write(
                start_row + 5 + drow, col, "", gl_style["gl_style_border_t"]
            )

    for row in range(0, 6 + drow):
        worksheet2.write(start_row + row - 1, 7, "", gl_style["gl_style_border_l"])

    return 6 + drow


def reportstatlog(worksheet3, df, dfa):
    """
    Вывод лога сообщений и ошибок в консоль и указанный лист excel

    Возвращает количество строк в списке выведенном в excel
    """
    global gl_df_control, startlog, dupelist_k, dupelist_z, dupelist_zd, gl_filters

    # gl_filtersdf = pd.DataFrame(columns=['item', 'sap', 'year', 'toonss', 'bgt'])
    out = []
    out.append("Затычка. заполняется перед закрытием отчета")
    out.append("Использованы файлы настройки:")
    # добавление версий конфигов в лог
    for li in gl_settings["конфиги"]:
        out.append(li)
    out.append("")

    # суммирование всех заявок в список
    values = []
    for yearv in gl_settings["Период"]:
        values = values + gl_filters.get(int(yearv), [])

    values += gl_filtersdf[gl_filtersdf["item"] == "контроль"]["sap"].to_list()

    # добавление в лог накопленных сообщений
    if startlog is not None:
        out += startlog
    out.append("")

    if dupelist_k is not None:
        out.append("Удалены заявки-контроль дубликаты. (не влияет на бюджет)")
        out += dupelist_k
    out.append("")

    if dupelist_z is not None:
        out.append("!Найдены заявки дубликаты! (Важно! Проверить!)")
        out += dupelist_z
    out.append("")

    if dupelist_zd is not None:
        out.append("!Удалены заявки дубликаты! (Важно! Проверить!)")
        out += dupelist_zd
    out.append("")

    if __debug__:
        print(f"Заявок в фильтре: {len(values)}")
    out.append(f"Заявок в фильтре: {len(values)}")

    if __debug__:
        print(f"Выбрано строк в dataframe: {df.shape[0]}")
        print(f"Выбрано строк в project: {gl_df_project.shape[0]}")
        print(f"Выбрано строк в control: {gl_df_control.shape[0]}")
    out.append(f"Выбрано строк в dataframe: {df.shape[0]}")
    out.append(f"Выбрано строк в project: {gl_df_project.shape[0]}")
    # убрать символ перевода строки из текста
    gl_df_project["Полное имя материала"] = gl_df_project[
        "Полное имя материала"
    ].str.replace(r"\n", "", regex=False)
    out += gl_df_project.to_csv(
        columns=[
            "/ САП",
            "Наименование заявки",
            "Полное имя материала",
            "Наименование заявителя",
            "СРОК",
            "Статус",
        ],
        index=False,
    ).splitlines()
    out.append("")

    out.append(f"Выбрано строк в control: {gl_df_control.shape[0]}")

    # все заявки из фильтра нашли в БД
    # sdft = df[~df['/ САП'].isin(values)]

    ress = list(
        set(values)
        - set(df["/ САП"])
        - set(gl_df_project["/ САП"])
        - set(gl_df_control["/ САП"])
    )

    if len(ress) > 0:
        if __debug__:
            print(
                f'Количество заявок из фильтра не нашли в "Возвратном плане": {len(ress)}'
            )
        out.append(
            f'Количество заявок из фильтра не нашли в "Возвратном плане": {len(ress)}'
        )
        out += ress
    else:
        if __debug__:
            print('Все заявки из фильтра нашли в "Возвратном плане"')
        out.append('Все заявки из фильтра нашли в "Возвратном плане"')
    ress = None

    false_state = ["Ожидается снятие", "НПП не утвержден", "0. Снято"]
    fs = dfa[dfa["Статус"].isin(false_state)]
    if fs.shape[0] > 0:
        if __debug__:
            print(f"Необходимо проверить статус заявок! {fs.shape[0]}шт.")
            print(fs)
        out.append("")
        out.append(f"\n Необходимо проверить статус заявок! {fs.shape[0]}шт.")
        out += fs.to_csv(
            columns=[
                "/ САП",
                "Наименование заявки",
                "Полное имя материала",
                "Наименование заявителя",
                "СРОК",
                "Статус",
            ],
            index=False,
        ).splitlines()
        out.append("")

    fs = gl_df_project[gl_df_project["Статус"].isin(false_state)]
    if fs.shape[0] > 0:
        out.append("")
        out.append(f"\n Необходимо проверить статус заявок проектов! {fs.shape[0]}шт.")
        out += fs.to_csv(
            columns=[
                "/ САП",
                "Наименование заявки",
                "Полное имя материала",
                "Наименование заявителя",
                "СРОК",
                "Статус",
            ],
            index=False,
        ).splitlines()
        out.append("")

    # анализ количества строк по заявителям + по фильтру заявок
    if len(gl_filters["заявитель"]) > 0:
        valuesz = gl_filters["заявитель"]

        date1 = pd.to_datetime(date(int(gl_settings["Период"][0]), 1, 1))
        # выборка заявок по Дате и Заявителю
        dft = dfa[
            (dfa["СРОК"] >= date1) & (dfa["Наименование заявителя"].isin(valuesz))
        ]

        drop_z = gl_filters.get("СброситьЗ", [])

        if len(drop_z) > 0:
            dft = dft[~dft["/ САП"].isin(drop_z)]

        if __debug__:
            print(f"Выбрано строк по заявителям из Dataframe:{dft.shape[0]}")
        out.append(f"Выбрано строк по заявителям из Dataframe:{dft.shape[0]}")
        # dft=null

        # все заявки по заявителям есть в "заявках" по номерам или в "контроль"
        sdft = dft[
            ~(
                (dft["/ САП"].isin(df["/ САП"]))
                | (
                    dft["/ САП"].isin(
                        gl_filtersdf[gl_filtersdf["item"] == "контроль"]["sap"]
                    )
                )
            )
        ]

        # все заявки по номерам есть в заявках по заявителям минус чужие заявки
        if gl_filters.get("ЧужиеЗ", None) is not None:
            sdft2 = df[
                ~(
                    (df["/ САП"].isin(dft["/ САП"]))
                    | (df["/ САП"].isin(gl_filters["ЧужиеЗ"]))
                )
            ]
        else:
            sdft2 = df[~df["/ САП"].isin(dft["/ САП"])]

        # выборка по не включнным в фильтр
        if sdft.shape[0] > 0:
            if __debug__:
                print(
                    f"Внимание, есть заявки не включенные в фильтр! Отчет может быть не верен!. \n \t Количество заявок отсутствующих в перечне заявок: (df): {sdft.shape[0]}"
                )
            out.append(
                f"Внимание, есть заявки не включенные в фильтр! Отчет может быть не верен!. \n \t Количество заявок отсутствующих в перечне заявок: {sdft.shape[0]}"
            )
            out += sdft.to_csv(
                columns=[
                    "/ САП",
                    "Наименование заявки",
                    "Полное имя материала",
                    "Наименование заявителя",
                    "СРОК",
                ],
                index=False,
            ).splitlines()
        else:
            if __debug__:
                print("Все заявки по заявителям выбранные из БД входят в фильтр")
            out.append("Все заявки по заявителям выбранные из БД входят в фильтр")
        out.append("")

        # выборка не по заявителям
        if sdft2.shape[0] > 0:
            if __debug__:
                print(
                    f"Количество заявок в фильтре не по заявителям (df): {sdft2.shape[0]}"
                )
            out.append(
                f"Количество заявок в фильтре не по заявителям (df): {sdft2.shape[0]}"
            )
            out += sdft2.to_csv(
                columns=[
                    "/ САП",
                    "Наименование заявки",
                    "Полное имя материала",
                    "Наименование заявителя",
                    "СРОК",
                ],
                index=False,
            ).splitlines()
    if __debug__:
        print(
            "\n=Проверка соответствия наименования бюджетов заявок запланированным наименованиям бюджетов="
        )
    out.append("")
    out.append(
        "\n=Проверка соответствия наименования бюджетов заявок запланированным наименованиям бюджетов="
    )

    # проверка что все бюджеты из заявок входят в итоговые бюджеты
    cdf = (
        gl_filtersdf[
            (gl_filtersdf["item"] == "заявка") | (gl_filtersdf["item"] == "контроль")
        ]["bgt"]
        .isin(gl_filtersdf[gl_filtersdf["item"] == "бюджетс"]["bgt"])
        .all()
    )
    if cdf:
        if __debug__:
            print("+ пройдено.")
        out.append("+ пройдено.")
    else:
        # построение битмаски ненайденных бюджетов для вычисления заявки
        cdfv = ~(
            gl_filtersdf[
                (gl_filtersdf["item"] == "заявка")
                | (gl_filtersdf["item"] == "контроль")
            ]["bgt"].isin(gl_filtersdf[gl_filtersdf["item"] == "бюджетс"]["bgt"])
        )
        cdd = gl_filtersdf[
            (gl_filtersdf["item"] == "заявка") | (gl_filtersdf["item"] == "контроль")
        ][cdfv]
        if __debug__:
            print("\n !Ошибка. Есть заявка с бюджетом отсутствующим в планах.")
            print(
                cdd.to_string(
                    columns=["item", "sap", "year", "toonss", "bgt"], index=False
                )
            )
        out.append("!Ошибка. Есть заявка с бюджетом отсутствующим в планах.")
        out += cdd.to_csv(
            columns=["item", "sap", "year", "toonss", "bgt"], index=False
        ).splitlines()

    if __debug__:
        print("\n=Проверка однотипности заявок в бюджете=")
    out.append("")
    out.append("\n=Проверка однотипности заявок в бюджете=")

    for ii in gl_filtersdf[gl_filtersdf["item"] == "заявка"]["bgt"].unique():
        # выборка номеров заявок принадлежащих бюджету
        bfgf = gl_filtersdf[
            (gl_filtersdf["item"] == "заявка") & (gl_filtersdf["bgt"] == ii)
        ]["sap"]
        # определение типа бюджета
        type_b = gl_filtersdf[
            (gl_filtersdf["item"] == "бюджетс") & (gl_filtersdf["bgt"] == ii)
        ]["toonss"].iloc[0]
        if type_b == "ОНСС":
            vidd = "Оборудование не входящее в смету строек"
            nvidd = "Основная деятельность"
        elif type_b == "МТР":
            vidd = "Основная деятельность"
            nvidd = "Оборудование не входящее в смету строек"
        else:
            vidd = ""
            print(f"Ошибка! Тип бюджета: {ii} на определён!")
            out.append(
                f"Ошибка! Тип бюджета: {ii} на определён! Необходимо скорректировать настройки!"
            )

        # выборка заявок с неправильным типом бюджета
        bfdf = df[(df["/ САП"].isin(bfgf)) & (df["Вид деятельности"] != vidd)]

        if len(bfdf.index) > 0:
            print(
                f'В бюджет {ii} - {vidd} входят заявки типа "{nvidd}", необходима корректировка!'
            )
            out.append("")
            out.append(
                f'В бюджет {ii} - {vidd} входят заявки типа "{nvidd}", необходима корректировка!'
            )
            out += bfdf.to_csv(
                columns=[
                    "/ САП",
                    "Наименование заявки",
                    "Полное имя материала",
                    "Вид деятельности",
                    "СРОК",
                ],
                index=False,
            ).splitlines()
    if __debug__:
        print("=Завершена проверка однотипности заявок в бюджете= \n")
    out.append("\n=Завершена проверка однотипности заявок в бюджете=")

    worksheet3.write_column(1, 1, out)
    sdft = None
    sdft2 = None

    return len(out)


def toexcelf(datf: pd.DataFrame, sname: str) -> bool:
    """Вывод датафрейма в excel (gl_writer) с применением формата колонок
    datf: - датафрейм для вывода
    sname: - имя страницы для вывода
    """
    datf.to_excel(gl_writer, sheet_name=sname, index=False)
    # установка форматов колонок
    ws = gl_writer.book.get_worksheet_by_name(sname)
    ind = datf.columns.get_loc("Сумма")
    ws.set_column(ind, ind, 14, gl_style["gl_style_money2"])
    ind = datf.columns.get_loc("Цена, руб. с НДС")
    ws.set_column(ind, ind, 14, gl_style["gl_style_money2"])
    ind = datf.columns.get_loc("Сумма, руб. с НДС")
    ws.set_column(ind, ind, 14, gl_style["gl_style_money2"])
    ind = datf.columns.get_loc("СРОК")
    ws.set_column(ind, ind, 10)
    # скрытие колонок НПП
    ind = datf.columns.get_loc("Номер НПП")
    ws.set_column(ind, ind, None, None, {"hidden": 1})
    ind = datf.columns.get_loc("Позиция НПП")
    ws.set_column(ind, ind, None, None, {"hidden": 1})

    return True


def report_year(
    # period: int, num_ytorep:int, df: pd.DataFrame, worksheet2, startrow: int, ttype: str
    num_ytorep: int,
    df: pd.DataFrame,
    worksheet2,
    startrow: int,
    ttype: str,
) -> int:
    """вывод в отчет информации за год
    num_ytorep:int индекс года из списка (с нуля),
    df: pd.DataFrame,
    worksheet2,
    startrow: int,
    ttype: str
    """
    global gl_settings
    global gl_filters
    global gl_df_control, gl_df_project
    dfp = pd.DataFrame
    period = int(gl_settings["Период"][num_ytorep])

    if ttype == "КОНТРОЛЬ":
        if __debug__:
            print("вывод контролируемых заявок")
        gl_df_control.to_excel(
            gl_writer, sheet_name="Ожидается_" + str(period) + "_" + ttype, index=False
        )  # type: ignore
        cfg = {}
        cfg["period"] = period
        cfg["колСУММном"] = xl_col_to_name(gl_df_control.columns.get_loc("Сумма"))  # type: ignore
        cfg["strn"] = startrow
        cfg["тип"] = ttype
        cfg["пост_стр"] = ""
        cfg["ожид_стр"] = str(gl_df_control.shape[0] + 1)  # type: ignore
        cfg["из"] = ""
        cfg["перенос_стр"] = ""
        cfg["dop_post"] = 0
        rowsw = reportblock2(worksheet2, cfg)
        return rowsw

    date1 = date(period, 1, 1)  # noqa: F841
    date2 = date(period, 12, 31)  # noqa: F841
    date3 = date(period + 1, 1, 1)  # noqa: F841
    # date4 = date(period + 1, 12, 31)

    perv_god = int(gl_settings["Период"][0])
    zzcur = gl_filters.get(period, [])  # noqa: F841

    zz = []
    zzi = []

    for np in range(0, num_ytorep + 1):
        zzi = zz  # noqa: F841
        zz = zz + gl_filters.get(int(gl_settings["Период"][np]), [])

    if ttype == "ОНСС":
        vidd = "Оборудование не входящее в смету строек"
    else:
        vidd = "Основная деятельность"  # noqa: F841

    # df2=поставлено
    df2 = df.query(
        '`Вид деятельности` == @vidd & СРОК >= @date1 & СРОК <= @date2 & Статус == "13. Поставлено"'
    )
    # df3=ожидается
    df3 = df.query(
        '`Вид деятельности` == @vidd & СРОК >= @date1 & СРОК <= @date2 & Статус != "13. Поставлено"'
    )
    # dfi=пришедшие слева. в пришедшие попадают все предыдущие заявки по годам еще не поставленные со сроком больше начала текущего периода
    dfi = df.query(
        '`Вид деятельности` == @vidd & Статус != "13. Поставлено" & `/ САП`==@zzi & СРОК >= @date1'
    )
    # df23=перенос вправо. в перенос попадают все предыдущие заявки по годам со сроком поставки больше года заявки
    df23 = df.query("`Вид деятельности` == @vidd & СРОК >= @date3 & `/ САП`==@zz")
    # выборка в БД проектов
    if period != perv_god and (not gl_df_project.empty):
        dfp = gl_df_project.query("`Вид деятельности` == @vidd & `/ САП`==@zzcur")

    sheet_name = "Поставлено_" + str(period) + "_" + ttype
    toexcelf(df2, sheet_name)

    sheet_name = "Ожидается_" + str(period) + "_" + ttype
    toexcelf(df3, sheet_name)

    sheet_name = "Из_" + str(period - 1) + "_" + ttype
    toexcelf(dfi, sheet_name)

    sheet_name = "Перенос_" + str(period + 1) + "_" + ttype
    toexcelf(df23, sheet_name)

    if period != perv_god and (not gl_df_project.empty):
        sheet_name = "Проекты_" + str(period) + "_" + ttype
        toexcelf(dfp, sheet_name)

    cfg = {}
    cfg["period"] = period
    cfg["колСУММном"] = xl_col_to_name(df2.columns.get_loc("Сумма"))
    cfg["колСУММплан"] = xl_col_to_name(df2.columns.get_loc("Сумма, руб. с НДС"))
    cfg["strn"] = startrow
    cfg["тип"] = ttype
    cfg["пост_стр"] = str(df2.shape[0] + 1)
    cfg["ожид_стр"] = str(df3.shape[0] + 1)
    cfg["из"] = str(dfi.shape[0] + 1)
    cfg["перенос_стр"] = str(df23.shape[0] + 1)

    if ttype == "ОНСС":
        sdp = gl_filtersdf[
            (gl_filtersdf["item"] == "поставка без заявок месяц")
            & (gl_filtersdf["year_d"].dt.year == period)
            & (gl_filtersdf["toonss"] == "ОНСС")
        ]["sap"].sum()
    else:
        sdp = gl_filtersdf[
            (gl_filtersdf["item"] == "поставка без заявок месяц")
            & (gl_filtersdf["year_d"].dt.year == period)
            & (gl_filtersdf["toonss"] == "МТР")
        ]["sap"].sum()

    cfg["dop_post"] = sdp
    if period != perv_god and (not gl_df_project.empty):
        cfg["проекты_стр"] = str(dfp.shape[0] + 1)  # type: ignore

    rowsw = reportblock2(worksheet2, cfg)
    return rowsw


def reportstat(df: pd.DataFrame, startyear: int, strow: int) -> int:
    """Заполнение листа Статистика
    возвращает номер следующей пустой строки
    startyear {0,1,2}     первый(нулевой) год
    ожидает "global gl_writer" для вывода листов
    """
    global gl_writer
    global gl_filtersdf

    fy = gl_settings["Период"][startyear][2:]  # first year
    sy = str(int(gl_settings["Период"][startyear][2:]) + 1)
    zayavka22 = gl_filters.get(int(gl_settings["Период"][startyear]), [])
    date3 = date(int(gl_settings["Период"][startyear]) + 1, 1, 1)  # noqa: F841
    date4 = date(int(gl_settings["Период"][startyear]) + 1, 12, 31)  # noqa: F841

    dfs_1 = (
        df.query('`/ САП`==@zayavka22 & `Вид деятельности` == "Основная деятельность"')
        .groupby(["Статус"])["Статус"]
        .count()
        .reset_index(name="МТР_" + fy)
    )
    dfs_2 = (
        df.query(
            '`/ САП`==@zayavka22  & `Вид деятельности` == "Основная деятельность" & СРОК >= @date3 & СРОК <= @date4'
        )
        .groupby(["Статус"])["Статус"]
        .count()
        .reset_index(name="МТР_" + fy + "->" + sy)
    )
    dfs = pd.merge(dfs_1, dfs_2, how="outer", on=["Статус"])
    dfs_1 = (
        df.query(
            '`/ САП`==@zayavka22 & `Вид деятельности` == "Оборудование не входящее в смету строек"'
        )
        .groupby(["Статус"])["Статус"]
        .count()
        .reset_index(name="ОНСС_" + fy)
    )
    dfs = pd.merge(dfs, dfs_1, how="outer", on=["Статус"])
    dfs_1 = (
        df.query(
            '`/ САП`==@zayavka22  & `Вид деятельности` == "Оборудование не входящее в смету строек" & СРОК >= @date3 & СРОК <= @date4'
        )
        .groupby(["Статус"])["Статус"]
        .count()
        .reset_index(name="ОНСС_" + fy + "->" + sy)
    )
    dfs = pd.merge(dfs, dfs_1, how="outer", on=["Статус"])
    dfs_1 = (
        df.query("`/ САП`==@zayavka22")
        .groupby(["Статус"])["Статус"]
        .count()
        .reset_index(name="Все_" + fy)
    )
    dfs = pd.merge(dfs, dfs_1, how="outer", on=["Статус"])
    dfs_1 = (
        df.query("`/ САП`==@zayavka22 & СРОК >= @date3 & СРОК <= @date4")
        .groupby(["Статус"])["Статус"]
        .count()
        .reset_index(name="Все_" + fy + "->" + sy)
    )
    dfs = pd.merge(dfs, dfs_1, how="outer", on=["Статус"])

    # zayavka22 = zayavka22 + gl_filters[int(gl_settings["Период"][startyear - 1])]
    # сборка списка всех предыдущих заявок обратным циклом по годам
    # startyear {0,1,2}     первый(нулевой) год
    for yn in range(startyear, 0, -1):
        zayavka22 = zayavka22 + gl_filters[int(gl_settings["Период"][yn - 1])]

    date1 = date(int(gl_settings["Период"][startyear]), 1, 1)
    date2 = date(int(gl_settings["Период"][startyear]), 12, 31)  # noqa: F841
    dfs_1 = (
        df.query(
            '`/ САП`==@zayavka22  & `Вид деятельности` == "Основная деятельность" & СРОК >= @date1'
        )
        .groupby(["Статус"])["Статус"]
        .count()
        .reset_index(name="Все_МТР_в_" + fy)
    )
    dfs = pd.merge(dfs, dfs_1, how="outer", on=["Статус"])
    # МТР заявки текущего и предыдущего годов со сроком поставки больше текущего года.
    dfs_1 = (
        df.query(
            '`/ САП`==@zayavka22  & `Вид деятельности` == "Основная деятельность" & СРОК >= @date2'
        )
        .groupby(["Статус"])["Статус"]
        .count()
        .reset_index(name="Все_МТР->" + sy)
    )
    dfs = pd.merge(dfs, dfs_1, how="outer", on=["Статус"])
    # прилепляем отсутствующие статусы и зануляем отсутствующие значения
    dfs = pd.merge(dfs, gl_df_status, how="outer")
    dfs.fillna(0, inplace=True)

    dfs["ind"] = dfs["Статус"].str.extract(r"(\d+)").apply(pd.to_numeric)
    dfs = dfs.sort_values("ind", axis=0, ascending=True)
    dfs.drop("ind", inplace=True, axis=1)

    cols = dfs.columns.tolist()
    if cols.index("Статус") != 0:
        cols.insert(0, cols.pop(cols.index("Статус")))
        dfs = dfs[cols]

    dfs.to_excel(gl_writer, sheet_name="Статистика", startrow=strow, index=False)

    workbook = gl_writer.book
    ws_stsat = workbook.get_worksheet_by_name("Статистика")

    ws_stsat.merge_range(
        "A" + str(strow) + ":W" + str(strow),
        "Заявочная компания " + gl_settings["Период"][startyear],
        gl_style["gl_style_cell_bc"],
    )

    # вывод сумм под статистикой по заявкам
    endrow = strow + dfs.shape[0]
    if endrow != strow:  # затычка для пустого фрейма, иначе формулы циклятся
        aa = 0
        if startyear != 0:
            aa = 2  # на две буквы больше

        zayavka22 = gl_filters.get(int(gl_settings["Период"][startyear]), [])
        for i in range(ord("B"), ord("G") + 1 + aa):
            ii = chr(i)

            # вывод расшифровки на "МТР_xx"
            if ii == "B":
                # zayavka22 = gl_filters[int(gl_settings["Период"][startyear])]

                dfs_1 = df.query(
                    '`/ САП`==@zayavka22 & `Вид деятельности` == "Основная деятельность"'
                )
                sheetname = "Стт_" + str(date1.year) + "_" + "МТР_" + fy
                toexcelf(dfs_1, sheetname)
                gl_back_addr[sheetname] = ii + str(endrow + 2)
                ws_stsat.write_url(
                    gl_back_addr[sheetname], "internal:'" + sheetname + "'!A1"
                )

            # вывод расшифровки на "ОНСС_xx"
            if ii == "D":
                # zayavka22 = gl_filters[int(gl_settings["Период"][startyear])]

                dfs_1 = df.query(
                    '`/ САП`==@zayavka22 & `Вид деятельности` == "Оборудование не входящее в смету строек"'
                )
                sheetname = "Стт_" + str(date1.year) + "_" + "ОНСС_" + fy
                toexcelf(dfs_1, sheetname)
                gl_back_addr[sheetname] = ii + str(endrow + 2)
                ws_stsat.write_url(
                    gl_back_addr[sheetname], "internal:'" + sheetname + "'!A1"
                )

            # вывод расшифровки на "Все_xx->xy"
            if ii == "G":
                # zayavka22 = gl_filters[int(gl_settings["Период"][startyear])]
                dfs_1 = df.query(
                    "`/ САП`==@zayavka22 & СРОК >= @date3 & СРОК <= @date4"
                )
                sheetname = "Стт_" + str(date1.year) + "_" + fy + sy
                toexcelf(dfs_1, sheetname)
                gl_back_addr[sheetname] = ii + str(endrow + 2)
                ws_stsat.write_url(
                    gl_back_addr[sheetname], "internal:'" + sheetname + "'!A1"
                )

            # вывод расшифровки на "Все_МТР_в_xx"
            # сборка списка заявок от текущего года до начального
            if ii == "H":
                for yn in range(startyear, 0, -1):
                    zayavka22 = (
                        zayavka22 + gl_filters[int(gl_settings["Период"][yn - 1])]
                    )

                date1 = date(int(gl_settings["Период"][startyear]), 1, 1)
                dfs_1 = df.query(
                    '`/ САП`==@zayavka22  & `Вид деятельности` == "Основная деятельность" & СРОК >= @date1'
                )
                sheetname = "Стт_" + str(date1.year) + "_Все_МТР_в_" + fy
                toexcelf(dfs_1, sheetname)
                gl_back_addr[sheetname] = ii + str(endrow + 2)
                ws_stsat.write_url(
                    gl_back_addr[sheetname], "internal:'" + sheetname + "'!A1"
                )

                dfs_1 = None

            ws_stsat.write_formula(
                ii + str(endrow + 2),
                "=SUM(" + ii + str(strow + 2) + ":" + ii + str(endrow + 1) + ")",
                gl_style["gl_style_moneyBr"],
            )
    endrow += 2

    # вывод помесячной статистики
    out = []  # список для ОНСС
    out_m = []  # список для материалов
    outh = [
        "январь",
        "февраль",
        "март",
        "апрель",
        "май",
        "июнь",
        "июль",
        "август",
        "сентябрь",
        "октябрь",
        "ноябрь",
        "декабрь",
    ]
    # год по которому делается отчет
    year = int(gl_settings["Период"][startyear])
    dfsum, dfsum_m = pd.DataFrame(), pd.DataFrame()
    for i in range(1, 13):
        dfsum = (
            df.loc[
                (df["Вид деятельности"] == "Оборудование не входящее в смету строек")
                & (df["СРОК"].dt.month == i)
                & (df["СРОК"].dt.year == year)
            ]["Сумма"].sum()
            / 1000
        )
        dfsum_m = (
            df.loc[
                (df["Вид деятельности"] == "Основная деятельность")
                & (df["СРОК"].dt.month == i)
                & (df["СРОК"].dt.year == year)
            ]["Сумма"].sum()
            / 1000
        )

        dopo = (
            gl_filtersdf[
                (gl_filtersdf["item"] == "поставка без заявок месяц")
                & (gl_filtersdf["year_d"].dt.year == year)
                & (gl_filtersdf["year_d"].dt.month == i)
                & (gl_filtersdf["toonss"] == "ОНСС")
            ]["sap"].sum()
            / 1000
        )
        dopm = (
            gl_filtersdf[
                (gl_filtersdf["item"] == "поставка без заявок месяц")
                & (gl_filtersdf["year_d"].dt.year == year)
                & (gl_filtersdf["year_d"].dt.month == i)
                & (gl_filtersdf["toonss"] == "МТР")
            ]["sap"].sum()
            / 1000
        )
        if dopm > 0:
            out_m.append("=" + str(dfsum_m) + "+" + str(dopm))
        else:
            out_m.append(dfsum_m)

        if dopo > 0:
            out.append("=" + str(dfsum) + "+" + str(dopo))
        else:
            out.append(dfsum)

    ws_stsat.write_row(
        strow + 1, 11, outh, gl_style["gl_style_cell_format_bold_border"]
    )
    ws_stsat.write_row(strow + 2, 11, out, gl_style["gl_style_moneyB"])
    ws_stsat.write_row(strow + 4, 11, out_m, gl_style["gl_style_moneyB"])
    ws_stsat.write(strow, 10, "Тыс.р. с НДС")
    ws_stsat.write(strow - 1, 23, "Суммы")
    ws_stsat.write(strow + 2, 10, "ОНСС", gl_style["gl_style_moneyB"])
    ws_stsat.write(strow + 4, 10, "МТР", gl_style["gl_style_moneyB"])

    for i in range(ord("L"), ord("Y")):
        ii = chr(i)
        ws_stsat.write_formula(
            ii + str(strow + 1),
            "=SUM(" + ii + str(strow + 3) + ":" + ii + str(strow + 5) + ")",
            gl_style["gl_style_moneyBr"],
        )

    #   вывод годовых листов, сумм по году и ссылок
    # BUG что делать с проектами ? выводить?
    out_year = df[
        (df["Вид деятельности"] == "Оборудование не входящее в смету строек")
        & (df["СРОК"].dt.year == year)
    ]
    sheetname = "Год_" + str(year) + "_ОНСС"
    toexcelf(out_year, sheetname)
    gl_back_addr[sheetname] = "X" + str(strow + 3)
    ws_stsat.write_url(gl_back_addr[sheetname], "internal:'" + sheetname + "'!A1")
    ws_stsat.write_formula(
        gl_back_addr[sheetname],
        "=SUM(L" + str(strow + 3) + ":W" + str(strow + 3) + ")",
        gl_style["gl_style_moneyBr"],
    )

    out_year = df[
        (df["Вид деятельности"] == "Основная деятельность")
        & (df["СРОК"].dt.year == year)
    ]
    sheetname = "Год_" + str(year) + "_МТР"
    toexcelf(out_year, sheetname)
    gl_back_addr[sheetname] = "X" + str(strow + 5)
    ws_stsat.write_url(gl_back_addr[sheetname], "internal:'" + sheetname + "'!A1")
    ws_stsat.write_formula(
        gl_back_addr[sheetname],
        "=SUM(L" + str(strow + 5) + ":W" + str(strow + 5) + ")",
        gl_style["gl_style_moneyBr"],
    )
    out_year = None

    if dfs.shape[0] < 2:
        endrow += 3  # доп отсуп если высота статусов меньше 2

    # признак наличия колонки "плохой" для расчета формулы с плановыми заявками
    bad_column = 0

    if dfs.shape[0] < 2:
        endrow += 3  # доп отсуп если высота статусов меньше 2
    endrow += 2  # отсуп перед следующим блоком

    out_bgt = pd.DataFrame(columns=outh + ["Отдел", "Тип"])  # фрейм для бюджетов

    # сборка фрейма с бюджетами помесячно
    # цикл по бюджетам из gl_filtersdf

    if len(gl_filtersdf["otdel"].unique()) > 1:
        # если отделов больше одного сложная сортировка по отделу и типу бюджета
        rdf_b = gl_filtersdf[gl_filtersdf["item"] == "бюджетс"][["toonss", "bgt"]]
        rdf_l = gl_filtersdf[
            ((gl_filtersdf["item"] == "заявка") | (gl_filtersdf["item"] == "бюджетс"))
            & (gl_filtersdf["toonss"] != "КОНТРОЛЬ")
        ].drop_duplicates(subset=["bgt"])
        rdf_lc = (
            pd.merge(rdf_l, rdf_b, how="left", left_on=["bgt"], right_on=["bgt"])
            .sort_values(by=["otdel", "toonss_y"])["bgt"]
            .unique()
        )
        rdf_l = None
        rdf_b = None
    else:
        rdf_lc = (
            gl_filtersdf[
                (
                    (gl_filtersdf["item"] == "заявка")
                    | (gl_filtersdf["item"] == "бюджетс")
                )
                & (gl_filtersdf["toonss"] != "КОНТРОЛЬ")
            ]["bgt"]
            .sort_values()
            .unique()
        )

    for ii in rdf_lc:
        # (
        # gl_filtersdf[((gl_filtersdf["item"] == "заявка") | (gl_filtersdf["item"] == "бюджетс"))& (gl_filtersdf["toonss"] != "КОНТРОЛЬ")]["bgt"].sort_values().unique()
        # ):
        summb = (
            gl_filtersdf[
                (gl_filtersdf["item"] == "бюджетс")
                & (gl_filtersdf["bgt"] == ii)
                & (gl_filtersdf["year"] == year)
            ]["sap"].sum()
            / 1000
        )
        typeb = gl_filtersdf[
            (gl_filtersdf["item"] == "бюджетс")
            & (gl_filtersdf["bgt"] == ii)
            & (gl_filtersdf["year"] == year)
        ]["toonss"]
        zza = gl_filtersdf.query('`item`=="заявка" & `bgt`==@ii')["sap"].to_list()
        yearsum = df.loc[(df["/ САП"].isin(zza)) & (df["СРОК"].dt.year == year)][
            "Сумма"
        ].sum()
        yearsum_prj = gl_df_project.loc[
            (gl_df_project["/ САП"].isin(zza))
            & (gl_df_project["Плановая дата поставки"].dt.year == year)
        ]["Сумма"].sum()

        # Вычисление пустых бюджет-строк.
        # план сумма бюджета на год=0, тип бюджета не установлен, сумма заявок на год=0
        # if (summb != 0) or (len(typeb) == 1) or (yearsum != 0) or (yearsum_prj != 0):
        if (summb != 0) or (yearsum != 0) or (yearsum_prj != 0):
            # добавление суммы бюджета
            out_bgt.loc[ii, "Бизнесплан"] = summb

            # Вывод суммы переходящих заявок
            if startyear == 1:
                # для первого года переходящие это заявки с нулевого года
                year_from = int(gl_settings["Период"][0])  # noqa: F841
                zza_per = gl_filtersdf.query(
                    '`item`=="заявка" & `bgt`==@ii & `year`==@year_from'
                )["sap"].to_list()
                msum = (
                    df.loc[(df["/ САП"].isin(zza_per)) & (df["СРОК"].dt.year == year)][
                        "Сумма"
                    ].sum()
                    / 1000
                )
            # elif startyear==2:
            else:
                # сборка списка из предыдущих годов с которых может перейти заявка
                yearl = []
                for yl in range(0, startyear):
                    yearl.append(int(gl_settings["Период"][yl]))
                # year_from = int(gl_settings["Период"][0])  # noqa: F841
                # year_from1 = int(gl_settings["Период"][1])  # noqa: F841
                # zza_per = gl_filtersdf.query(
                #     '`item`=="заявка" & `bgt`==@ii & (`year`==@year_from | `year`==@year_from1)'
                # )["sap"].to_list()
                zza_per = gl_filtersdf.query(
                    '`item`=="заявка" & `bgt`==@ii & (`year`==@yearl)'
                )["sap"].to_list()

                msum = (
                    df.loc[(df["/ САП"].isin(zza_per)) & (df["СРОК"].dt.year == year)][
                        "Сумма"
                    ].sum()
                    / 1000
                )
            out_bgt.loc[ii, "Переходящие"] = msum
            # вывод нулей, для фиксации расположения колонки
            out_bgt.loc[ii, "Плановые"] = 0

            otd = gl_filtersdf[
                (gl_filtersdf["item"] == "бюджетс")
                & (gl_filtersdf["bgt"] == ii)
                & (gl_filtersdf["year"] == year)
            ]["otdel"]
            if len(otd) == 1:
                out_bgt.loc[ii, "Отдел"] = otd.values[0]
            else:
                otd = gl_filtersdf[
                    (gl_filtersdf["item"] == "бюджетс")
                    & (gl_filtersdf["bgt"] == ii)
                    & (gl_filtersdf["year"] == year - 1)
                ]["otdel"]
                if len(otd) == 1:
                    out_bgt.loc[ii, "Отдел"] = otd.values[0]
                else:
                    out_bgt.loc[ii, "Отдел"] = "не определено"

            # добавление типа бюджета
            if len(typeb) == 1:
                out_bgt.loc[ii, "Тип"] = typeb.values[0]
            else:
                out_bgt.loc[ii, "Тип"] = "не задан"

            # расчет номера строки для обратной ссылки
            out_bgt["idx"] = range(1, len(out_bgt) + 1)
            # переделать на arrow после перехода
            # df['new_col'] = np.arange(1, df.shape[0] + 1)

            for i in range(1, 13):
                msum = (
                    df.loc[
                        (df["/ САП"].isin(zza))
                        & (df["СРОК"].dt.month == i)
                        & (df["СРОК"].dt.year == year)
                    ]["Сумма"].sum()
                    / 1000
                )

                if yearsum_prj != 0:
                    msum_prj = (
                        gl_df_project.loc[
                            (gl_df_project["/ САП"].isin(zza))
                            & (gl_df_project["Плановая дата поставки"].dt.month == i)
                            & (gl_df_project["Плановая дата поставки"].dt.year == year)
                        ]["Сумма"].sum()
                        / 1000
                    )
                else:
                    msum_prj = 0

                if (msum == 0) and (msum_prj == 0):
                    out_bgt.loc[ii, outh[i - 1]] = msum
                else:
                    # вывод расшифровки суммы в отдельный лист
                    # сборка адресов для обратной навигации
                    sheetname = "Сум_" + str(ii) + "_" + str(year) + "_" + str(i)
                    out_bgt.loc[ii, outh[i - 1]] = (
                        "=HYPERLINK(\"#'"
                        + sheetname
                        + "'!A1\", "
                        + str(msum + msum_prj)
                        + ")"
                    )
                    dfsumm = df[
                        (df["/ САП"].isin(zza))
                        & (df["СРОК"].dt.month == i)
                        & (df["СРОК"].dt.year == year)
                    ]

                    if msum_prj != 0:
                        dfsumm_prj = gl_df_project[
                            (gl_df_project["/ САП"].isin(zza))
                            & (gl_df_project["Плановая дата поставки"].dt.month == i)
                            & (gl_df_project["Плановая дата поставки"].dt.year == year)
                        ]
                        dfsumm = pd.concat([dfsumm, dfsumm_prj])

                    toexcelf(dfsumm, sheetname)
                    iind = out_bgt.loc[ii]["idx"]
                    gl_back_addr[sheetname] = chr(ord("C") + i) + str(endrow + 1 + iind)

            # вывод фрейм-фильтров по строкам бюджета на отдельные листы. только не в год №0
            # выводим только не бюджеты не попадающие под сброс
            dff = df[(df["/ САП"].isin(zza)) & (df["СРОК"].dt.year == year)]
            if yearsum_prj != 0:
                dff_prj = gl_df_project[
                    (gl_df_project["/ САП"].isin(zza))
                    & (gl_df_project["Плановая дата поставки"].dt.year == year)
                ]
                dff = pd.concat([dff, dff_prj])

            sheetname = "Стр_" + str(ii) + "_" + str(year)
            toexcelf(dff, sheetname)

            # проверка содержимого бюджета на однотипность заявок
            bfgf = gl_filtersdf.loc[
                (gl_filtersdf["item"] == "заявка") & (gl_filtersdf["bgt"] == ii)
            ]
            bfdf = df[
                (df["/ САП"].isin(bfgf["sap"])) & (df["СРОК"].dt.year == year)
            ].drop_duplicates(subset=["Вид деятельности"])
            if len(bfdf.index) > 1:
                out_bgt.loc[ii, "плохой"] = "Есть рекласс"
                bad_column = 1

    # добавление в итоговый фрейм сумм поставок без заявок в виде формул
    # BUG если будет добавка в пустой бюджет-строку ругнемся и потеряем бюджет-строку.
    # возможно стоит перебросить в условие фильтрации бюджетов
    dopo = gl_filtersdf[
        (gl_filtersdf["item"] == "поставка без заявок месяц")
        & (gl_filtersdf["year_d"].dt.year == year)
    ]
    # dopo.reset_index(drop=True)
    for index, ii in dopo.iterrows():
        if ii["bgt"] in out_bgt.index:
            oldres = str(out_bgt.loc[ii["bgt"], outh[ii["year_d"].month - 1]])
            if oldres.startswith("=H"):
                out_bgt.loc[ii["bgt"], outh[ii["year_d"].month - 1]] = (
                    oldres[:-1] + "+" + str(ii["sap"] / 1000) + ")"
                )
            else:
                if oldres.startswith("="):
                    out_bgt.loc[ii["bgt"], outh[ii["year_d"].month - 1]] = (
                        oldres + "+" + str(ii["sap"] / 1000)
                    )
                else:
                    out_bgt.loc[ii["bgt"], outh[ii["year_d"].month - 1]] = (
                        "=" + oldres + "+" + str(ii["sap"] / 1000)
                    )
        else:
            print(
                f"Ошибка! Попытка добавить в пустую бюджет-строку {ii['bgt']}, {ii['year_d']}"
            )

    # добавление на фрейм колонки с остатком. заменено на вывод формулы
    # out_bgt['Остаток'] = out_bgt['Бюджет'] - out_bgt.iloc[:, 0:12].sum(axis=1)

    # удаление служебной колонки
    if "idx" in out_bgt.columns:
        out_bgt.drop(["idx"], axis=1, inplace=True)

    # перемещение колонок влево

    if "Отдел" not in out_bgt.columns:
        out_bgt["Отдел"] = ""
        out_bgt["Тип"] = ""

    cols_to_move = ["Отдел", "Тип"]
    out_bgt = out_bgt[
        cols_to_move + [x for x in out_bgt.columns if x not in cols_to_move]
    ]

    out_bgt.to_excel(gl_writer, sheet_name="Статистика", startrow=endrow, startcol=0)

    # вычисление последней буквы в таблице
    crow = ord("B") + out_bgt.shape[1] + 1

    # Вывод итогов по колонкам
    for i in range(ord("D"), ord("D") + out_bgt.shape[1]):
        ii = chr(i)
        ws_stsat.write_formula(
            ii + str(endrow),
            "=SUBTOTAL(9,"
            + ii
            + str(endrow + 2)
            + ":"
            + ii
            + str(endrow + 1 + out_bgt.shape[0])
            + ")",
            gl_style["gl_style_moneyBr"],
        )

    ws_stsat.write_string("B" + str(endrow), "Тыс.р. с НДС")

    # Вывод формул итогов по строкам
    for i in range(endrow + 2, endrow + out_bgt.shape[0] + 2):
        # вывод формулы суммы строки
        ws_stsat.write_formula(
            chr(crow - 1) + str(i),
            "=SUM(D" + str(i) + ":O" + str(i) + ")",
            gl_style["gl_style_moneyBr"],
        )
        # вывод формулы остатка на строке
        ws_stsat.write_formula(
            chr(crow) + str(i),
            "=P" + str(i) + "-" + chr(crow - 1) + str(i),
            gl_style["gl_style_moneyBr"],
        )
        # вывод формул плановых сумм заявок
        ws_stsat.write_formula(
            chr(crow - 2 - bad_column) + str(i),
            "=" + chr(crow - 1) + str(i) + "-" + chr(crow - 3 - bad_column) + str(i),
            gl_style["gl_style_moneyBr"],
        )

        # вывод гиперссылок на листы с бюджетами
        sheetname = "Стр_" + str(out_bgt.index[i - endrow - 2]) + "_" + str(year)
        gl_back_addr[sheetname] = "A" + str(i)
        ws_stsat.write_url(
            "A" + str(i),
            "internal:'" + sheetname + "'!A1",
            string=str(out_bgt.index[i - endrow - 2]),
            cell_format=gl_style["gl_style_hl"],
        )
    # TODO добавить вывод формул суммирования по отделу и типу бюджета =СУММЕСЛИМН()

    # подготовка заголовков и форматирование в таблицу
    column_settings = (
        [{"header": "строка"}]
        + [{"header": column} for column in out_bgt.columns]
        + [{"header": "Сумма стр."}, {"header": "Остаток стр."}]
    )
    namet = "Table" + gl_settings["Период"][startyear]
    ws_stsat.add_table(
        "A" + str(endrow + 1) + ":" + chr(crow) + str(endrow + out_bgt.shape[0] + 1),
        {
            "columns": column_settings,
            "style": "Table Style Medium 9",
            "name": namet,
        },
    )

    # вывод заголовков и формул суммирования групп
    ws_stsat.write_string(
        chr(crow + 2) + str(endrow + 1),
        "Сумма тип",
        gl_style["gl_style_cell_format_bold_border"],
    )
    ws_stsat.write_string(
        chr(crow + 3) + str(endrow + 1),
        "Отдел",
        gl_style["gl_style_cell_format_bold_border"],
    )
    ws_stsat.write_string(
        chr(crow + 4) + str(endrow + 1),
        "Тип",
        gl_style["gl_style_cell_format_bold_border"],
    )

    df_vars = (
        out_bgt[["Отдел", "Тип"]]
        .drop_duplicates(subset=["Отдел", "Тип"])
        .sort_values(by="Тип")
    )
    df_vars.to_excel(
        gl_writer,
        sheet_name="Статистика",
        startrow=endrow + 1,
        startcol=out_bgt.shape[1] + 5,
        header=False,
        index=False,
    )
    for i in range(endrow + 2, endrow + 2 + len(df_vars.index)):
        ws_stsat.write_formula(
            chr(crow + 2) + str(i),
            f"=SUMIFS({namet}[Остаток стр.],{namet}[Отдел],"
            + chr(crow + 3)
            + str(i)
            + f",{namet}[Тип],"
            + chr(crow + 4)
            + str(i)
            + ")",
            gl_style["gl_style_moneyBr"],
        )

    # установить стиль для таблицы с бюджетами
    for i in range(endrow, endrow + out_bgt.shape[0] + 1):
        ws_stsat.set_row(i, None, gl_style["gl_style_moneyB"])

    endrow = endrow + out_bgt.shape[0] + 1
    endrow += 2
    return endrow


def mfunc3(inpt):
    """раскладывание прогноза по списанию для строки фрейма"""
    m1 = inpt["m1"]
    l_today = pd.to_datetime("today")
    raspr = inpt["Кол-во АС расч."]
    if raspr == 0:
        raspr = inpt["Кол-во в ЕИ"]

    mon = 0
    if m1 <= 0:
        # время списания прошло. всё в текущий месяц
        inpt[l_today.strftime("%Y-%m")] = raspr
    else:
        if inpt["СРОК"] > l_today:
            mnth_shift = inpt["СРОК"]
        else:
            mnth_shift = l_today

        while raspr > 0:
            if m1 > 0:
                cum = ceil(raspr / (m1 - mon))
            else:
                cum = raspr

            dsp = (mnth_shift + pd.offsets.DateOffset(months=mon)).strftime("%Y-%m")
            inpt[dsp] = cum
            if inpt["Кол-во АС расч."] == 0:
                inpt["summ_" + dsp] = inpt["Сумма"] / inpt["Кол-во в ЕИ"] * cum
            else:
                inpt["summ_" + dsp] = inpt["ismtr"] * cum

            raspr -= cum
            mon += 1

    return inpt


def prognoz(in_dfs: pd.DataFrame, startd=2024) -> pd.DataFrame:
    """возвращает фрейм с раскладом по датам списания для прогноза
    in_dfs - фрейм для фильтрации, startd - заявки с меньшим годом планирования будут сброшены"""
    l_dfs = in_dfs.copy()
    goodcols = [
        "/ САП",
        "Наименование заявки",
        "Вид деятельности",
        "Наименование службы заказчика",
        "Наименование заявителя",
        "Материал",
        "Класс",
        "Полное имя материала",
        "Кол-во в ЕИ",
        "Кол-во АС расч.",
        "СРОК",
        "ismtr",
        "Сумма",
        "year",
        "bgt",
    ]

    l_dfs = l_dfs[goodcols]
    l_dfs["year"] = l_dfs["year"].astype("int32")

    # скинуть позиции раньше старт-года
    mask = l_dfs["year"] < startd
    l_dfs.drop(l_dfs[mask].index, inplace=True)

    l_dfs["КрайДат"] = l_dfs["СРОК"] + pd.offsets.DateOffset(years=1)

    l_dfs["m1"] = l_dfs["КрайДат"].dt.to_period("M").astype("int64") - pd.to_datetime(
        ["today"]
    ).to_period("M").astype("int64")

    mask = l_dfs["m1"] > 12
    l_dfs.loc[mask, "m1"] = 12
    l_dfs["Кол-во АС расч."] = l_dfs["Кол-во АС расч."].fillna(0)
    l_dfs = l_dfs.apply(mfunc3, axis=1)

    cols_to_move_l = goodcols + ["КрайДат", "m1"]
    l_dfs = l_dfs[
        cols_to_move_l + [x for x in l_dfs.columns if x not in cols_to_move_l]
    ]

    return l_dfs


def report_proto():
    """Вывод листа анализа протоколов"""
    global gl_writer
    global gl_df
    workbook = gl_writer.book  # type: ignore
    ws_stsat = workbook.get_worksheet_by_name("Протоколы")

    date1 = date(int(gl_settings["Период"][1]), 1, 1)  # noqa: F841
    year = int(gl_settings["Период"][1])

    # выборка заявок в которых есть запись в поле протокол
    strow = 2
    zzi = gl_filtersdf[
        (gl_filtersdf["item"] == "заявка") & (gl_filtersdf["protcl"] != "Не указан")
    ]["sap"].to_list()
    dfi = gl_df.query("`/ САП`==@zzi & СРОК >= @date1")

    dfi2 = pd.merge(dfi, gl_filtersdf, how="left", left_on=["/ САП"], right_on=["sap"])

    if len(gl_df_cmtr.index > 3):
        dfi2 = pd.merge(
            dfi2, gl_df_cmtr, how="left", left_on=["Класс"], right_on=["class"]
        )
        dfi2.drop(["item", "sap", "year_d", "otdel", "class"], axis=1, inplace=True)
    else:
        dfi2.drop(["item", "sap", "year_d", "otdel"], axis=1, inplace=True)

    dfi2.rename(columns={"protcl": "Протокол", "protcl_d": "Дата п."}, inplace=True)
    sheetname = "Про_" + str(year) + "_Есть"
    dfi2.to_excel(gl_writer, sheet_name=sheetname, index=False)
    gl_back_addr[sheetname] = "A" + str(strow)
    ws_stsat.write_url(
        gl_back_addr[sheetname],
        "internal:'" + sheetname + "'!A1",
        string="Заявки с протоколами ",
    )
    # выборка строк с протоколом где его быть не должно
    strow += 2

    dfi2 = dfi2[(dfi2["Протокол"] != "В исключениях") & (dfi2["level"] != 1)]
    sheetname = "Про_" + str(year) + "_Есть_ненадо"
    dfi2.to_excel(gl_writer, sheet_name=sheetname, index=False)
    gl_back_addr[sheetname] = "A" + str(strow)
    ws_stsat.write_url(
        gl_back_addr[sheetname],
        "internal:'" + sheetname + "'!A1",
        string="Заявки с протоколами но его быть не должно(!=1) ",
    )

    # выборка строк с протоколом "в исключениях" но он должен быть
    strow += 2
    dfi2 = dfi2[(dfi2["Протокол"] == "В исключениях") & (dfi2["level"] == 1)]
    sheetname = "Про_" + str(year) + "_Искл_надо"
    dfi2.to_excel(gl_writer, sheet_name=sheetname, index=False)
    gl_back_addr[sheetname] = "A" + str(strow)
    ws_stsat.write_url(
        gl_back_addr[sheetname],
        "internal:'" + sheetname + "'!A1",
        string='Заявки с протоколами "В исключениях" но он должен быть (==1) ',
    )

    # выборка просроченных протоколов срок действия меньше прогнозного срока поставки
    strow += 2
    zzd = gl_filtersdf[
        (gl_filtersdf["item"] == "заявка") & (gl_filtersdf["protcl"] != "Не указан")
    ].copy()
    # zzd.loc[:, "protcl_d"] = pd.to_datetime(zzd["protcl_d"], errors="coerce")
    zzd["protcl_d"] = pd.to_datetime(zzd["protcl_d"], errors="coerce")
    zzd = zzd.dropna(subset=["protcl_d"])
    zzi = zzd["sap"].to_list()
    zzd = None
    dfi = gl_df.query("`/ САП`==@zzi & СРОК >= @date1")
    dfi2 = pd.merge(dfi, gl_filtersdf, how="left", left_on=["/ САП"], right_on=["sap"])

    # сброс строк дубликатов в которых нет даты
    # dfi2.loc[:, "protcl_d"] = pd.to_datetime(dfi2["protcl_d"], errors="coerce")
    dfi2["protcl_d"] = pd.to_datetime(dfi2["protcl_d"], errors="coerce")
    dfi2 = dfi2.dropna(subset=["protcl_d"])

    if len(gl_df_cmtr.index > 3):
        dfi2 = pd.merge(
            dfi2, gl_df_cmtr, how="left", left_on=["Класс"], right_on=["class"]
        )
        dfi2.drop(["item", "sap", "year_d", "otdel", "class"], axis=1, inplace=True)
    else:
        dfi2.drop(["item", "sap", "year_d", "otdel"], axis=1, inplace=True)

    dfi2["protcl_d2"] = dfi2["protcl_d"] + pd.offsets.DateOffset(years=2)  # type: ignore
    dfi2.drop(dfi2[dfi2["СРОК"] < dfi2["protcl_d2"]].index, inplace=True)
    dfi2.rename(
        columns={"protcl": "Протокол", "protcl_d": "Дата п.", "protcl_d2": "Срок д.п."},
        inplace=True,
    )
    sheetname = "Про_" + str(year) + "_Плохой"
    dfi2.to_excel(gl_writer, sheet_name=sheetname, index=False)

    gl_back_addr[sheetname] = "A" + str(strow)
    ws_stsat.write_url(
        gl_back_addr[sheetname],
        "internal:'" + sheetname + "'!A1",
        string="Заявки с просроченными протоколами ",
    )

    # выборка заявок без протоколов
    strow += 2
    zzi = gl_filtersdf[
        (gl_filtersdf["item"] == "заявка") & (gl_filtersdf["protcl"] == "Не указан")
    ]["sap"].to_list()
    dfi = gl_df.query("`/ САП`==@zzi & СРОК >= @date1")
    dfi2 = pd.merge(dfi, gl_filtersdf, how="left", left_on=["/ САП"], right_on=["sap"])

    if len(gl_df_cmtr.index > 3):
        dfi2 = pd.merge(
            dfi2, gl_df_cmtr, how="left", left_on=["Класс"], right_on=["class"]
        )
        dfi2.drop(["item", "sap", "year_d", "otdel", "class"], axis=1, inplace=True)
    else:
        dfi2.drop(["item", "sap", "year_d", "otdel"], axis=1, inplace=True)

    dfi2.rename(columns={"protcl": "Протокол", "protcl_d": "Дата п."}, inplace=True)

    sheetname = "Про_" + str(year) + "_Нет"
    dfi2.to_excel(gl_writer, sheet_name=sheetname, index=False)
    gl_back_addr[sheetname] = "A" + str(strow)
    ws_stsat.write_url(
        gl_back_addr[sheetname],
        "internal:'" + sheetname + "'!A1",
        string="Заявки без протокола",
    )

    # выборка строк без протокола но он должен быть
    strow += 2
    dfi2 = dfi2[dfi2["level"] == 1]
    sheetname = "Про_" + str(year) + "_Нет_надо"
    dfi2.to_excel(gl_writer, sheet_name=sheetname, index=False)
    gl_back_addr[sheetname] = "A" + str(strow)
    ws_stsat.write_url(
        gl_back_addr[sheetname],
        "internal:'" + sheetname + "'!A1",
        string="Заявки без протокола но он должен быть (==1) ",
    )

    # выборка все заявки + протоколы
    strow += 2
    zzi = gl_filtersdf[gl_filtersdf["item"] == "заявка"]["sap"].to_list()  # noqa: F841
    # dfi = gl_df.query("`/ САП`==@zzi & СРОК >= @date1")
    dfi = gl_df[gl_df["/ САП"].isin(zzi)]
    dfi2 = pd.merge(dfi, gl_filtersdf, how="left", left_on=["/ САП"], right_on=["sap"])

    if len(gl_df_cmtr.index > 3):
        dfi2 = pd.merge(
            dfi2, gl_df_cmtr, how="left", left_on=["Класс"], right_on=["class"]
        )
        dfi2.drop(["item", "sap", "year_d", "otdel", "class"], axis=1, inplace=True)
    else:
        dfi2.drop(["item", "sap", "year_d", "otdel"], axis=1, inplace=True)

    dfi2.rename(columns={"protcl": "Протокол", "protcl_d": "Дата п."}, inplace=True)

    sheetname = "Про_" + str(year) + "_Все"
    dfi2.to_excel(gl_writer, sheet_name=sheetname, index=False)
    gl_back_addr[sheetname] = "A" + str(strow)
    ws_stsat.write_url(
        gl_back_addr[sheetname],
        "internal:'" + sheetname + "'!A1",
        string="Все заявки",
    )

    # формирование и вывод страницы прогноза списания
    strow += 2
    sheetname = "План_" + str(pd.to_datetime("today").year) + "_Списания"
    dfis = prognoz(dfi2, pd.to_datetime("today").year)
    dfis.to_excel(gl_writer, sheet_name=sheetname, index=False)
    gl_back_addr[sheetname] = "A" + str(strow)
    ws_stsat.write_url(
        gl_back_addr[sheetname],
        "internal:'" + sheetname + "'!A1",
        string="План списания",
    )
    strow += 2

    # вывод сводной
    coltosumm = [e for e in dfis.columns if e[:5] == "summ_"]
    df_fsv = dfis.copy()
    df_fsv = pd.pivot_table(
        df_fsv,
        index=["Вид деятельности", "Наименование службы заказчика", "bgt"],
        values=coltosumm,
        aggfunc="sum",
    ).reset_index()
    sheetname = "Суммы_списания_" + str(pd.to_datetime("today").year)
    df_fsv.to_excel(gl_writer, sheet_name=sheetname, index=False)
    gl_back_addr[sheetname] = "A" + str(strow)
    ws_stsat.write_url(
        gl_back_addr[sheetname],
        "internal:'" + sheetname + "'!A1",
        string="Суммы списания",
    )
    df_fsv = None

    return 0


def report_pto():
    """Вывод листа заготовка для ПТО"""
    global gl_writer
    global gl_df
    workbook = gl_writer.book  # type: ignore
    ws_stsat = workbook.get_worksheet_by_name("ПТО")

    date1 = date(int(gl_settings["Период"][1]), 1, 1)  # noqa: F841
    # year = int(gl_settings["Период"][1])

    # выборка заявок для отчета ПТО
    # онсс, не поставлено, не проект, срок текущий год (1) и далее

    strow = 2
    zzi = gl_filtersdf[gl_filtersdf["item"] == "заявка"]["sap"].to_list()  # noqa: F841
    dfi = gl_df.query(
        '`/ САП`==@zzi & `Вид деятельности` == "Оборудование не входящее в смету строек" & СРОК >= @date1 & Статус != "13. Поставлено"'
    )

    dfi2 = pd.merge(
        dfi,
        gl_filtersdf[gl_filtersdf["item"] == "заявка"],
        how="left",
        left_on=["/ САП"],
        right_on=["sap"],
    )

    # сброс лишних колонок
    dfi2.drop(
        [
            "item",
            "sap",
            "year_d",
            "Класс",
            "Исполнитель закупки",
            "Поставщик",
            "Номер дог. документа",
            "Номер НПП",
            "Позиция НПП",
            "Номер лота из отчета по лотам",
            "Договор 1С",
            "ismtr",
            "toonss",
            # "Кол-во АС расч.",
            "Кол-во по сводной заявке плановое",
            "Статус",
            "Плановая дата поставки",
            "Дата Утверждения  НПП",
            "Доп. данные о материале",
        ],
        axis=1,
        inplace=True,
    )

    dfi2["Цена за единицу"] = dfi2["Цена, руб. с НДС"] / 1.2 / 1000
    dfi2["Стоимость, тыс. руб. с НДС"] = dfi2["Сумма, руб. с НДС"] / 1000

    # переписываем плановые суммы фактическими
    mask = dfi2["Кол-во АС расч."] > 0
    dfi2.loc[mask, "Цена за единицу"] = (
        dfi2.loc[mask, "Сумма"] / dfi2.loc[mask, "Кол-во АС расч."] / 1.2 / 1000
    )
    dfi2.loc[mask, "Стоимость, тыс. руб. с НДС"] = dfi2.loc[mask, "Сумма"] / 1000

    dfi2["Стоимость, тыс. руб. без НДС"] = dfi2["Стоимость, тыс. руб. с НДС"] / 1.2

    dfi2["Номер заявки SAP"] = dfi2["/ САП"].str[:10]
    dfi2["Позиция заявки SAP"] = (
        dfi2["/ САП"].str.split(pat="/", n=1, expand=True)[1].str.strip()
    )
    # вариант ручного определения имени месяца
    # look_up = {'01': 'Jan', '02': 'Feb', '03': 'Mar', '04': 'Apr', '05': 'May',
    #         '06': 'Jun', '07': 'Jul', '08': 'Aug', '09': 'Sep', '10': 'Oct', '11': 'Nov', '12': 'Dec'}
    # df['Month'] = df['Month'].apply(lambda x: look_up[x])

    look_up = {
        1: "Январь",
        2: "Февраль",
        3: "Март",
        4: "Апрель",
        5: "Май",
        6: "Июрь",
        7: "Июль",
        8: "Август",
        9: "Сентябрь",
        10: "Октябрь",
        11: "Ноябрь",
        12: "Декабрь",
    }
    dfi2["месяц освоения"] = dfi2["СРОК"].dt.month.apply(lambda x: look_up[x])

    # dfi2["месяц освоения"] = dfi2["СРОК"].dt.month_name(locale="Russian")

    dfi2["месяц ввода (перевод в состав ОС)"] = (
        dfi2["СРОК"] + pd.DateOffset(months=1)
    ).dt.month.apply(lambda x: look_up[x])

    # dfi2["месяц ввода (перевод в состав ОС)"] = (
    #     dfi2["СРОК"] + pd.DateOffset(months=1)
    # ).dt.month_name(locale="Russian")

    dfi2["месяц финансирования"] = (
        dfi2["СРОК"] + pd.DateOffset(months=2)
    ).dt.month.apply(lambda x: look_up[x])

    # dfi2["месяц финансирования"] = (
    #     dfi2["СРОК"] + pd.DateOffset(months=2)
    # ).dt.month_name(locale="Russian")

    # перемещение колонок влево
    cols_to_move = [
        "year",
        "Материал",
        "Полное имя материала",
        "Цена за единицу",
        "Стоимость, тыс. руб. с НДС",
        "Сумма, руб. с НДС",
        "Стоимость, тыс. руб. без НДС",
        "Кол-во в ЕИ",
        "ЕИ ввода",
        "Номер заявки SAP",
        "Позиция заявки SAP",
        "месяц освоения",
        "месяц финансирования",
        "месяц ввода (перевод в состав ОС)",
        "bgt",
    ]
    dfi2 = dfi2[cols_to_move + [x for x in dfi2.columns if x not in cols_to_move]]

    dfi2 = dfi2.rename(
        columns={
            "year": "Год",
            "Материал": "КСМ",
            "Полное имя материала": "Наименование",
            "Кол-во в ЕИ": "Количество",
            "ЕИ ввода": "Изм",
            "bgt": "21р",
        }
    )

    dfi2.drop(["Сумма, руб. с НДС"], axis=1, inplace=True)

    sheetname = "Пто_" + "Заготовка"
    dfi2.to_excel(gl_writer, sheet_name=sheetname, index=False, startrow=1)
    gl_back_addr[sheetname] = "A" + str(strow)
    ws_stsat.write_url(
        gl_back_addr[sheetname],
        "internal:'" + sheetname + "'!A1",
        string="Заготовка для отчета ПТО",
    )
    return 0


def loadwf(sheetname: str, dirtol: str) -> pd.DataFrame:
    """Загрузка таблиц из каталога для факт поставки.
    возвращает суммарный датафрейм.

    sheetname - имя листа для загрузки
    dirtol - каталог для загрузки
    """
    g_res = pd.DataFrame()

    list_file = get_files(dirtol, gl_dagmode, gl_dagmode_env, gl_prefix_smb)

    if gl_dagmode is False:
        list_file = convert_fakt_prqt(list_file, gl_dagmode)

    # print("Нашел файлы для загрузки:")
    # pprint.pprint([{num: value} for num, value in enumerate(list_file)])
    # for num, value in enumerate(list_file):
    #     print(f"{num}: {value}")

    for li in list_file:
        print(f"Загрузка файла: {li}")

        if PureWindowsPath(li).suffix.lower() == ".prqt":
            if gl_dagmode:
                filetl = open_file(li, mode="rb", share_access="rw")
            else:
                filetl = li

            res = pd.read_parquet(filetl)

        if len(g_res.index) == 0:
            g_res = res.copy()
        else:
            g_res = pd.concat([g_res, res])

    return g_res


def fakt(
    sheetname: str,
    year_ft: int,
    filefact: str,
    shnamef="Факт_поставки",
    shnamepf="План_факт_пост",
) -> int:
    """Генерация отчета по факту поставки и корректировка ВП под факт
    Args:
        sheetname (str): имя листа для загрузки
        year_ft (int): год для вывода в имя листа и фильтр по плану
        filefact (str): путь до каталого с выгрузками отчета ОВПП
        shnamef (str, optional): имя листа для вывода факта поставки. Defaults to "Факт_поставки".
        shnamepf (str, optional): имя листа для вывода план-факта поставки по ВП. Defaults to "План_факт_пост".

    Returns:
        int: результат. сейчас 0
    """

    global gl_df
    df_fakt = loadwf(sheetname, filefact)

    # формирование номера заявки
    df_fakt[["НомЗаяв", "Позиция"]] = (
        df_fakt[["НомЗаяв", "Позиция"]].fillna(0.0).astype("Int64")
    )

    df_fakt["sap"] = (
        df_fakt["НомЗаяв"].astype(str) + " / " + df_fakt["Позиция"].astype(str)
    )

    # выборка заявка должна быть в файлах настроек или отписана на менеджера из списка
    man_ts = ["Менеджер УМАИТиТ ОАСУТП", "Менеджер УМАИТиТ ОТ", "Менеджер УМАИТиТ ОИТ"]

    df_fakt = df_fakt[
        df_fakt["sap"].isin(gl_filtersdf[gl_filtersdf["item"] == "заявка"]["sap"])
        | df_fakt["Отдел(sap)"].isin(man_ts)
    ]

    df_fakt.drop(labels=["НомЗаяв", "Позиция"], axis="columns", inplace=True)

    df_fakt = df_fakt.merge(
        gl_filtersdf[gl_filtersdf["item"] == "заявка"][
            ["sap", "bgt", "otdel", "vidtmc"]
        ],
        how="left",
        left_on="sap",
        right_on="sap",
    )

    # поля для присоединения к отчету факт из базы заявок
    needfield = [
        "/ САП",
        "Договор 1С",
        "Поставщик",
        "Номер дог. документа",
        "Номер лота из отчета по лотам",
        "Статус",
        "СРОК",
        "Исполнитель закупки",
        "Вид деятельности",
        # "Кол-во по сводной заявке плановое",
        "Кол-во в ЕИ",
    ]
    df_fakt = pd.merge(
        left=df_fakt,
        right=gl_df[needfield],
        how="left",
        right_on=["/ САП"],
        left_on=["sap"],
    )

    df_fakt.rename(
        columns={
            "otdel": "Отдел(план)",
            "Вид деятельности": "Вид деят(план)",
            "vidtmc": "ВидТМЦ",
        },
        inplace=True,
    )

    df_fakt["Отдел(sap)"] = df_fakt["Отдел(sap)"].map(
        {
            "Менеджер УМАИТиТ ОАСУТП": "АСУТП",
            "Менеджер УМАИТиТ ОТ": "ОТ",
            "Менеджер УМАИТиТ ОИТ": "ИТ",
        },
        na_action="ignore",
    )

    df_fakt["Вид деят(план)"] = df_fakt["Вид деят(план)"].map(
        {
            "Оборудование не входящее в смету строек": "ОНСС",
            "Основная деятельность": "МТР",
        },
        na_action="ignore",
    )

    # замена содержимого в колонке по нахождению слова
    df_fakt.loc[
        df_fakt["ВидТМЦ"].str.contains("расходники", na=False, case=False), "ВидТМЦ"
    ] = "расходники"

    # добавление колонок для сортировки по датам/отделам
    df_fakt["YYYY-mm"] = pd.to_datetime(
        df_fakt["Дата проводки СФ"], format="%b %Y"
    ).dt.strftime("%Y-%m")
    df_fakt.sort_values(
        [
            "YYYY-mm",
            "Отдел(план)",
            "Вид деят(план)",
            "bgt",
        ],
        ascending=[
            False,
            True,
            True,
            True,
        ],
        inplace=True,
    )

    df_fakt.drop(
        [
            "/ САП",
            "№ Сводной заявки",
            "№ поз",
            "YYYY-mm",
        ],
        axis="columns",
        inplace=True,
    )

    df_fakt["Разница с заявкой"] = (
        df_fakt["Кол-во Итого по позиции спец-и"] - df_fakt["Кол-во в ЕИ"]
    )

    cols_to_move_l = [
        "sap",
        "Отдел(план)",
        "Отдел(sap)",
        "Вид деят(план)",
        "ВидТМЦ",
        "bgt",
        "Номенклатура фактическая",
        "№ С/Ф поставщика",
        "Дата СФ",
        "Дата проводки СФ",
        "Разница с заявкой",
        "Кол-во Итого по позиции спец-и",
        "Сумма c НДС",
        "Договор с пост (ОВПП)",
        "Договор 1С",
    ]

    cols_to_move_r = ["Статус_Позиции", "Состояние позиции", "Склад_прихода"]
    df_fakt = df_fakt[
        cols_to_move_l
        + [
            x
            for x in df_fakt.columns
            if x not in cols_to_move_l and x not in cols_to_move_r
        ]
        + cols_to_move_r
    ]

    sheetnm = shnamef
    df_fakt.to_excel(gl_writer, sheet_name=sheetnm, startrow=1, index=False)
    wsl = gl_writer.book.get_worksheet_by_name(sheetnm)  # type: ignore
    wsl.set_column(10, 12, 14, gl_style["gl_style_money"])  # type: ignore
    wsl.autofit()  # type: ignore

    column_settings = [{"header": column} for column in df_fakt.columns]
    wsl.add_table(  # type: ignore
        1,
        0,
        1 + len(df_fakt.index),
        0 + df_fakt.shape[1] - 1,
        {
            "columns": column_settings,
            "style": "Table Style Medium 9",
            "name": "Факт_ОВПП",
        },
    )

    # Добавление условного форматирования по сумме и признаку
    wsl.conditional_format(  # type: ignore
        1,
        1,
        1 + len(df_fakt.index),
        6,
        {
            "type": "formula",
            "criteria": '=AND($D2="ОНСС",$M2/$L2<120000)',
            "format": gl_style["gl_style_moneyRed"],
        },
    )

    # вывод сводной
    df_fsv = df_fakt.copy()
    df_fsv["Дата_п"] = df_fsv["Дата проводки СФ"].dt.strftime("%Y-%m")
    df_fsv = pd.pivot_table(
        df_fsv,
        index=["Отдел(план)", "Вид деят(план)", "bgt"],
        columns="Дата_п",
        values="Сумма c НДС",
        aggfunc="sum",
    ).reset_index()

    # sheetnm = "Факт_ОВПП" + str(year_ft) + "_свод"
    sheetnm = shnamef + "_свод"
    df_fsv.to_excel(gl_writer, sheet_name=sheetnm, startrow=1, index=False)
    df_fsv = None

    # скопировать факт для корректировки ВП
    df_corr = df_fakt.copy()
    df_fakt_start = df_fakt.copy()
    # скопировать факт для поиска не пришедших позиций

    # TODO скорректировать вывод план-факт чтобы не портились исходные данные
    #   сборка листа план-факт
    df_fakt.drop(
        [
            "СРОК",
            "Номер дог. документа",
            "Статус",
            "bgt",
        ],
        axis="columns",
        inplace=True,
    )

    needfield = [
        "/ САП",
        "СРОК",
        "Наименование факт",
        "Статус",
        "Вид деятельности",
        "Сумма",
        "Наименование заявки",
        # "Кол-во по сводной заявке плановое",
        "Кол-во в ЕИ",
    ]

    gl_df_c = gl_df[gl_df["СРОК"].dt.year >= year_ft]

    df_fakt = pd.merge(
        left=df_fakt,
        right=gl_df_c[needfield],
        how="outer",
        right_on=["/ САП"],
        left_on=["sap"],
    ).merge(
        gl_filtersdf[gl_filtersdf["item"] == "заявка"][["sap", "otdel", "bgt"]],
        how="left",
        left_on="/ САП",
        right_on="sap",
    )

    df_fakt["Вид деятельности"] = df_fakt["Вид деятельности"].map(
        {
            "Оборудование не входящее в смету строек": "ОНСС",
            "Основная деятельность": "МТР",
        },
        na_action="ignore",
    )

    # заполнение информации по пустым партиям для возможности фильтрации
    filt_name = df_fakt["sap_y"].isna()
    df_fakt.loc[filt_name, "Наименование факт"] = df_fakt["Номенклатура фактическая"]
    filt_name = df_fakt["СРОК"].isna()
    df_fakt.loc[filt_name, "СРОК"] = df_fakt["Дата проводки СФ"]

    df_fakt = (
        df_fakt.sort_values(
            [
                "Отдел(план)",
                "Вид деят(план)",
                "Дата проводки СФ",
                "bgt",
                "СРОК",
            ]
        )
        .drop(
            [
                "/ САП",
                "sap_x",
                "Отдел(план)",
                "Вид деят(план)",
                "Номенклатура фактическая",
            ],
            axis="columns",
        )
        .rename(
            columns={
                "sap_y": "sap",
                "otdel": "Отдел(план)",
                "Вид деятельности": "Вид деят(план)",
                "Наименование факт": "Номенклатура фактическая",
                "Сумма": "Сумма (план)",
                "Сумма c НДС": "Сумма c НДС(факт)",
            }
        )
    )

    cols_to_move_l = [
        "sap",
        "Отдел(план)",
        "Отдел(sap)",
        "Вид деят(план)",
        "bgt",
        "Номенклатура фактическая",
    ]

    df_fakt = df_fakt[
        cols_to_move_l + [x for x in df_fakt.columns if x not in cols_to_move_l]
    ]

    column_settings = None
    sheetnm = shnamepf
    df_fakt.to_excel(gl_writer, sheet_name=sheetnm, startrow=1, index=False)
    wsl = gl_writer.book.get_worksheet_by_name(sheetnm)  # type: ignore
    wsl.set_column(0, 8, 14)  # type: ignore
    wsl.set_column(12, 20, 14)  # type: ignore
    wsl.set_column(9, 11, 14, gl_style["gl_style_money"])  # type: ignore
    # wsl.autofit()

    column_settings = [{"header": column} for column in df_fakt.columns]
    wsl.add_table(  # type: ignore
        1,
        0,
        1 + len(df_fakt.index),
        0 + df_fakt.shape[1] - 1,
        {
            "columns": column_settings,
            "style": "Table Style Medium 9",
            "name": "Факт_план",
        },
    )

    # вывод сводной
    df_fsv = df_fakt.copy()
    df_fsv["Дата_п"] = df_fsv["СРОК"].dt.strftime("%Y-%m")
    df_fsv["Сумма_пф"] = df_fsv["Сумма (план)"]
    mask = ~df_fsv["Сумма c НДС(факт)"].isna()

    df_fsv.loc[mask, "Сумма_пф"] = df_fsv["Сумма c НДС(факт)"]

    df_fsv = pd.pivot_table(
        df_fsv,
        index=["Отдел(план)", "Вид деят(план)", "bgt"],
        columns="Дата_п",
        # values="Сумма c НДС(факт)",
        values="Сумма_пф",
        aggfunc="sum",
    ).reset_index()

    sheetnm = shnamepf + "_свод"
    df_fsv.to_excel(gl_writer, sheet_name=sheetnm, startrow=1, index=False)
    df_fsv = None

    # --корректировка ВП по факту поставки--
    # выборка строк в которых месяц поставки не равен месяцу в СРОК
    df_corr = df_corr[
        df_corr["Дата проводки СФ"].dt.month != df_corr["СРОК"].dt.month
    ].dropna(subset="СРОК")

    df_corr.to_excel(gl_writer, sheet_name="Корректировки_ВП", startrow=1, index=False)

    # подготовка фрейма для обновления gl_df
    # выбросить колонки не в списке,переименовать, отсортировать по срок, сбросить дубликаты с сохранением последнего,
    # установить индекс по колонке sap
    df_corr = (
        (
            df_corr[df_corr.columns.intersection(["Дата проводки СФ", "sap"])].rename(
                columns={"Дата проводки СФ": "СРОК"}
            )
        )
        .sort_values("СРОК")
        .drop_duplicates("sap", keep="last")
    ).set_index(["sap"])

    gl_df.set_index(["/ САП"], inplace=True)
    gl_df.update(df_corr)
    gl_df.reset_index(inplace=True)

    # корректировка ВП по не пришедшим позициям или вывод отчета
    # проверить что факт за тек месяц не пустой

    # cur_date1 = date(datetime.now().year, datetime.now().month, 1)
    # cur_date2 = (cur_date1 + pd.offsets.DateOffset(months=1) - timedelta(days=1)).date()

    cur_date1 = pd.to_datetime(date(datetime.now().year, datetime.now().month, 1))
    cur_date2 = cur_date1.date() + pd.offsets.DateOffset(months=1) - timedelta(days=1)

    # prev_date2 = date(datetime.now().year, datetime.now().month, 1) - timedelta(days=1)
    # prev_date1 = date(prev_date2.year, prev_date2.month, 1)

    prev_date2 = pd.to_datetime(
        date(datetime.now().year, datetime.now().month, 1) - timedelta(days=1)
    )
    prev_date1 = pd.to_datetime(date(prev_date2.year, prev_date2.month, 1))

    # записи факта прихода за текущий месяц
    cmf = df_fakt_start[df_fakt_start["Дата проводки СФ"].between(cur_date1, cur_date2)]

    # ecли есть записи за текущий месяц, значит прошлый скорее всего закрыт.
    # значит можно проверять что не пришло
    if len(cmf.index) > 0:
        # записи факта прихода за предыдущий месяц
        pmf = df_fakt_start[
            df_fakt_start["Дата проводки СФ"].between(prev_date1, prev_date2)
        ]
        if len(pmf.index) > 0:
            # есть факт за предыдущий месяц, считаем, что всё что не отражено по предыдущему месяцу не пришло
            # список заявок из предыдущего месяца
            prev_df = gl_df[gl_df["СРОК"].between(prev_date1, prev_date2)]

            # поиск заявок из предыдущего месяца которых нет в
            # выборке из факта строк где проставлена дата проводки или есть первичный номер заявки.
            diff_z = prev_df[~prev_df["/ САП"].isin(df_fakt_start["sap"])]

            diff_z.to_excel(
                gl_writer, sheet_name="Не_пришло_ВП", startrow=1, index=False
            )

    # в факте поставки отсутствует часть заявок из-за консолидации сводных
    return 0


def generatereport(dfa) -> int:
    """
    генерация отчета
    dfa - фрейм по заявкам + по заявителю
    """
    global gl_writer, gl_style, gl_prefix_smb, gl_df_project, gl_df

    # подготовка выходного файла
    dpath = (
        str(PureWindowsPath(gl_out_path))
        + "\\"
        + gl_settings.get("префикс", "")
        + "_pandas_v2_to_excel-"
        + gl_settings.get("версиявп", "")
        + ".xlsx"
    )

    if gl_dagmode is True:
        wfile = open_file(gl_prefix_smb + dpath, mode="wb")

        gl_writer = pd.ExcelWriter(
            wfile,
            mode="w",
            engine="xlsxwriter",
            date_format="dd/mm/yyyy",
            datetime_format="dd/mm/yyyy",
        )

    else:
        gl_writer = pd.ExcelWriter(
            dpath,
            mode="w",
            engine="xlsxwriter",
            date_format="dd/mm/yyyy",
            datetime_format="dd/mm/yyyy",
        )

    workbook = gl_writer.book
    worksheet2 = workbook.add_worksheet("Итоги")
    worksheet3 = workbook.add_worksheet("Лог загрузки")
    workbook.add_worksheet("Статистика")
    workbook.add_worksheet("Протоколы")
    workbook.add_worksheet("ПТО")
    workbook.add_worksheet("Факт_поставки")
    workbook.add_worksheet("План_факт_пост")
    workbook.add_worksheet("Факт_поставки_свод")
    workbook.add_worksheet("План_факт_пост_свод")
    workbook.add_worksheet("Корректировки_ВП")
    workbook.add_worksheet("Не_пришло_ВП")
    gl_style = set_xl_styles(workbook)

    print("Сборка и добавление отчетов по факту поставки")
    fakt(
        "Общий список",
        date.today().year,
        gl_factfile,
        "Факт_поставки",
        "План_факт_пост",
    )

    # формирование отчета после корректировки ВП
    df = gl_df
    df["Экономия"] = df["Сумма, руб. с НДС"] - df["Сумма"]

    # сдвиг колонок вправо
    cols_to_move = [
        "Класс",
        "Материал",
        "Доп. данные о материале",
        "Номер дог. документа",
        "Договор 1С",
        "Поставщик",
        "Исполнитель закупки",
        "Номер лота из отчета по лотам",
    ]
    df = df[[x for x in df.columns if x not in cols_to_move] + cols_to_move]
    if not gl_df_project.empty:
        gl_df_project = gl_df_project[
            [x for x in gl_df_project.columns if x not in cols_to_move] + cols_to_move
        ]

    # выключено 09/03/2025
    # date1 = date(int(gl_settings["Период"][0]), 1, 1)  # noqa: F841

    """
    # давно выключено
    date2 = date(int(gl_settings['Период'][0]), 12, 31)
    date3 = date(int(gl_settings['Период'][2]), 1, 1)
    date4 = date(int(gl_settings["Период"][2]), 12, 31)
    date4 = date(int(gl_settings["Период"][-1]), 12, 31)
    """
    # выключено 09/03/2025
    # date4 = date(2023, 12, 31)  # noqa: F841
    # date100 = date(2024, 1, 1)  # noqa: F841

    # добавление номеров протоколов к копии БД из возвратного плана
    df = pd.merge(df, gl_filtersdf, how="left", left_on=["/ САП"], right_on=["sap"])
    df.drop(
        ["item", "sap", "year", "toonss", "bgt", "year_d", "otdel"],
        axis=1,
        inplace=True,
    )
    df.rename(columns={"protcl": "Протокол", "protcl_d": "Дата п."}, inplace=True)

    reportstatlog(worksheet3, df, dfa)
    worksheet2.activate()

    # выводим заголовок в итоги
    worksheet2.write("A1", "Тип", gl_style["gl_style_cell_format_bold_bd"])  # pyright: ignore[reportArgumentType]
    worksheet2.write(
        "B1",  # pyright: ignore[reportArgumentType]
        "Разрез группировки",  # pyright: ignore[reportArgumentType]
        gl_style["gl_style_cell_format_bold_bd"],
    )
    worksheet2.write("C1", "Суммы (тыс.руб.)", gl_style["gl_style_cell_format_bold_bd"])  # pyright: ignore[reportArgumentType]
    worksheet2.write("D1", "Остаток сейчас", gl_style["gl_style_cell_format_bold_bd"])  # pyright: ignore[reportArgumentType]
    worksheet2.write(
        "E1",  # pyright: ignore[reportArgumentType]
        "Итого остаток с учетом ожид",  # pyright: ignore[reportArgumentType]
        gl_style["gl_style_cell_format_bold_bd"],
    )
    worksheet2.write(
        "F1",  # pyright: ignore[reportArgumentType]
        "Итого остаток если всё закупят",  # pyright: ignore[reportArgumentType]
        gl_style["gl_style_cell_format_bold_bd"],
    )
    worksheet2.write(
        "G1",  # pyright: ignore[reportArgumentType]
        "Прогноз остатка " + gl_settings["Период"][2],
        gl_style["gl_style_cell_format_bold_bd"],
    )

    worksheet2.write("I1", "Экономия пл-фкт", gl_style["gl_style_cell_format_bold_bd"])  # pyright: ignore[reportArgumentType]

    strow = 2
    for yn in range(0, len(gl_settings["Период"])):
        # вывод статистики по статусу заявки c нулевого года со строки strow
        strow = reportstat(df, yn, strow)

    ws_stsat = workbook.get_worksheet_by_name("Статистика")  # type: ignore
    ws_stsat.set_column("A:A", 40)  # type: ignore
    ws_stsat.set_column("B:X", 12)  # type: ignore

    # вывод блоков отчета по перечню годов из поля "Период", высота+1 пустая
    crow = 2

    for yn in range(0, len(gl_settings["Период"])):
        wr = report_year(yn, df, worksheet2, crow, "ОНСС")
        crow = crow + wr + 1
        wr = report_year(yn, df, worksheet2, crow, "МТР")
        crow = crow + wr + 1

    report_year(1, df, worksheet2, crow, "КОНТРОЛЬ")

    worksheet2.set_column("B:B", 24)  # type: ignore
    worksheet2.set_column("C:I", 20)  # type: ignore

    # Вывод отчета по протоколам если классификатор загружен
    if len(gl_df_cmtr.index) > 1:
        report_proto()
    else:
        print("Классификатор МТР не загружен! Отчет по протоколам не генерируется.")

    report_pto()

    # добавление гиперссылок назад на листы
    wsts = workbook.worksheets()
    for sheet in wsts:  # type: ignore
        sn = sheet.get_name()
        match sn[:4]:  # type: ignore
            case "Итог":
                continue
            case "Сум_" | "Стр_" | "Год_" | "Стт_":
                sheet.write_url(  # type: ignore
                    "A1", "internal:'Статистика'!" + gl_back_addr[sn], string="/ САП"
                )
            case "Про_":
                sheet.write_url(  # type: ignore
                    "A1", "internal:'Протоколы'!" + gl_back_addr[sn], string="/ САП"
                )
            case "Пто_":
                sheet.write_url(  # type: ignore
                    "A1", "internal:'ПТО'!" + gl_back_addr[sn], string="/ САП"
                )

            case "Лог " | "Стат":
                sheet.write_url("A1", "internal:'Итоги'!A1", string="Итоги")  # type: ignore
            case _:
                sheet.write_url("A1", "internal:'Итоги'!A1", string="/ САП")  # type: ignore

    statstr = f"Генерация отчета выполнена: {str(datetime.now())}, листов в отчете: {len(wsts)}"
    worksheet3.write("B2", statstr)  # type: ignore

    # добавить вывод в лог списка примененных корректировок по переменной из main
    worksheet3.write(
        worksheet3.dim_rowmax + 2,  # type: ignore
        1,
        "Применены корректировки (по заявкам из настроек):",
    )
    if gl_load_cor_res is not None:
        worksheet3.write_column(worksheet3.dim_rowmax + 1, 1, gl_load_cor_res)  # type: ignore

    if gl_dagmode is True:
        print("Cохранение отчета " + gl_prefix_smb + dpath)
    else:
        print("Cохранение отчета " + dpath)
    gl_writer.close()
    if gl_dagmode is True:
        wfile.close()
    return 0


def loadclassmtr(engine=None, need_load=False) -> int:
    """Загрузка классификатора МТР.
    Загрузка производится из gl_settings['классификатор']
    """
    global gl_df_cmtr, gl_dagmode

    if gl_settings.get("классификатор", None) is None:
        print("Путь к классификатору в настройках не найден. Пропускаем.")
        return -1

    filecmtr = gl_settings["классификатор"]

    # проверка даты файла для режима DAG
    if gl_dagmode is True:
        print("Проверка необходимости обновления классификатора МТР")
        ftl = gl_prefix_smb + str(filecmtr).partition(":")[2]
        print(f"----ftl: {ftl}")
        statinfo = stat(ftl)
        modified = datetime.fromtimestamp(statinfo.st_mtime)
        # print(f"Проверка классификатора, {modified}, {ftl}")
    else:
        print("Проверка необходимости обновления классификатора МТР")
        if Path("classmtr.pkl").is_file():
            # проверка существованя кэша классификатора
            ftl = str(filecmtr)
            statinfo = stat_file(ftl)
            modified = datetime.fromtimestamp(statinfo.st_mtime)
        else:
            need_load = True

    if need_load is not True:
        if check_file_data(engine, modified, ftl) is True:
            need_load = True

    if need_load is True:
        print("Обновление классификатора МТР")
        try:
            if gl_dagmode is True:
                ftl = gl_prefix_smb + str(filecmtr).partition(":")[2]
                print(f"Загрузка классификатора из файла: {ftl}")
                file = open_file(ftl, mode="rb", share_access="r")
            else:
                file = open(filecmtr)
        except IOError as e:
            print(
                f"не удалось открыть файл классификатор МТР {filecmtr}. Скорректируйте путь или удалите настройку"
            )
            print(e)
            sys.exit(0)
        else:
            with file:
                if __debug__:
                    print(f"Читаем классификатор из файла {filecmtr}")
                if gl_dagmode is True:
                    df_cmtr2_dict = pd.read_excel(
                        file,
                        engine="calamine",
                        sheet_name=["Расшифровка", "Классификатор_МТР"],
                    )

                else:
                    df_cmtr2_dict = pd.read_excel(
                        filecmtr,
                        engine="calamine",
                        sheet_name=["Расшифровка", "Классификатор_МТР"],
                    )

                df_cmtr2 = df_cmtr2_dict["Расшифровка"]
                minrow = int(df_cmtr2[df_cmtr2.iloc[:, 0] == "Начало"].iloc[0, 1]) - 2
                if minrow < 0:
                    minrow = 0
                df_cmtr2_head = df_cmtr2[
                    (df_cmtr2.iloc[:, 0] != "Начало") & (df_cmtr2.iloc[:, 0] != "#")
                ].iloc[:, :2]
                df_cmtr2_head.columns = [
                    "class",
                    "classname",
                ]
                df_cmtr2 = df_cmtr2_dict["Классификатор_МТР"].iloc[minrow:, 1:]
                df_cmtr2.columns = [
                    "class",
                    "classname",
                    "level",
                    "direction",
                    "addreq",
                ]
                gl_df_cmtr = pd.concat([df_cmtr2_head, df_cmtr2]).reset_index(drop=True)
                gl_df_cmtr["class"] = gl_df_cmtr["class"].str.strip()
                gl_df_cmtr["addreq"] = gl_df_cmtr["addreq"].str.replace("\r", "")
                gl_df_cmtr["direction"] = gl_df_cmtr["direction"].str.replace("\r", "")

                if gl_dagmode is True and engine is not None:
                    print("Запись классификатора в SQL базу")
                    gl_df_cmtr.to_sql("classmtr.pkl", engine, if_exists="replace")
                    save_file_data(engine, modified, ftl)
                else:
                    gl_df_cmtr.to_pickle("classmtr.pkl")
                    save_file_data(engine, modified, ftl, gl_dagmode)

                print(
                    f"Загружен классификатор МТР {len(gl_df_cmtr[gl_df_cmtr['level'] != 'legend'].index)} строк"
                )

    else:
        print(
            "Загрузка не требуется. Дата загруженного файла больше или совпадает с найденным."
        )
    return 0


def load_cor_file(prefix_smb="") -> list[str]:
    """
    загрузка файла корректировок и обновление фрейма заявок gl_df
    """
    global gl_df, gl_dagmode

    if gl_dagmode is True:
        fpp = prefix_smb + str(gl_settings.get("корректировки", None)).partition(":")[2]
        print(f"Читаем файл: {fpp}")
        file = open_file(
            fpp,
            mode="rb",
            share_access="rw",
        )
        df_b1 = pd.read_excel(file, sheet_name="Лист1", engine="calamine")
    else:
        my_db = Path(gl_settings.get("корректировки", None))
        if not my_db.is_file():
            print(
                "Нет файла с корректировками. Варианты исправления: \n Исправить путь и имя файла корректировок\n Удалить настройку 'Корректировки' \n Указать значение настройки 'Корректировки' = '0'"
            )
            sys.exit(0)

        df_b1 = pd.read_excel(
            gl_settings["корректировки"], sheet_name="Лист1", engine="calamine"
        )

    if df_b1["НомЗаяв"].dtype != "int64":
        df_b1["НомЗаяв"] = pd.to_numeric(df_b1["НомЗаяв"], errors="coerce")
    if df_b1["Позиция"].dtype != "int64":
        df_b1["Позиция"] = pd.to_numeric(df_b1["Позиция"], errors="coerce")

    df_b1["SAP"] = df_b1["НомЗаяв"].astype(str) + " / " + df_b1["Позиция"].astype(str)

    date1 = pd.to_datetime(gl_settings["датавп"])

    # Выборка корректировок для применения по статусам
    good_cor = ["На согласовании", "Применена", "Согласовано"]
    # df_b1.drop(
    #     df_b1.loc[(df_b1["ДатаИзм"] < date1) | (df_b1["Наим"] != "Применена")].index,
    #     inplace=True,
    # )
    df_b1.drop(
        df_b1.loc[(df_b1["ДатаИзм"] < date1) | (~df_b1["Наим"].isin(good_cor))].index,
        inplace=True,
    )

    df_b1.drop(
        [
            "Наим",
            "Вид коррек",
            "Сокр Опер",
            "Создал",
            "Изменил",
            "Код объект",
            "Номер мате",
            "Номер НПП",
            "Поз. НПП",
            "Статус НПП",
            "НомСводЗв",
            # "Позиция.1",
            "Позиция",
            "Статус",
            "Код формы",
            "Комментарий к виду Корректировки 00 - Пр",
            "АналитСпрв",
            "Статус сводного лота",
            "№СводЛота",
            "Примечание",
            "ПричинаКор",
            "Класс",
            "БЕИ",
        ],
        axis="columns",
        inplace=True,
    )
    df_b1 = df_b1.sort_values("ДатаИзм").drop_duplicates("SAP", keep="last")

    # список заявок в фильтре
    zai = gl_filtersdf[gl_filtersdf["item"] == "заявка"]["sap"]
    # выводим в лог только корректировки по заявкам из фильтра
    logres = (
        df_b1[df_b1["SAP"].isin(zai)]
        .to_csv(
            columns=[
                "SAP",
                "№корректировки",
                "Краткий текст материала",
                "изм. кол-ва",
                "Количество",
                "изм. Даты поставки",
                "Дата поста",
                "Цена(Новый)",
                "ДатаИзм",
            ],
            index=False,
        )
        .splitlines()
    )

    df_b1.set_index(["SAP"], inplace=True)
    df_b1.rename(
        # columns={"Количество": "Кол-во в ЕИ", "Сумма по п": "Сумма"}, inplace=True
        columns={"Количество": "Кол-во в ЕИ", "Сумма по п": "Сумма, руб. с НДС"},
        inplace=True,
    )

    gl_df.set_index(["/ САП"], inplace=True)
    gl_df.update(df_b1)
    gl_df.reset_index(inplace=True)

    # обновление дат по корректировкам
    df_b1.reset_index(inplace=True)

    # сбросить корректировки по чужим заявкам
    df_b1 = df_b1[df_b1["SAP"].isin(gl_df["/ САП"])]

    # пересчитать "Сумма" по строкам с корректировкой
    # риск снести сумму если идёт изменение стоимости на этапе АС.
    mask = (gl_df["/ САП"].isin(df_b1["SAP"])) & (
        gl_df["Кол-во АС расч."] == 0 | gl_df["Кол-во АС расч."].isna()
    )

    # mask = gl_df["/ САП"].isin(df_b1["SAP"])
    gl_df.loc[mask, "Сумма"] = (
        gl_df.loc[mask, "Кол-во в ЕИ"] * gl_df.loc[mask, "Цена, руб. с НДС"]
    )

    # прилепить к корректровкам информацию из заявок
    df_b2 = pd.merge(df_b1, gl_df, how="left", left_on=["SAP"], right_on=["/ САП"])

    # Сбросить корректировки не изменяющие дату поставки вправо
    df_b2.drop(
        df_b2[
            (df_b2["Дата поста"] <= df_b2["Плановая дата поставки"])
            & (df_b2["Дата поста"] <= df_b2["СРОК"])
        ].index,
        inplace=True,
    )

    df_b2["СРОК"] = df_b2["Дата поста"]

    goodcol = [
        "SAP",
        "Дата поста",
        "СРОК",
    ]
    dropcol = [x for x in df_b2.columns if x not in goodcol]

    df_b2.drop(
        labels=dropcol,
        axis="columns",
        inplace=True,
    )
    df_b2.set_index(["SAP"], inplace=True)
    gl_df.set_index(["/ САП"], inplace=True)
    gl_df.update(df_b2)
    gl_df.reset_index(inplace=True)

    return logres


def startdag(envval: dict) -> bool:
    """Вызов генерации отчета в режиме DAG"""
    global \
        gl_out_path, \
        gl_dagmode, \
        gl_settings, \
        gl_filters, \
        gl_filtersdf, \
        gl_df_cmtr, \
        gl_df_project, \
        gl_df_control, \
        gl_df, \
        gl_prefix_smb, \
        gl_dagmode_env, \
        gl_load_cor_res

    gl_dagmode = True
    gl_dagmode_env = envval

    print("---Запущена процедура startdag-----")
    print(f"Версия python: {pyversion}")
    print(f"Версия XlsxWriter: {version('XlsxWriter')}")
    print(f"Версия python-calamine: {version('python-calamine')}")
    print(f"Версия Pandas: {version('pandas')}")
    startTime = timer(name="Таймер запущен. Чтение настроек")

    register_session(
        envval["uri_samba"]["_host"],
        username=envval["uri_samba"]["username"],
        password=envval["uri_samba"]["password"],
    )

    # gl_out_path = r"\source\python\Python-xls\data\gen"
    gl_out_path = envval["outpath"]
    gl_prefix_smb = (
        "\\"
        + str(envval["uri_samba"]["_host"])
        + "\\"
        + str(envval["uri_samba"]["_share"])
    )

    # print(f"---gl_prefix_smb, {gl_prefix_smb}")
    configs = []

    if "it" in envval["whois"]:
        configs.append(
            gl_prefix_smb
            # + r"\source\python\Python-xls\data\настройки\ИТ_настройки2.xlsx"
            + envval["confdir"]
            + r"\ИТ_настройки2.xlsx"
        )
        print("configs:", configs)
    if "ot" in envval["whois"]:
        configs.append(
            gl_prefix_smb
            # + r"\source\python\Python-xls\data\настройки\ОТ_настройки2.xlsx"
            + envval["confdir"]
            + r"\ОТ_настройки2.xlsx"
        )
    if "asutp" in envval["whois"]:
        configs.append(
            gl_prefix_smb
            # + r"\source\python\Python-xls\data\настройки\АСУТП_настройки2.xlsx"
            + envval["confdir"]
            + r"\АСУТП_настройки2.xlsx"
        )

    gl_settings, gl_filters, gl_filtersdf = loadsettings3(configs, gl_dagmode)

    engine = create_engine(envval["uri_pg"])
    monkey_path2()
    # обновление справочника классов МТР
    loadclassmtr(engine)

    dupecontrol()
    gl_filtersdf["year_d"] = pd.to_datetime(gl_filtersdf["year"], errors="coerce")

    timer("Настройки загружены", startTime)

    # if close_report():
    #     print("Файл отчета принудительно закрыт!")

    timer("Загрузка в DF из SQL", startTime)

    # построение списка заявок
    values = gl_filtersdf[gl_filtersdf["item"] == "заявка"]["sap"].tolist()
    valuesz = gl_filters["заявитель"]  # noqa: F841

    # TO-DO добавить проверку наличия БД
    # загрузка справочника классов МТР
    gl_df_cmtr = pd.read_sql_table("classmtr.pkl", engine).drop(
        labels="index", axis="columns"
    )

    # загрузка из БД в Dataframe
    # Загрузка кэша ВП заявок с полем "срок"
    # TO-DO добавить проверку наличия БД
    # dfb = pd.read_sql_table("to_pickle", engine).drop(labels="index", axis="columns")
    dfb = pd.read_sql_table("to_pickle", engine)

    # Загрузка в DF из кэша заяков без поля "срок" => "БД проекты"
    # TO-DO добавить проверку наличия БД
    # gl_df_project = pd.read_sql_table("pickle_na", engine).drop(
    #     labels="index", axis="columns"
    # )
    gl_df_project = pd.read_sql_table("pickle_na", engine)

    # удалить из БД проектов строки по чужим заявкам
    gl_df_project.drop(
        gl_df_project[~gl_df_project["/ САП"].isin(values)].index, inplace=True
    )

    # удаление колонки с 'наименованием факт'
    vvn = gl_filtersdf[
        (gl_filtersdf["item"] == "вывести наименование факт")
        & (gl_filtersdf["sap"] == "Нет")
    ].shape[0]
    if vvn == 1:
        dfb.drop("Наименование факт", axis=1, inplace=True)

    start_date = date(int(gl_settings["Период"][0]), 1, 1)  # noqa: F841

    # формирование фрейма контролируемых заявок
    control = gl_filtersdf[gl_filtersdf["item"] == "контроль"]["sap"].to_list()
    gl_df_control = dfb[dfb["/ САП"].isin(control)]

    # выборка без ограничения правого срока
    dfb = dfb.query(
        "`/ САП`==@values | (`СРОК`>=@start_date & `Наименование заявителя`==@valuesz)"
    )

    # выборка по заявкам
    gl_df = dfb[dfb["/ САП"].isin(values)].copy()
    timer("Загружено в DF из SQL", startTime)

    # Применение исправлений к возвратному плану
    cor_data()

    # проверка необходимости загрузки корректировок
    if gl_settings.get("версиявп", None) and gl_settings.get("корректировки", None):
        gl_load_cor_res = load_cor_file(gl_prefix_smb)
        print(f"Загружен файл корректировок {gl_settings['корректировки']}")

    # выгрузка кэша ВП если запуск по ветке "Упр"
    if gl_settings.get("префикс", "") == "Упр":
        if check_data_vp_cache(gl_dagmode, gl_settings["путь"], "вп_corr.prqt") is True:
            # сливаем ВП по заявкам и проектам и выгружаем в to_pickle_corr
            prj = gl_df_project.copy()
            mask = prj["СРОК"].isna()
            prj.loc[mask, "СРОК"] = prj.loc[mask, "Плановая дата поставки"]
            res = pd.concat([gl_df, prj], axis="index")

            save_vp_cache(res, gl_dagmode, gl_settings["путь"], "вп_corr.prqt")

            res = None
            prj = None

    generatereport(dfb)
    timer("Отчет записан", startTime)

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


def check_data_vp_cache(dagmode: bool, path_to_save: str, filename: str) -> bool:
    """Проверка что дата кэш файла самая новая в каталоге с ВП

    Args:
        dagmode (bool): _description_
        path_to_save (str): _description_
        filename (str): _description_

    Returns:
        bool: Возвращает True если требуется обновление кэша (дата кэша не самая новая).
    """
    file_tl_str = ""
    if dagmode:
        dpath = gl_prefix_smb + str(gl_dagmode_env["loaddirvp"])
        nfiled = datetime(1970, 1, 1)

        for file_info in scandir(dpath):
            if file_info.is_file():
                extfile = PureWindowsPath(str(file_info.path)).suffix.lower()
                if extfile == ".xlsx" or extfile == ".prqt":
                    # print(f"Найден файл: {file_info.name}")
                    statinfo = stat(file_info.path)
                    modified = datetime.fromtimestamp(statinfo.st_mtime)
                    if modified > nfiled:
                        nfiled = modified
                        file_tl = file_info
                        file_tl_str = file_tl.name

    else:
        # Поиск последнего по дате файла в каталоге, если доступна файловая система.
        dpath = path_to_save
        dir_path = Path(path_to_save)

        # Filter for files only and find the one with the latest modification time
        file_tl = max(dir_path.iterdir(), key=lambda f: f.stat().st_mtime)
        file_tl_str = file_tl.name

    if file_tl_str == "вп_corr.prqt":
        print(
            f"Обновление кэша ВП не требуется. Самый новый файл:{file_tl_str} в каталоге: {dpath}"
        )
        return False
    else:
        return True


def save_vp_cache(
    res: pd.DataFrame, dagmode: bool, path_to_save: str, filename: str
) -> bool:
    if dagmode:
        import io

        buffer = io.BytesIO()
        dpath = gl_prefix_smb + str(gl_dagmode_env["loaddirvp"]) + "\\" + filename

        with open_file(dpath, mode="wb") as wfile:
            # прямой вывод в сетевой файл не работает. запись через буффер.
            res.to_parquet(buffer)
            wfile.write(buffer.getvalue())

        print("Вывод кэша скорректированного ВП: ", dpath)

    return True


# ----------start--------------
startTime = timer(name="Таймер запущен. Чтение настроек")
if __name__ == "__main__":
    print(f"Версия python: {pyversion}")
    print(f"Версия XlsxWriter: {version('XlsxWriter')}")
    print(f"Версия openpyxl: {version('openpyxl')}")
    print(f"Версия Pandas: {version('pandas')}")
    print(f"Версия Python-calamine: {version('python-calamine')}")

    print(f"Arguments count: {len(sys.argv)}")
    for i, arg in enumerate(sys.argv):
        print(f"Argument {i:>6}: {arg}")

    if len(sys.argv) > 1:
        ii = 1
        Need_refresh = False
        loadlist = []
        while ii < len(sys.argv):
            match sys.argv[ii].lower():
                case "refresh":
                    Need_refresh = True
                    gl_out_path = sys.argv[ii + 1]
                case "generate":
                    gl_out_path = sys.argv[ii + 1]
                case "conf":
                    loadlist.append(sys.argv[ii + 1])
            ii += 2
    else:
        print(
            'Ошибка запуска. Формат запуска: pyt_load_xls-v2.py refresh|generate "path_out" conf "conf_file". Выход!'
        )
        sys.exit(0)

    gl_settings, gl_filters, gl_filtersdf = loadsettings3(loadlist, gl_dagmode)
    monkey_path2()
    if Need_refresh:
        loadclassmtr()
        loadvp3(gl_settings["путь"])

    dupecontrol()
    # gl_filtersdf["year_d"] = pd.to_datetime(gl_filtersdf["year"], errors="coerce")

    timer("Настройки загружены", startTime)
    timer("Загрузка в DF из Pickle", startTime)

    # загрузка справочника классов МТР
    my_db = Path("classmtr.pkl")
    if my_db.is_file():
        gl_df_cmtr = pd.read_pickle("classmtr.pkl")
    else:
        print(
            "Нет базы данных классов МТР, пропускаем сравнение. Для восстановления необходимо запустить 'refresh'"
        )

    # загрузка из БД в Dataframe
    # построение списка заявок
    values = gl_filtersdf[gl_filtersdf["item"] == "заявка"]["sap"].tolist()
    valuesz = gl_filters["заявитель"]

    # Загрузка кэша ВП заявок с полем "срок"
    my_db = Path("to_pickle")
    if not my_db.is_file():
        print("Нет базы данных to_pickle, необходимо запустить 'refresh'")
        sys.exit(0)
    dfb = pd.read_pickle("to_pickle")

    # Загрузка в DF из кэша заяков без поля "срок" => "БД проекты"
    my_db2 = Path("pickle_na")
    if not my_db2.is_file():
        print("Нет базы данных проектов pickle_na, необходимо запустить 'refresh'")
    gl_df_project = pd.read_pickle("pickle_na")
    # удалить из БД проектов строки по чужим заявкам
    gl_df_project.drop(
        gl_df_project[~gl_df_project["/ САП"].isin(values)].index, inplace=True
    )

    # удаление колонки с 'наименованием факт'
    vvn = gl_filtersdf[
        (gl_filtersdf["item"] == "вывести наименование факт")
        & (gl_filtersdf["sap"] == "Нет")
    ].shape[0]
    if vvn == 1:
        dfb.drop("Наименование факт", axis=1, inplace=True)

    start_date = date(int(gl_settings["Период"][0]), 1, 1)

    # формирование фрейма контролируемых заявок
    control = gl_filtersdf[gl_filtersdf["item"] == "контроль"]["sap"].to_list()
    gl_df_control = dfb[dfb["/ САП"].isin(control)]

    # выборка без ограничения правого срока
    dfb = dfb.query(
        "`/ САП`==@values | (`СРОК`>=@start_date & `Наименование заявителя`==@valuesz)"
    )

    # выборка по заявкам
    gl_df = dfb[dfb["/ САП"].isin(values)].copy()
    timer("Загружено в DF из Pickle", startTime)

    # Применение исправлений к возвратному плану
    cor_data()

    # проверка необходимости загрузки корректировок
    if gl_settings.get("версиявп", None) and gl_settings.get("корректировки", None):
        gl_load_cor_res = load_cor_file()
        print(f"Загружен файл корректировок {gl_settings['корректировки']}")
        # print("Применены:")
        # print(*gl_load_cor_res, sep="\n")

    # вывод скорректированного ВП для внешних загрузок
    # сливаем ВП по заявкам и проектам и выгружаем в вп_corr.prqt
    if check_data_vp_cache(gl_dagmode, gl_settings["путь"], "вп_corr.prqt") is True:
        prj = gl_df_project.copy()
        mask = prj["СРОК"].isna()
        prj.loc[mask, "СРОК"] = prj.loc[mask, "Плановая дата поставки"]
        res = pd.concat([gl_df, prj], axis="index")

        patho = Path(gl_settings["путь"], "вп_corr.prqt")
        print("Вывод кэша скорректированного ВП: ", str(patho))
        res.to_parquet(patho)
        res = None
        prj = None

    generatereport(dfb)
    timer("Отчет записан", startTime)

    patho = (
        str(PureWindowsPath(gl_out_path))
        + "\\"
        + gl_settings.get("префикс", "")
        + "_pandas_v2_to_excel-"
        # + str(datetime.now().month)
        + gl_settings.get("версиявп", "")
        + ".xlsx"
    )
    system('start excel.exe "' + str(patho) + '"')

"""
    шаблоны для отладки
    # print(gl_filtersdf[gl_filtersdf['item']=='контроль']['sap'].to_list())
    # pd.set_option('display.max_columns', None)
    # print(gl_filtersdf[gl_filtersdf['item']=='поставка без заявок месяц'])
    # pd.reset_option('display.max_columns')
    # print(gl_filtersdf.dtypes)

    # print(df.dtypes)
    # pd.options.display.width = 0
    # print(df.head(5))
    # getch()

    start_time = timeit.default_timer()
    gl_df.loc[:, "ismtr"] = gl_df.apply(div("Сумма", "Кол-во АС расч."), axis=1)
    print("Div apply = ", timeit.default_timer() - start_time)
        
    # print("бюджет ",ii,"тип ", type_b)
    # print("неправильных ", len(bfdf.index))
    # print(bfdf["Вид деятельности"])
    
"""
