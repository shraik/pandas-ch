# pip install sqlalchemy-cratedb==0.42.0.dev2
import clickhouse_connect
import pandas as pd
import sys
from datetime import date, datetime, timedelta
from pathlib import Path, PureWindowsPath
from python_calamine import CalamineWorkbook

from os import walk, environ as int_env, scandir, stat as stat_file
from sqlalchemy import create_engine, inspect

# from sqlalchemy_cratedb.support import insert_bulk
import sqlalchemy as sa

if dict(int_env).get("AIRFLOW_HOME", None) is not None:
    from smbclient import open_file, register_session, stat, scandir as scandir_smb  # type: ignore


gl_factfile = r"Y:\ilja\source\python\Python-xls\data\факт\помесячно"


def loadsettings3(
    filelist: list, dagmode: bool, defcolstoload=True
) -> tuple[dict, dict, pd.DataFrame]:
    """загрузка настроек
    filelist - список имен файлов для загрузки
    dagmode - признак запуска из DAG,
    defcolstoload - флаг загрузки списка колонок по умолчанию

    возвращает 2 словаря и датафрейм настроек
    """

    filtersdf = pd.DataFrame(
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
            "szk",
        ]
    )

    # для установки префикса по настройкам отдела
    local_pref = ""
    addition = False
    settings = {}
    filter = {}

    for settfile in filelist:
        try:
            if defcolstoload is True:
                colstoload = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
            else:
                colstoload = None

            if dagmode is True:
                file = open_file(settfile, mode="rb", share_access="rw")
                print(f"Читаем настройки из файла {settfile}")
                df_load = pd.read_excel(
                    file,
                    engine="calamine",
                    sheet_name=["Настройки", "Заявки"],
                    usecols=colstoload,
                )
            else:
                # file = open(settfile)
                # with file:
                print(f"Читаем настройки из файла {settfile}")
                df_load = pd.read_excel(
                    settfile,
                    engine="calamine",
                    sheet_name=["Настройки", "Заявки"],
                    usecols=colstoload,
                )

        except IOError as e:
            print(f"не удалось открыть файл {settfile}")
            print(e)
            sys.exit(0)

        # выбрать из считанного первые 5 колонок
        df_cfg = df_load["Настройки"].iloc[:, [0, 1, 2, 3, 4]].copy()
        df_zai = df_load["Заявки"]

        conf = settings.get("конфиги", [])
        if dagmode is True:
            # res = stat(settfile)
            dconf = datetime.fromtimestamp(stat(settfile).st_mtime).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
        else:
            dconf = datetime.fromtimestamp(Path(settfile).stat().st_mtime).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
        conf.append(PureWindowsPath(settfile).name + " " + dconf)
        settings.update({"конфиги": conf})

        # форматирование конфигов.
        df_cfg.rename(
            columns={
                "Ключевое слово": "item",
                "Значение1": "sap",
                "Значение2": "year",
                "Значение3": "toonss",
                "Значение4": "bgt",
                # "Значение5": "protcl",
                # "Значение6": "protcl_d",
            },
            inplace=True,
        )

        df_zai.rename(
            columns={
                "Ключевое слово": "item",
                "Номер заявки": "sap",
                "Год": "year",
                "Рекласс": "toonss",
                "Бюджет": "bgt",
                "Протокол": "protcl",
                "Протокол дата": "protcl_d",
                "Заявочная": "szk",
                "вид ТМЦ": "vidtmc",
                "сп. Вовлечения": "spv",
            },
            inplace=True,
        )

        df_cfg["item"] = df_cfg["item"].str.lower()
        df_zai["item"] = df_zai["item"].str.lower()
        df_zai["toonss"] = df_zai["toonss"].str.upper()
        df_cfg["toonss"] = df_cfg["toonss"].str.upper()

        # выборка настроек из фрейма
        # выборка значения "путь"
        if addition is False:
            value = df_cfg[df_cfg["item"] == "путь"]["sap"].iloc[0]
            settings.update({"путь": value})

            if settings.get("путь", None) is None:
                print("не удалось прочитать настройку: Путь")
                sys.exit(0)

            # выборка значения "классификатор"
            value = df_cfg[df_cfg["item"] == "классификатор"]["sap"].iloc[0]
            settings.update({"классификатор": value})

            # выборка версиявп
            value = df_cfg[df_cfg["item"] == "версиявп"]["sap"].iloc[0]
            settings.update({"версиявп": value})
            gmonth = int(str(value).split("+")[0])
            datevp = date(datetime.now().year, gmonth, 1)
            # дата для фильтрации = версия возвратного плана 7+6=> 7-1 => 01.06.2023
            datevp = datevp.replace(month=datevp.month, day=1) - timedelta(days=1)

            # TODO проверить вычисление даты возвратного плана
            settings.update({"датавп": datevp})

            # выборка периода настроек
            # период отчета устанавливаем только по первой загрузке настроек
            value = df_cfg[df_cfg["item"] == "период"]["sap"].iloc[0]
            settings["Период"] = str(value).split(",")

            # путь к корректировкам
            value = df_cfg[df_cfg["item"] == "корректировки"]["sap"]
            # защита от отсутствия настройки
            if len(value.index) > 0:
                value = value.iloc[0]
                # принимаем параметр только из первого файла настроек
                # если значение 'корректировки' == 0 не принимаем в настройки
                if value != "0":
                    settings.update({"корректировки": value})

        #  выборка-установка префикса отчета
        value = df_cfg[df_cfg["item"] == "префикс"]["sap"].iloc[0]
        local_pref = str(value)
        if addition is True:
            settings["префикс"] = "Упр"
        else:
            settings["префикс"] = str(value)

        # выборка заявителей
        value = df_cfg[(df_cfg["item"] == "заявитель")]["sap"].to_list()
        list4 = filter.get("заявитель", []) + value
        filter.update({"заявитель": list4})
        list4 = None

        # выборка из фрейма заявок и присвоение их фильтру
        za_years = df_zai[df_zai["item"] == "заявка"]["year"].unique()
        for ii in za_years:
            za_1y = df_zai[(df_zai["item"] == "заявка") & (df_zai["year"] == ii)][
                "sap"
            ].to_list()
            list3 = filter.get(int(ii), []) + za_1y
            filter.update({int(ii): list3})

        # выборка чужих заявок для гашения предупреждения в логе по заявителю
        za_1y = df_zai[(df_zai["item"] == "заявкач")]["sap"].to_list()

        # выборка своих заявок которые не надо показывать для гашения предупреждения в логе по заявителю
        za_drop = df_zai[(df_zai["item"] == "чужая")]["sap"].to_list()

        print("список гашения", za_drop)

        list5 = filter.get("ЧужиеЗ", []) + za_1y
        filter.update({"ЧужиеЗ": list5})
        if len(za_drop) > 0:
            listd = filter.get("СброситьЗ", []) + za_drop
            filter.update({"СброситьЗ": listd})

        # добавление списка на рекласс
        for ii in ["МТР", "ОНСС"]:
            za_rcls = df_zai[(df_zai["item"] == "заявка") & (df_zai["toonss"] == ii)][
                "sap"
            ].to_list()

            if len(za_rcls) > 0:
                list4 = filter.get(ii.lower(), []) + za_rcls
                filter.update({ii.lower(): list4})
        za_rcls = None

        # df_cfg["year_d"] = ""
        df_cfg["otdel"] = local_pref
        # df_zai["year_d"] = ""
        df_zai["otdel"] = local_pref

        # переименование чужих заявок чтобы они попали в отбор
        df_zai["item"] = df_zai["item"].replace({"заявкач": "заявка"}, regex=True)

        # удаляем строки не входящие в шаблоны настроек
        dropv = [
            "бюджетс",
            "поставка без заявок месяц",
            "вывести наименование факт",
            "заявка",
            "контроль",
        ]

        df_cfg = df_cfg.drop(df_cfg[~df_cfg["item"].isin(dropv)].index)
        df_zai = df_zai.drop(df_zai[~df_zai["item"].isin(dropv)].index)

        if len(filtersdf.index) > 0:
            filtersdf = pd.concat([filtersdf, df_cfg, df_zai]).reset_index(drop=True)
        else:
            # блок удаления пустых колонок перед первым слиянием для гашения futurewarning
            # colmns = df_cfg.columns
            list_of_dfs = [df_cfg, df_zai]
            # now remove all columns from the dataframes which are empty or have all-NA
            cleaned_list_of_dfs = [df.dropna(axis=1, how="all") for df in list_of_dfs]
            filtersdf = (
                pd.concat(cleaned_list_of_dfs)
                # .reindex(columns=filtersdf.columns.union(colmns))
                .reset_index(drop=True)
            )

        # если цикл не закончился, следующий файл будет в режиме добавления
        addition = True
    filtersdf["year_d"] = pd.to_datetime(filtersdf["year"], errors="coerce")

    return settings, filter, filtersdf


def initdb(mode="file") -> str:
    if mode == "file":
        eng = create_engine("sqlite:///configs.db")
    return eng


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


def loadvp3(pathtovp: str) -> bool:
    """pathtovp - путь к каталогу или файлу с ВП.
    Конвертация ВП в датафрейм и сохранение в pickle. путь из настроек.
    Если путь это каталог делаем рекурсивный обход и загружаем последний по дате файл.
    возвращает true если загрузка прошла или прерывает выполнение"""
    global gl_import_df, gl_settings
    engine = initdb()
    vpp = Path(pathtovp)

    print(f"Путь в настройках: \n {vpp}")
    if Path.exists(vpp):
        print("путь существует")
        if Path.is_dir(vpp):
            print("путь это каталог, выбираем файлы *.xlsx")
            # list_of_files = glob.glob(vpp + "/*.xlsx")

            # нужен питон 3.13
            # list_of_files = list(Path(vpp).rglob("*.xlsx", case_sensitive=False))
            # list_of_files = list(Path(vpp).rglob("*.xlsx"))

            nfiled = datetime(1970, 1, 1)
            file_tl = None
            for file_info in scandir(Path(vpp)):
                if (
                    file_info.is_file()
                    and PureWindowsPath(file_info).suffix.lower() == ".xlsx"
                ):
                    print(f"Найден файл: {file_info.name}")
                    modified = datetime.fromtimestamp(file_info.stat().st_mtime)
                    if modified > nfiled:
                        nfiled = modified
                        file_tl = file_info
            print(f"Выбрал файл {file_tl.path}, {nfiled}")
            if file_tl is None:
                print(f"Не нашел файл .xlsx в каталоге {vpp}")
                sys.exit(1)
            # file_tl = max(list_of_files, key=lambda item: item.stat().st_mtime)
            # print(f"выбран последний по дате файл по маске *.xlsx -> \n{file_tl}")

        if Path.is_file(vpp):
            file_tl = vpp
            print(f"путь это файл: {file_tl}")

        if check_file_data(engine, nfiled, "ДатаВозвратногоПлана") is False:
            print("файл уже загружен")
        else:
            print("файл надо загружать")

            file = open(file_tl, mode="rb")
            workbook = CalamineWorkbook.from_filelike(file)
            wsname = workbook.sheet_names
            print(f"листы в книге: {file_tl.name}, {wsname}")

            wsnames = {ii.lower() for ii in wsname}

            # список имен листов с которых будет попытка загрузки ВП
            good_list = ["расшифровка", "лист1"]
            if bool(wsnames.intersection(good_list)):
                for index, ws in enumerate(wsname):
                    if ws.lower() in good_list:
                        print(
                            f"Старт загрузки файла: {file_tl.name}, лист: {wsname[index]}"
                        )
                        gl_import_df = pd.read_excel(
                            file,
                            sheet_name=wsname[index],
                            engine="calamine",
                        )
                        if cleanvp("", gl_import_df) is True:
                            print("Загрузка ВП завершена, обновляем версию")
                            save_file_data(engine, nfiled, "ДатаВозвратногоПлана")
                        # если сделали одну загрузку выходим из цикла
                        break
            else:
                print(f"Не нашел листов {wsnames}. Надо проверить файл {file_tl.name}")
                sys.exit(1)

    else:
        print("Путь не существует, надо проверить настройки")
        sys.exit(1)
    return True


def saveframe(
    frame: pd.DataFrame, tn: str, conn: sa.engine.Connection, modes="replace"
) -> int:
    """Cохранить фрейм в БД

    Args:
        frame (pd.DataFrame): dataframe
        tn (str): Имя таблицы для сохранения
        eng (sa.engine.Connection): connection

    Returns:
        int: _статус выполнения_
    """
    # чтобы не испортить фрейм переименованием заголовка при записи
    rename_flag = False

    if conn.engine.driver == "crate-python":
        # замена точек в наименовании колонок на шаблон
        frame.columns = frame.columns.str.replace(".", "_ТЧК_")
        rename_flag = True

        frame.to_sql(
            tn,
            conn,
            if_exists=modes,
            index=False,
            method=insert_bulk,
        )
        conn.exec_driver_sql(f"REFRESH TABLE {tn};")
    else:
        frame.to_sql(
            tn,
            conn,
            if_exists=modes,
            index=False,
        )
    if rename_flag:
        frame.columns = frame.columns.str.replace("_ТЧК_", ".")

    return 0


def loadframe(tn: str, conn: sa.engine.Connection) -> pd.DataFrame:
    """_загрузить фрейм из БД_

    Args:
        tn (str): _Имя таблицы для загрузки_
        eng (sa.engine.Connection): _connection_

    Returns:
        pd.DataFrame: _Загруженный датафрейм_
    """

    df2 = pd.read_sql_table(tn, conn)
    if conn.engine.driver == "crate-python":
        df2.columns = df2.columns.str.replace("_ТЧК_", ".")

    return df2


def cleanvp(uri_pg: str, lc_import_df: pd.DataFrame) -> bool:
    """очистка считанного фрейма и сохранение на диске или в БД"""
    # конвертация строки с именами колонок в имена колонок
    # pp = lc_import_df[lc_import_df.iloc[:, 0] == "/ перв"]
    # if len(pp.index) > 0:
    #     rown = int(pp.index[0])
    #     lc_import_df = lc_import_df.rename(columns=lc_import_df.iloc[rown]).loc[
    #         rown + 1 :
    #     ]

    if str(lc_import_df.columns[0]).casefold() != "/ перв":
        # поиск "/ перв". на выходе series
        fdf_c = lc_import_df.apply(
            lambda row: row.astype(str).str.contains("/ перв", case=False).any(),
            axis="index",
        )
        fdf_i = lc_import_df.apply(
            lambda row: row.astype(str).str.contains("/ перв", case=False).any(),
            axis="columns",
        )

        # print("нашел индексы", fdf_i.idxmax())
        # print("name=", fdf_c.idxmax(), "index=", lc_import_df.columns.get_loc(fdf_c.idxmax()))

        # преобразование найденого в индексы
        rowi = fdf_i.idxmax()
        coli = lc_import_df.columns.get_loc(fdf_c.idxmax())

        if (rowi == 0 and coli == 0) and (
            str(lc_import_df.iat[rowi, coli]).lower() != "/ перв"
        ):
            print("Колонка '/ перв' не найдена. Необходимо проверить файл ВП. Выход.")
            sys.exit(-1)
        else:
            # сбросить колонки слева, переименовать по найденной строке,
            # сбросить строки сверху
            lc_import_df = (
                lc_import_df.drop(lc_import_df.iloc[:, 0:coli], axis="columns")
                .rename(columns=lc_import_df.iloc[rowi])
                .loc[rowi + 1 :]
            )

    lc_import_df.rename(columns={lc_import_df.columns[0]: "/ САП"}, inplace=True)

    # переименование вариантов колонки СРОК
    clmns = lc_import_df.columns.to_list()
    for index, i in enumerate(clmns.copy()):
        clmns[index] = str.strip(str(clmns[index]))
        clmns[index] = "".join(clmns[index].splitlines())
        if clmns[index].lower() == "ожидаемый срок поставки":
            clmns[index] = "СРОК"
        elif clmns[index].lower() == "срок поставки":
            clmns[index] = "СРОК"
        elif clmns[index].lower() == "срок поставки ожид/факт":
            clmns[index] = "СРОК"
        elif clmns[index] == "Срок":
            clmns[index] = "СРОК"

    lc_import_df.columns = clmns

    goodcol = [
        "/ САП",
        "Вид деятельности",
        "Номер НПП",
        "Позиция НПП",
        "Дата Утверждения  НПП",
        "Наименование службы заказчика",
        "Наименование заявителя",
        # "Сокр оператор",
        "Оператор закупки",
        "Наименование заявки",
        "Полное имя материала",
        "ЕИ ввода",
        "Кол-во в ЕИ",
        "Цена, руб. с НДС",
        "Сумма, руб. с НДС",
        "Плановая дата поставки",
        "Кол-во по сводной заявке плановое",
        "Наименование факт",
        "Кол-во АС расч.",
        "Статус",
        "СРОК",
        "Сумма",
        "Класс",
        "Материал",
        "Доп. данные о материале",
        "Номер дог. документа",
        "Договор 1С",
        "Поставщик",
        "Исполнитель закупки",
        "Номер лота из отчета по лотам",
    ]

    # проверка на дубликат колонки статус. если да, берём вторую
    indexes = [i for i, x in enumerate(lc_import_df.columns) if x == "Статус"]
    print(f"Обработка дубликата колонки 'Статус' в столбцах: {indexes}")
    if len(indexes) >= 2:
        lc_import_df.columns.values[indexes[0]] = "Статус сводной"

    # выборка колонок по списку
    lc_import_df = lc_import_df[lc_import_df.columns.intersection(goodcol)]

    # удаление колонок дубликатов
    lc_import_df = lc_import_df.loc[:, ~lc_import_df.columns.duplicated()].copy()

    # формат колонки в текстовый вид, т.к. в ней есть мусорные даты
    lc_import_df["Номер лота из отчета по лотам"] = lc_import_df[
        "Номер лота из отчета по лотам"
    ].astype("string")

    # формат чисел для конвертации
    lc_import_df["Сумма"] = (
        lc_import_df["Сумма"]
        .astype("string")
        .str.replace("-", "0", regex=True)
        .str.replace(",", "", regex=True)
    )

    lc_import_df["Кол-во АС расч."] = (
        lc_import_df["Кол-во АС расч."]
        .astype("string")
        .str.replace(r"[^\d\.-]", "", regex=True)
    )

    # скорректировать формат колонки 'Кол-во АС расч.', 'Сумма': str к цифровому
    # exportv = lc_import_df[["/ САП", "Кол-во АС расч."]]
    # print("-----!----")
    # print(lc_import_df[lc_import_df["Кол-во АС расч."].astype("string") == "601,0.072"])
    lc_import_df[
        [
            "Позиция НПП",
            "Кол-во в ЕИ",
            "Цена, руб. с НДС",
            "Сумма, руб. с НДС",
            "Кол-во по сводной заявке плановое",
            "Кол-во АС расч.",
            "Сумма",
        ]
    ] = lc_import_df[
        [
            "Позиция НПП",
            "Кол-во в ЕИ",
            "Цена, руб. с НДС",
            "Сумма, руб. с НДС",
            "Кол-во по сводной заявке плановое",
            "Кол-во АС расч.",
            "Сумма",
        ]
    ].apply(pd.to_numeric, errors="coerce")

    # exportv2 = lc_import_df[["/ САП", "Кол-во АС расч."]]
    # exportv.to_csv("tanalyse.csv")
    # exportv2.to_csv("tanalyse2.csv")

    # lc_import_df["Сумма"] = (
    #     lc_import_df["Сумма"].str.replace(r"[^\d\.-]", "", regex=True).astype(float)
    # )
    # lc_import_df["Сумма"] = lc_import_df["Сумма"].astype("Float64")

    print(f"Строк до фильтрации по дате {len(lc_import_df.index)}")

    lc_import_df[["СРОК", "Дата Утверждения  НПП", "Плановая дата поставки"]] = (
        lc_import_df[["СРОК", "Дата Утверждения  НПП", "Плановая дата поставки"]].apply(
            pd.to_datetime, errors="coerce", format="%d/%m/%Y"
        )
    )

    dfna = lc_import_df[lc_import_df["СРОК"].isna()]

    lc_import_df = lc_import_df.dropna(subset=["СРОК"])
    print(f"Строк после фильтрации по дате {len(lc_import_df.index)}")

    if uri_pg == "":
        print("Запуск выгрузки DF в файлы")
        # соединение
        client = contc()
        client.command("CREATE DATABASE IF NOT EXISTS pandas")
        client = contc("pandas")
        intoclickhouse(client, dfna, "pickle_na")

        # dfna.to_parquet("pickle_na.parquet")
        dfna.to_pickle("pickle_na")
        lc_import_df.to_pickle("to_pickle")
        print("Выгрузка DF в файлы завершена")
    else:
        # timer("Запуск выгрузки DF в pickle", gl_startTime)
        print("Запуск выгрузки DF в SQL")
        engine = create_engine(uri_pg)
        with engine.connect() as conn:
            # dfna.to_sql("pickle_na", conn, if_exists="replace")
            saveframe(dfna, "pickle_na", conn, "replace")
            # lc_import_df.to_sql("to_pickle", conn, if_exists="replace")
            saveframe(lc_import_df, "to_pickle", conn, "replace")
        # timer("Выгрузка DF на SQL завершена", gl_startTime)
        print("Выгрузка DF на SQL завершена")
    return True


def contc(dbname="default") -> clickhouse_connect.driver.client:
    client = clickhouse_connect.get_client(
        host="192.168.5.17",
        port=8123,
        username="default",
        password="",  # Your password, if any
        database=dbname,
    )
    return client


def intoclickhouse(client, df, table_name):

    # 2. Map Pandas/Parquet dtypes to ClickHouse types (this is a simplified mapping)
    # A more robust implementation would handle nested types and specific ClickHouse types
    dtype_mapping = {
        "int64": "Int64",
        "float64": "Float64",
        "object": "String",
        "bool": "Bool",
        "datetime64[ns]": "DateTime64(3)",
        "str": "String",
        "Float64": "Float64",
        "timestamp[ns][pyarrow]": "DateTime64(3)",
        "double[pyarrow]": "Float64",
        "string[pyarrow]":"String",
    }
    # dft = df.select_dtypes("str")
    # df[dft.columns] = dft.apply(lambda x: x.fillna(""))

    df = df.convert_dtypes(dtype_backend="pyarrow")
    print(df.dtypes)
    
    columns_sql = []
    for col_name, dtype in zip(df.columns, df.dtypes):
        ch_type = dtype_mapping.get(
            str(dtype), "String"
        )  # Default to String if not found
        columns_sql.append(f"`{col_name}` {ch_type}")

    schema_sql = ", ".join(columns_sql)

    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {schema_sql}
    ) ENGINE = MergeTree()
    ORDER BY tuple(); -- Use an appropriate ORDER BY clause for your data
    """

    # 3. Connect to ClickHouse and execute the CREATE TABLE statement
    # Replace with your actual ClickHouse connection details

    # Drop the table if it exists for a clean run
    client.command(f"DROP TABLE IF EXISTS {table_name}")

    client.command(create_table_sql)

    print(f"Table '{table_name}' created with schema inferred from Parquet file.")

    # Optional: Insert the data using the client's insert method
    # client.insert_df(table_name, df)
    client.insert_df_arrow(table_name, df)

    print(f"Data from dataframe inserted into clickhouse table '{table_name}'.")


def loadfile(
    filename: str | bytes,
    clumns: dict,
    sh_name: str,
    header_row=[],
    ucols=None,
    nrws=15,
) -> pd.DataFrame:
    """Считать excel файл по настройкам, вернуть датафрейм"""

    if header_row == []:
        res = pd.read_excel(filename, nrows=15, sheet_name=sh_name, engine="calamine")

    else:
        res = pd.read_excel(
            filename,
            nrows=nrws,
            sheet_name=sh_name,
            header=header_row,
            engine="calamine",
        )
        # обнуление уровней с пустым значением
        for nl in range(0, res.columns.nlevels):
            res = res.rename(columns=lambda x: "" if "Unnamed" in x else x, level=nl)

        if len(header_row) > 1:
            # ветка работы с мультиндексом. объединение в строку
            res.columns = [
                "_".join(filter(None, a)) for a in res.columns.to_flat_index()
            ]
        lisc = res.columns.to_list()

        # список найденных имён колонок
        resl = []
        # дикт для переименования найденных колонок
        renmd = {}

        for li in clumns:
            if isinstance(li, int):
                resl.append(lisc[li - 1])
                if clumns[li] != "":
                    # нет проверки выхода за правую границу массива
                    renmd[lisc[li - 1]] = clumns[li]
            else:
                dfit = next((x for x in lisc if x.find(li) > -1), "Not found")
                if dfit != "Not found":
                    resl.append(dfit)
                    renmd[dfit] = clumns[li]
                else:
                    print(f'Ошибка. Не нашел колонку "{li}" в файле: {filename}')
                    sys.exit()

        # обрезка считанного по выбранным колонкам
        res = res[resl]

        res.rename(
            columns=renmd,
            inplace=True,
        )

    return res


def get_files(dirs: str, dagmode: bool, dagmode_env: dict, prefix_smb: str) -> list:
    """
    dirs - Каталог для поиска файлов. Выбирает файлы с расширением .xlsx .prqt.
    дата файла .prqt должна быть больше .xlsx
    возвращает список для считывания

    """
    res = []
    fres = []
    fmd = {}

    if dagmode:
        fulldir = PureWindowsPath(
            prefix_smb + str(dagmode_env["confdir"])
        ).parent.joinpath("факт\помесячно")

        register_session(
            dagmode_env["uri_samba"]["_host"],
            username=dagmode_env["uri_samba"]["username"],
            password=dagmode_env["uri_samba"]["password"],
        )

        for file_info in scandir_smb(fulldir):
            # print(f"Найден файл: {file_info.path}")
            suffix = PureWindowsPath(file_info.path).suffix.lower()
            if PureWindowsPath(file_info.path).name[0] != "~" and (
                suffix == ".xlsx" or suffix == ".prqt"
            ):
                res.append(PureWindowsPath(file_info.path).as_posix())
                fmd[PureWindowsPath(file_info.path).as_posix()] = (
                    datetime.fromtimestamp(file_info.smb_info.change_time.timestamp())
                )
    else:
        # сбор файлов c файлового доступа
        for li in walk(dirs):
            dirlist = li[2]
            for lii in dirlist:
                suffix = PureWindowsPath(lii).suffix.lower()
                if lii[0] != "~" and (suffix == ".xlsx" or suffix == ".prqt"):
                    fullpath = PureWindowsPath(li[0]).joinpath(lii).as_posix()
                    res.append(fullpath)
                    fmd[fullpath] = datetime.fromtimestamp(stat_file(fullpath).st_mtime)

    for li in res:
        if PureWindowsPath(li).suffix.lower() == ".xlsx":
            datep = fmd.get(
                PureWindowsPath(li).with_suffix(".prqt").as_posix(),
                datetime(1970, 1, 1, 0, 0, 1),
            )
            if PureWindowsPath(li).with_suffix(".prqt").as_posix() not in res or (
                fmd[li] >= datep
            ):
                if dagmode:
                    print(
                        f"Берем {li}, дата prqt {datep:%Y.%m.%d:%H:%M:%S}, дата .xlsx {fmd[li]:%Y.%m.%d:%H:%M:%S}"
                    )
                fres.append(li)

        else:
            datex = fmd.get(
                PureWindowsPath(li).with_suffix(".xlsx").as_posix(),
                datetime(1970, 1, 1, 0, 0, 1),
                # datetime(1970, 1, 1, 0, 0, 1, tzinfo=None).replace(tzinfo=None),
            )
            if fmd[li] >= datex:
                if dagmode:
                    print(
                        f"Берем {li}, дата prqt {fmd[li]:%Y.%m.%d:%H:%M:%S}, дата .xlsx {datex:%Y.%m.%d:%H:%M:%S}"
                    )
                fres.append(li)
    return fres


def convert_fakt_prqt(ftc: list, dagmode: bool) -> list:
    """конвертирует .xlsx файлы факт в формат parquet
    считывает лист "Общий список"
    Возвращает скорректированный список файлов
    """
    res_ftc = []
    sheetname = "Общий список"

    """
    # вариант загрузки по номерам колонок
    loaddict1 = {
        2: "НомЗаяв",
        3: "Позиция",
        21: "Отдел(sap)",
        40: "№ Сводной заявки",
        41: "№ поз",
        53: "Номенклатура фактическая",
        119: "Договор с пост (ОВПП)",
        204: "№ Поставщика",
        210: "№ С/Ф поставщика",
        212: "Дата СФ",
        213: "Дата проводки СФ",
        218: "Кол-во Итого по позиции спец-и",
        219: "Сумма c НДС",
        274: "Состояние позиции",
    }
    """
    # вариант загрузки по объединенным именам колонок
    loaddict2 = {
        "Первичная заявка_№ Первичн. заявки на закупку": "НомЗаяв",
        "Первичная заявка_№ поз": "Позиция",
        "Первичная заявка_Менеджер": "Отдел(sap)",
        "Первичная заявка_Статус Позиции Первичной Заявки": "Статус_Позиции",
        "Сводная заявка_№ Сводной заявки": "№ Сводной заявки",
        "Сводная заявка_№ поз": "№ поз",
        "Сводная заявка_Номенклатура фактическая": "Номенклатура фактическая",
        "Договор с Поставщиком": "Договор с пост (ОВПП)",
        "Поступление (Вх. СФ) ОП_№ Поставщика": "№ Поставщика",
        "Поступление (Вх. СФ) ОП_№ С/Ф поставщика": "№ С/Ф поставщика",
        "Поступление (Вх. СФ) ОП_Дата СФ": "Дата СФ",
        "Поступление (Вх. СФ) ОП_Дата проводки СФ": "Дата проводки СФ",
        "Поступление (Вх. СФ) ОП_Кол-во Итого по позиции спец-и": "Кол-во Итого по позиции спец-и",
        "Поступление (Вх. СФ) ОП_Сумма Итого по позиции спец-и": "Сумма c НДС",
        "Состояние позиции": "Состояние позиции",
        "Уведомления об отгрузке ОП_Склад первичного прихода": "Склад_прихода",
    }

    # сдвиг для первой строки заголовка
    ind0 = 3

    for li in ftc:
        if PureWindowsPath(li).suffix.lower() == ".prqt":
            res_ftc.append(li)
        else:
            print(f"Конвертация {li}")
            if dagmode:
                filetl = open_file(li, mode="rb", share_access="rw")
            else:
                filetl = li

            res = loadfile(
                filetl,
                loaddict2,
                sheetname,
                [ind0 + 1, ind0 + 2, ind0 + 3, ind0 + 4],
                nrws=None,
            )

            # res = res.dropna(
            #     subset="Кол-во Итого по позиции спец-и", axis="index"
            # ).ffill(axis="index")

            res = res.dropna(
                subset=["НомЗаяв", "Кол-во Итого по позиции спец-и"],
                axis="index",
                how="all",
            )
            # tmpf = (
            #     str(PureWindowsPath(li).parent)
            #     + "\\"
            #     + str(PureWindowsPath(li).stem)
            #     + "-t.xlsx"
            # )
            # res
            # ffill(axis="index")

            # df.loc[:, df.columns != 'b']

            res.loc[
                :, ~res.columns.isin(["Кол-во Итого по позиции спец-и", "Сумма c НДС"])
            ] = res.loc[
                :, ~res.columns.isin(["Кол-во Итого по позиции спец-и", "Сумма c НДС"])
            ].ffill(axis="index")

            # res.to_excel(tmpf)
            # print(tmpf)

            # преобразование формата числа
            res[["Сумма c НДС", "Кол-во Итого по позиции спец-и"]] = (
                res[["Сумма c НДС", "Кол-во Итого по позиции спец-и"]]
                .replace({r"\.": "", ",": "."}, regex=True)
                .astype("Float64")
            )

            filepath = PureWindowsPath(li).with_suffix(".prqt")

            if dagmode:
                wfile = open_file(filepath, mode="wb")
                res.to_parquet(wfile)
                wfile.close()
            else:
                res.to_parquet(filepath)

            res_ftc.append(filepath)

    return res_ftc


def set_xl_styles(workbook) -> dict:
    """Получает workbook, возвращает dict с установленными стилями"""

    l_style = {}
    l_style["gl_style_money"] = workbook.add_format(
        {
            "num_format": "#,##0.00;-#,##0.00;-",
            "align": "vcenter",
        }
    )

    l_style["gl_style_moneyRed"] = workbook.add_format(
        {
            "num_format": "#,##0.00;-#,##0.00;-",
            "align": "vcenter",
            "font_color": "black",
            "bg_color": "red",
        }
    )
    l_style["gl_style_money2"] = workbook.add_format(
        {"num_format": '_* #,##0.00;[Red]* -#,##0.00;_* "-"??'}
    )
    l_style["gl_style_moneyB"] = workbook.add_format(
        {"num_format": "#,##0.00;-#,##0.00;-", "bold": True}
    )
    l_style["gl_style_moneyBr"] = workbook.add_format(
        {"num_format": "#,##0.00;[Red]-#,##0.00;-", "bold": True, "font_color": "blue"}
    )
    l_style["gl_style_hl"] = workbook.add_format(
        {"bold": True, "font_color": "blue", "align": "right"}
    )

    l_style["gl_style_cell_format_bold"] = workbook.add_format({"bold": True})
    l_style["gl_style_cell_format_bold_border"] = workbook.add_format(
        {"bold": True, "border": 1}
    )
    l_style["gl_style_cell_format_bold_bd"] = workbook.add_format(
        {"bold": True, "bottom": 1}
    )

    l_style["gl_style_text_wrap"] = workbook.add_format(
        {
            "text_wrap": True,
            "align": "vcenter",
        }
    )

    l_style["gl_style_border_td"] = workbook.add_format({"top": 1, "bottom": 1})
    l_style["gl_style_border_t"] = workbook.add_format({"top": 1})
    l_style["gl_style_border_l"] = workbook.add_format({"left": 1})
    l_style["gl_style_cell_bc"] = workbook.add_format(
        {
            "bold": True,
            "align": "center",
            "bg_color": "#FFC000",
        }
    )
    l_style["gl_style_text_wrap"] = workbook.add_format(
        {
            "text_wrap": True,
            "align": "vcenter",
        }
    )

    return l_style


def check_df(
    df: pd.DataFrame,
    check_list=[],
    soft_rename={},
    hard_rename={},
) -> pd.DataFrame:
    """Проверка датафрейма на соответствие формату

    Args:
        df (pd.DataFrame): Входной датафрейм
        check_list (list, optional): Список колонок для проверки. Defaults to [].
        soft_rename (dict, optional): Список колонок для переименования. Если имя колонки не найдено, то пропускается. Defaults to {}.
        hard_rename (dict, optional): Список колонок для переименования. Если имя колонки не найдено, то выбрасывается исключение. Defaults to {}.

    Returns:
        pd.DataFrame: Исправленный датафрейм
    """

    # переименование, ошибки в игнор
    if len(soft_rename) > 0:
        df.rename(
            columns=soft_rename,
            inplace=True,
            errors="ignore",
        )

    # переименование, ошибки raise
    if len(hard_rename) > 0:
        df.rename(
            columns=hard_rename,
            inplace=True,
        )

    return df


# ----------start--------------
if __name__ == "__main__":
    print("модуль с общими процедурами")
