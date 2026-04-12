import pandas as pd
import re
from pathlib import Path
import sys
from shared_chouse import (
    contc,
    # save_file_data_ch,
    intoclickhouse,
    # check_file_data_ch,
    load_ch,
    # load_mol_сh,
    timer,
)
import configparser
from joblib import Parallel, delayed


def initexcel(pathtofile: str):
    writer = pd.ExcelWriter(
        pathtofile,
        mode="w",
        engine="xlsxwriter",
        date_format="dd/mm/yyyy",
        datetime_format="dd/mm/yyyy",
        # engine_kwargs={"options": {"strings_to_urls": True}},
    )
    return writer


gl_writer: pd.ExcelWriter


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


def extract(sap_ost: pd.DataFrame, c1_ost: pd.DataFrame) -> pd.DataFrame:

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
    c1_ost = pd.merge(
        left=c1_ost, right=sap_ost, how="left", left_on="key", right_on="key"
    )

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

    mask = c1_ost["Наименование категории запаса"].isna()
    c1_ost.loc[mask, "Наименование категории запаса"] = (
        "Запасы под потребность текущего периода"
    )

    # вывести результат в файл
    print("Датафрейм для записи в выходной файл:")
    print(c1_ost.info())

    # return c1_ost, mol_file
    return c1_ost


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
    # res = res.dropna(axis="columns", how="all")

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
    DATA_C1 = "C1_in"

    if load:
        startTime = timer(name="Начало чтения входных файлов")

        tasks = [
            delayed(pdread_sap)(DATA_SAP),
            delayed(pdread_c1)(DATA_C1),
        ]
        results = Parallel(n_jobs=-1)(tasks)
        timer("Чтение завершено.", startTime)

        # sys.exit(0)

        c1_ost = extract(results[0], results[1][0])  # type: ignore
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
        # tmp_writer.close()
        # timer("Завершена запись в промежуточный файл", startTime)
        # print(f"Промежуточный файл сохранен: '{str(Path(filenametosave).resolve())}'")

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

    # c2_df = load_ch(gl_client, "c1_sap_ost_flat", "\"Версия2\"='2026-02-28'")
    c2_df = load_ch(gl_client, "c1_sap_ost_flat")
    timer("Читаем из clickhouse, таблицу c1_sap_ost_flat", startTime)

    # print(c2_df)
    print("====считанная таблица====")
    print(c2_df.info())
    # print("====")
    # print(c2_df.dtypes)

    repfile = "out/report.xlsx"
    gl_writer = initexcel(repfile)

    startTime = timer(name="Формирование выборки")
    # формирование выходного файла
    report(repfile, c2_df, toch=True, client=gl_client)
    timer("Формирование выборки", startTime)

    if gl_client.ping():
        gl_client.close()
        print("\nConnection to ClickHouse closed.")

    if gl_writer is not None:
        gl_writer.close()

    print(f"Отчет сформирован в файле: '{str(Path(repfile).resolve())}'")
    timer("Итого времени выполнения скрипта", Main_startTime)


def report(pathtofile: str, dfl: pd.DataFrame, toch=False, client=None):
    """Формирование выходного отчета. Запись промежуточной таблицы в Clickhouse

    Args:
        pathtofile (str): Путь к файлу для записи
        dfl (pd.DataFrame): датафрейм
        toch (bool, optional): Флаг записи в БД. Defaults to False.
    """
    global gl_writer

    # проверка и создание выходного каталога
    folder_path = Path(pathtofile).parents[0]
    folder_path.mkdir(parents=True, exist_ok=True)

    workbook = gl_writer.book
    workbook.add_worksheet("base")  # pyright: ignore[reportAttributeAccessIssue]
    workbook.add_worksheet("filter")  # pyright: ignore[reportAttributeAccessIssue]
    # workbook.add_worksheet("Суммы")

    goodlist = [
        "Счет",
        "Номенклатура / ОС",
        "КСМ",
        "Единица измерения",
        "Склад / Контрагент / Работник",
        "Начальный остаток_Количество",
        "Приход_Количество",
        "Приход_Сумма (без НДС)",
        "Приход_в т.ч. сумма доп. расходов",
        "Расход_Количество",
        "Расход_Сумма (без НДС)",
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
    ]
    dfl_s = dfl[goodlist]

    # записать в excel выборку колонок
    dfl_s.to_excel(gl_writer, sheet_name="base", index=False)

    # фильтр по "Наименование подразделения"
    podr_sap = [
        "Отдел автоматизированных систем управления технологическим процессом",
        "Отдел телекоммуникаций",
        "Отдел информационных технологий",
        "Управление метрологии, автоматизации и информационных технологий и телекоммуникаций (не использовать)",
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

    # сбросить пустые строки по набору столбцов
    empty_f = [
        "Начальный остаток_Количество",
        "Приход_Количество",
        "Приход_Сумма (без НДС)",
        "Приход_в т.ч. сумма доп. расходов",
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
    dfl_s[empty_f] = dfl_s[empty_f].replace(0.0, pd.NA)
    dfl_s = dfl_s.dropna(subset=empty_f, axis="index", how="all")

    # print("==na==")
    # print(dfl_s[dfl_s["Склад / Контрагент / Работник"] == "МОЛ ЦАП"][empty_f])

    # записать в excel выборку колонок
    dfl_s.to_excel(gl_writer, sheet_name="filter", index=False)

    if toch:
        intoclickhouse(
            client,
            dfl_s,
            "c1_ost_filter",
        )

    # генерация и вывод сводной
    param_sum = [
        "Конечный остаток_Сумма (без НДС)",
        "Конечный остаток_Итого с ТЗР (без НДС)",
    ]
    dfl_s[param_sum] = dfl_s[param_sum].fillna(0.0)
    param_ind = [
        "Вид деят 1с",
        "Склад / Контрагент / Работник",
        # "ФИО менеджера",
        "Наименование подразделения",
        "Наименование категории запаса",
    ]
    # заполнить пустые значения в "Наименование категории запаса" (из 1с) на "Запасы под потребность текущего периода"
    dfl_s["Наименование категории запаса"] = dfl_s[
        "Наименование категории запаса"
    ].fillna("Запасы под потребность текущего периода")

    # свертка по подразделениям
    table = pd.pivot_table(
        dfl_s,
        values=param_sum,
        # "pivot_sum": ["Расход. Сумма (без НДС)", "Остаток (без НДС)"],
        index=param_ind,
        # "pivot_ind": ["Наименование подразделения", "Вид деят"],
        aggfunc="sum",
        # dropna=False,
    )

    table = table.reindex(param_sum, axis=1)
    table.reset_index(inplace=True)

    table.to_excel(gl_writer, sheet_name="Сводная", index=False)
    workbook.get_worksheet_by_name("Сводная").autofit()  # type: ignore


if __name__ == "__main__":
    print(
        "================================================================Запуск скрипта===="
    )
    start_parellel()
