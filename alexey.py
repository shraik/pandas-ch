from pathlib import Path

import pandas as pd
from pandas.api.typing.aliases import ArrayLike


def hyperlink(param: dict):
    """Добавление гиперссылок назад на листы

    Args:
        param (dict): Словарь с параметрами
    """

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


def initexcel(pathtofile: str) -> pd.ExcelWriter:
    """Инициализация excel файла для записи. Настройка стилей.

    Args:
        pathtofile (str): Путь к файлу для записи

    Returns:
        pd.ExcelWriter: Excel writer.
    """
    global gl_format0, gl_format1, gl_link_format, gl_wrap_format  # ty: ignore[unresolved-global]

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


def oglavlenie(writerl: pd.ExcelWriter, addrl: ArrayLike, df_l: pd.DataFrame):

    # страница оглавления
    ws = writerl.book.add_worksheet("Оглавление")

    for position, addr in enumerate(addrl):
        if position % 50 == 0:
            print(f"Выведено страниц {position}", end="\r")

        ffilter = df_l[df_l["Местоположение_БД"] == addr]

        shname = "addr_" + str(position)
        ws.write_url(
            position,
            0,
            "internal:" + shname + "!A1",
            string=addr,
        )
        ws.write_number(position, 1, len(ffilter.index))

        ffilter.to_excel(writerl, sheet_name=shname)

        gl_back_addr[shname] = chr(ord("A")) + str(position + 1)
        gl_back_addr[shname + "_p"] = "Оглавление"

    ws.autofit()

    params = {
        "writer": writerl,
        "префиксл": "addr_",
        "страница": "Оглавление",
    }
    print("Расстановка обратных ссылок")
    hyperlink(params)


if __name__ == "__main__":
    # словарь обратных адресов
    gl_back_addr = {}

    # df = pd.read_excel(
    #     r"R:\source\python\alexey\invent_v2.xlsx",
    #     sheet_name="БД_1С_СП_инв. описи",
    #     engine="calamine",
    # )

    # # преобразуем лист в паркет, для быстрого считывания при отладке
    # df.to_parquet("invent_v2.parquet")

    df = pd.read_parquet("invent_v2.parquet")
    print(df)

    # выборка уникальных адресов
    addrlist = df["Местоположение_БД"].unique()
    print(f"Уникальных адресов {len(addrlist)}")

    # инициализируем выходной файл
    repfile = "out/alex2.xlsx"
    gl_writer = initexcel(repfile)

    # добавляем оглавление и расшифровки
    oglavlenie(gl_writer, addrlist, df)

    print("Сохранение файла")
    gl_writer.close()

    reptxt = f"Отчет сформирован в файле: '{Path(repfile).resolve()!s}'"
    print(reptxt)
