# формирование плоского справочника по цвету
from pathlib import Path

import pandas as pd
from openpyxl import load_workbook

if __name__ == "__main__":
    # 1. Загружаем Excel-файл (укажите путь к вашему файлу)
    ppath = r"R:\03. ЗГД_ГИ\07. УМАИТТ\06. Общая\Документация к рассмотрению\ВКС модернизация\отчеты вп\tools\import\xlsx\Справочник сотрудников-1.xlsx"
    wb = load_workbook(
        ppath,
        data_only=True,
    )

    df = pd.read_excel(
        ppath,
        sheet_name="Справочник сотрудников",
        engine="openpyxl",
        header=None,
    )
    # 2. Выбираем лист
    sheet = wb.worksheets[1]

    # Функция для вывода информации о цвете
    # def print_cell_color(cell_address):
    #     cell = sheet[cell_address]

    #     # Получаем объект заливки
    #     fill = cell.fill

    #     # Проверяем тип заливки (если solid, то цвет задан пользователем)
    #     if fill and fill.fill_type == "solid":
    #         # Получаем HEX-код цвета (обычно в формате ARGB, например 'FF00FF00')
    #         color_hex = fill.start_color.index
    #         print(f"Ячейка {cell_address}: цвет заливки HEX = {color_hex}")
    #     else:
    #         print(
    #             f"Ячейка {cell_address}: нет сплошной заливки (цвет по умолчанию/белый)"
    #         )

    # Пример проверки конкретных ячеек
    # print_cell_color("A1")
    # print_cell_color("B2")

    # Обход всех ячеек в диапазоне
    print("------------------")
    print("\nПроверка диапазона ячеек:")
    level1 = ""
    level2 = ""
    level3 = ""
    level4 = ""
    # переименование колонок
    df.columns = df.iloc[1]
    df["level1"] = ""
    df["level2"] = ""
    df["level3"] = ""
    df["level4"] = ""
    print(df.shape)
    print("------------------")
    for row in sheet.iter_rows(min_row=1, max_row=sheet.max_row, min_col=1, max_col=1):
        for cell in row:
            # if cell.fill and cell.fill.fill_type == "solid":
            print(
                f"Координаты: {cell.coordinate} | Цвет: {cell.fill.start_color.index} |Узор: {cell.fill.fill_type},{cell.fill.fgColor.rgb},{cell.fill.bgColor.rgb}| {cell.value}"
            )
            # print("Тип заполнения (fill_type / patternType):", fill.fill_type)
            # print("Цвет узора (fgColor.rgb / start_color):", fill.fgColor.rgb if fill.fgColor else None)
            # print("Цвет фона (bgColor.rgb / end_color):", fill.bgColor.rgb if fill.bgColor else None)

            if cell.fill.start_color.index == "FFFFFF00":
                # отдел
                if cell.fill.fill_type == "solid":
                    level3 = cell.value
                    level4 = ""
                else:
                    level4 = cell.value

            elif cell.fill.start_color.index == "FFFABF8F":
                # упр
                level2 = cell.value
                level3 = ""
            elif cell.fill.start_color.index == "FFF79646":
                # згд
                level1 = cell.value
                level2 = ""
                level3 = ""

            df.iat[cell.row - 1, 7] = level1
            df.iat[cell.row - 1, 8] = level2
            df.iat[cell.row - 1, 9] = level3
            df.iat[cell.row - 1, 10] = level4

    print("------------------\nИтоговый фрейм")
    print(df.dtypes)

    # print(df)
    # print("------------------")
    # print(df[df.columns[0]].head(25))
    df = df.dropna(subset="ФИО")
    df.to_excel("Спр2.xlsx", engine="xlsxwriter", index=True)
    print(f"Выполнено. выгружено в файл {Path('Спр2.xlsx').resolve()!s}")
