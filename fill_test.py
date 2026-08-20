import pandas as pd

# тест для настройки стратегии заливки пустых данных в выгрузках

# df = pd.read_excel(
#     r"C:\uv\pandas-ch\SAP_in\__stock-2026-06{до}.xlsx", engine="calamine"
# )
# df.to_parquet(r"C:\uv\pandas-ch\SAP_in\filez.parquet")

df = pd.read_parquet(r"C:\uv\pandas-ch\SAP_in\filez.parquet")
df = df[df["Наименование подразделения"] == "Отдел информационных технологий"]
df_clone = df.copy()

df = df.sort_values(["Материал", "Партия", "НомЗаяв"])

print(df)

cvl = df["НомЗаяв"].count()
print("заполненных значений до заливки", cvl)
df[["НомЗаяв", "Позиция"]] = df.groupby(["Материал", "Партия"])[
    ["НомЗаяв", "Позиция"]
].ffill()
cvl2 = df["НомЗаяв"].count()
print(
    f"заполненных значений {cvl2} изменение {cvl2 - cvl}",
)


selected = [
    "Материал",
    "Партия",
    "НомЗаяв",
    "Позиция",
    "ФИО менеджера",
    "Наименование подразделения",
    # "Обознач. склада",
    "Обозначение",
    "КатегЗапас",
    "Наименование категории запаса",
    "Цена",
    # "Количество",
    "Доступное кол-во",
    "Стоимость",
    "ПрзСрВвлЗп",
    "ДатаПервПост",
    "Краткий текст материала",
]
# df.to_excel(r"C:\uv\pandas-ch\SAP_in\123\out.xlsx", engine="xlsxwriter")
df[selected].to_excel(
    r"C:\uv\pandas-ch\SAP_in\123\out_filtred.xlsx", engine="xlsxwriter"
)
df_clone[selected].to_excel(
    r"C:\uv\pandas-ch\SAP_in\123\out_clone_filtred.xlsx", engine="xlsxwriter"
)
