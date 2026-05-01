# проверка возможности считывания стилей
from python_calamine import CalamineWorkbook

workbook = (
    CalamineWorkbook.from_path("вырезка/1с-2026-02-вырезка.xlsx")
    .get_sheet_by_name("TDSheet")
    .to_python(skip_empty_area=False)
)

# print(workbook)
wb2 = CalamineWorkbook.from_path("вырезка/1с-2026-02-вырезка.xlsx")
ww = wb2.sheets_metadata

print(ww)

# [
# [",  ",  ",  ",  ",  ",  "],
# ["1",  "2",  "3",  "4",  "5",  "6",  "7"],
# ["1",  "2",  "3",  "4",  "5",  "6",  "7"],
# ["1",  "2",  "3",  "4",  "5",  "6",  "7"],
# ]
