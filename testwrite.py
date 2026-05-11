import pandas as pd
import numpy as np

from shared_chouse import timer
from xlsxwriter.workbook import Workbook


def save_base(ldf: pd.DataFrame, pathtofile: str):
    ldf.to_excel(pathtofile, index=False, engine="xlsxwriter")


def save_2(ldf: pd.DataFrame, pathtofile: str, time):

    # workbook = Workbook(pathtofile, {"nan_inf_to_errors": True})
    workbook = Workbook(pathtofile)
    worksheet = workbook.add_worksheet()

    worksheet.write_row(0, 0, [col for col in ldf.columns])
    ldf = ldf.fillna(np.nan).replace({np.nan: None})
    timer("===конвертация завершена", time)
    for index, row in ldf.iterrows():
        worksheet.write_row(index + 1, 0, [col for col in row])  # type: ignore

    workbook.close()


def save_22(ldf: pd.DataFrame, pathtofile: str, time):
    workbook = Workbook(pathtofile)

    worksheet = workbook.add_worksheet()
    ldf = ldf.fillna(np.nan).replace({np.nan: None})
    timer("===конвертация завершена", time)

    index = 1
    worksheet.write_row(0, 0, ldf.columns.to_list())
    for row in ldf.itertuples(name=None):
        worksheet.write_row(index, 0, list(row)[1:])
        index += 1

    workbook.close()


def save_3(ldf: pd.DataFrame, pathtofile: str):

    from rustpy_xlsxwriter import FastExcel

    FastExcel(pathtofile).sheet("Data", ldf).save()


# gst = timer("===запуск чтения parquet файла")
df = pd.read_parquet("base.parquet")
df.info()
# timer("===запуск чтения parquet файла", gst)


print("\n ===метод xlsxwriter")
tim = timer("===запуск записи. метод xlsxwriter")
save_base(df, "out/base.xlsx")
timer("===запуск записи. метод xlsxwriter", tim)

# print("\n ===метод ws writerow2")
# tim = timer("===запуск записи. ws writerow2")
# save_2(df, "out/base2.xlsx", tim)
# timer("===запуск записи. ws writerow2", tim)

print("\n ===метод ws writerow22")
tim = timer("===запуск записи. метод ws writerow22")
save_22(df, "out/base22.xlsx", tim)
timer("===запуск записи. метод ws writerow22", tim)

# print("\n ===метод rustpy_xlsxwriter")
# tim = timer("===запуск записи. метод rustpy_xlsxwriter")
# save_3(df, "out/base22.xlsx")
# timer("===запуск записи. метод rustpy_xlsxwriter", tim)
