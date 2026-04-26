from datetime import date, datetime

import pandas as pd

# import re
from pathlib import Path
from typing import Optional
from pandera import Timestamp
import pandera.pandas as pa

# import sys
# from clickhouse_connect import driver as ch_driver

# from xlsxwriter import workbook
from shared_chouse import (
    # contc,
    # save_file_data_ch,
    # intoclickhouse,
    # check_file_data_ch,
    # load_ch,
    # load_mol_сh,
    timer,
)
# import configparser
# from joblib import Parallel, delayed


def find_latest_file(directory: str, pattern: str) -> str:
    """Находит самый последний измененный файл в каталоге по заданному шаблону."""
    try:
        dir_path = Path(directory)
        files = list(dir_path.glob(pattern))
        if not files:
            print(
                f"Ошибка: Не найдено файлов по шаблону '{pattern}' в каталоге '{directory}'."
            )
            return ""

        latest_file = max(files, key=lambda p: p.stat().st_mtime)
        print(
            f"Найден самый новый файл по шаблону '{pattern}' в каталоге '{directory}':\n{str(Path(latest_file).resolve())}"
        )
        return str(latest_file)
    except FileNotFoundError:
        print(f"Ошибка: Каталог '{directory}' не найден.")
        return ""
    except Exception as e:
        print(f"Произошла непредвиденная ошибка: {e}")
        return ""


def pdread_sap(DATA_SAP) -> pd.DataFrame:
    """Для использования с joblib. Загрузка файла SAP"""
    print("--выбираем файл с остатками SAP---")
    # filesap = find_latest_file(DATA_SAP, "*.xlsx")
    filesap = find_latest_file(DATA_SAP, "*.parquet")

    print(f"--читаем SAP файл:\n{filesap}")
    # return pd.read_excel(filesap, engine="calamine")
    return pd.read_parquet(filesap)


def typecheck(df: pd.DataFrame):
    # снятие ограничений на ширину и высоту вывода на экран
    pd.set_option("display.max_rows", None)
    pd.set_option("display.max_columns", None)
    # pd.reset_option("display.max_rows")
    # pd.reset_option("display.max_columns")

    goodcols = [
        "БЕ",
        "Завод",
        "Материал",
        "Полное имя материала",
        "ПервДатПр",
        "Стоимость",
    ]
    df = df[goodcols]
    print(df.dtypes)

    # define a schema
    schema = pa.DataFrameSchema(
        {
            "БЕ": pa.Column(str),
            "Завод": pa.Column(int),
            "Материал": pa.Column(int),
            "Полное имя материала": pa.Column(str),
            "ПервДатПр": pa.Column(pa.dtypes.DateTime),
            # "column1": pa.Column(int, pa.Check.ge(0)),
            # "column2": pa.Column(float, pa.Check.lt(10)),
            # "column3": pa.Column(
            #     str,
            #     [
            #         pa.Check.isin([*"abc"]),
            #         pa.Check(lambda series: series.str.len() == 1),
            #     ]
            # ),
        }
    )
    print("=========pandera=========")
    print(schema.validate(df.head(50)))


if __name__ == "__main__":
    print(
        "================================================================Запуск скрипта===="
    )

    DATA_SAP = "SAP_in"
    global_st = timer("Запуск считывания файла")
    resdf = pdread_sap(DATA_SAP)
    timer("Считано", global_st)
    typecheck(resdf)
