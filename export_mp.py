import pandas as pd
import asyncio
from concurrent.futures import ThreadPoolExecutor
import functools
from datetime import datetime


def timer(name, startTime=None):
    """таймер"""
    if startTime:
        print(f"Таймер: Прошло времени для [{name}]: {datetime.now() - startTime}")
    else:
        startTime = datetime.now()
        print(f"Таймер: Запущен [{name}] at {startTime}")
        return startTime


async def write_excel_async(df: pd.DataFrame, file_path):
    """
    Asynchronously writes a pandas DataFrame to an Excel file.
    """
    # Get the current event loop
    loop = asyncio.get_running_loop()
    # Use a ThreadPoolExecutor to run the blocking to_excel operation
    partial_func = functools.partial(
        df.to_excel, file_path, sheet_name="sheet1", index=False
    )

    with ThreadPoolExecutor() as executor:
        # result = await loop.run_in_executor(None, partial_func)
        await loop.run_in_executor(executor, partial_func)

    print(f"Finished writing to {file_path} asynchronously.")


# Example Usage:
async def main():
    # Ensure you have the required engine (e.g., openpyxl) installed:
    # pip install openpyxl

    df = pd.read_parquet(r"C:\uv\pandas-ch\data\скл\все мтр на_27.02.2026.parquet")
    df2 = pd.read_parquet(r"C:\uv\pandas-ch\data\скл\все мтр на_27.02.2026.parquet")

    startTime = timer(name="Начало записи в выходной файл")

    await write_excel_async(df, "output_async.xlsx")
    await write_excel_async(df2, "output_async2.xlsx")

    # res1, res2 = await asyncio.gather(
    #     write_excel_async(df, "output_async.xlsx"),
    #     write_excel_async(df2, "output_async2.xlsx"),
    # )

    timer("Завершена запись в выходной файл", startTime)


if __name__ == "__main__":
    asyncio.run(main())
