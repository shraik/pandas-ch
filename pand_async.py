import pandas as pd
from datetime import datetime
from joblib import Parallel, delayed
from concurrent.futures import ProcessPoolExecutor
from functools import partial


def timer(name, startTime=None):
    """таймер"""
    if startTime:
        print(f"Таймер: Прошло времени для [{name}]: {datetime.now() - startTime}")
    else:
        startTime = datetime.now()
        print(f"Таймер: Запущен [{name}] at {startTime}")
        return startTime


def parallel_writer1(filelist: list[str], df: pd.DataFrame):

    # Запускаем задачи параллельно
    startTime = timer("Начало сохранения файлов 1...")

    tasks = [
        delayed(df.to_excel)(excel_writer=filelist[0], engine="xlsxwriter"),
        delayed(df.to_excel)(excel_writer=filelist[1], engine="xlsxwriter"),
    ]
    results = Parallel(n_jobs=-1)(tasks)

    print("Все операции завершены.")
    timer("Итого времени выполнения скрипта 1", startTime)
    return results


def parallel_writer2(filelist: list[str], df: pd.DataFrame):
    startTime = timer("Начало сохранения файлов 2...")

    save_func = partial(df.to_excel, engine="xlsxwriter")

    # with ThreadPoolExecutor(max_workers=3) as executor:
    with ProcessPoolExecutor(max_workers=3) as executor:
        results = list(executor.map(save_func, filelist))

    timer("Итого времени выполнения скрипта 2", startTime)
    return results


if __name__ == "__main__":
    Main_startTime = timer(name="============Запуск скрипта===============")
    df = pd.read_parquet(r"out\все мтр на_27.02.2026.parquet")

    filetosave = [r"out\file1.xlsx", r"out\file2.xlsx"]

    _ = parallel_writer1(filetosave, df)

    _ = parallel_writer2(filetosave, df)
    # print(res)
