import pandas as pd
from datetime import datetime
from joblib import Parallel, delayed


def timer(name, startTime=None):
    """таймер"""
    if startTime:
        print(f"Таймер: Прошло времени для [{name}]: {datetime.now() - startTime}")
    else:
        startTime = datetime.now()
        print(f"Таймер: Запущен [{name}] at {startTime}")
        return startTime


if __name__ == "__main__":
    Main_startTime = timer(name="============Запуск скрипта===============")
    df = pd.read_parquet(r"out\все мтр на_02.02.2026.parquet")

    # Запускаем задачи параллельно
    print("Начало сохранения файлов...")
    # df.to_excel(r"out\file1.xlsx")
    # df.to_excel(r"out\file2.xlsx")

    tasks = [
        delayed(df.to_excel)(excel_writer=r"out\file1.xlsx", engine="xlsxwriter"),
        delayed(df.to_excel)(excel_writer=r"out\file2.xlsx", engine="xlsxwriter"),
    ]
    results = Parallel(n_jobs=-1)(tasks)

    print("Все операции завершены.")
    timer("Итого времени выполнения скрипта", Main_startTime)
