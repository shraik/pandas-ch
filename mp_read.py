# вариант параллельного считывания файлов excel с использованием multiprocessing
# import multiprocessing
from multiprocessing import Pool

# import time
import re
import os

from pathlib import Path
import sys
import pandas as pd

from shared_chouse import (
    timer,
)


# --- ОПРЕДЕЛЕНИЕ ДВУХ РАЗНЫХ ФУНКЦИЙ ---


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
        # print(
        #     f"\nНайден самый новый файл по шаблону '{pattern}' в каталоге '{directory}':\n{str(Path(latest_file).resolve())}"
        # )
        return str(latest_file)
    except FileNotFoundError:
        print(f"Ошибка: Каталог '{directory}' не найден.")
        return None
    except Exception as e:
        print(f"Произошла непредвиденная ошибка: {e}")
        return None


def find_latest2_file(directory: str, pattern: str) -> list[str] | None:
    """Находит два последних измененных файла в каталоге по заданному шаблону.
    Первый в списке должен быть файл с суффиксом "до". Если первый заканчивается на "до", поменяет их местами.

    Args:
        directory (str): _Каталог для поиска файла_
        pattern (str): _Шаблон для поиска файла_

    Returns:
        list[str] | None: _Найденные файлы по шаблону_
    """

    try:
        dir_path = Path(directory)
        files = list(dir_path.glob(pattern))
        if not files:
            print(
                f"Ошибка: Не найдено файлов по шаблону '{pattern}' в каталоге '{directory}'."
            )
            return None

        # сортируть файлы по времени изменения и взять два последних
        latest_files = sorted(files, key=lambda p: p.stat().st_mtime, reverse=True)[:2]
        # первый в списке должен быть файл с суффиксом "до". Если первый заканчивается на "до", поменять их местами.
        if len(latest_files) == 2 and latest_files[0].stem[-3:-1] == "до":
            latest_files[0], latest_files[1] = latest_files[1], latest_files[0]

        # print(
        #     f"\nНайдены два последних файла по шаблону '{pattern}' в каталоге '{directory}':\n{[str(Path(f).resolve()) for f in latest_files]}"
        # )
        return [str(f) for f in latest_files]
    except FileNotFoundError:
        print(f"Ошибка: Каталог '{directory}' не найден.")
        return None
    except Exception as e:
        print(f"Произошла непредвиденная ошибка: {e}")
        return None


def pdread_sap(filesap: str) -> tuple[pd.DataFrame, str]:
    """Для использования с joblib. Загрузка файла SAP"""

    # if filesap[-3:-1] == "до":
    if filesap.find("{от}") > -1:
        ftype = "начальный"
    elif filesap.find("{до}") > -1:
        ftype = "конечный"
    else:
        ftype = "неизвестный"

    print(f"--читаем {ftype} SAP файл: {filesap}")
    res = pd.read_excel(filesap, engine="calamine")
    print(f"--{ftype} SAP файл прочитан: {filesap}")

    return res, filesap


def load_mol_excel(
    clumns: dict, header_row: list, filename: str, only_selected=True, drop_un=False
) -> pd.DataFrame:
    """считывает xlsx файл с поиском колонок в 2х этажном заголовке таблицы
    возвращает фрейм с найденными колонками
    """

    res = pd.read_excel(filename, dtype=str, header=header_row, engine="calamine")
    res2 = pd.read_excel(filename, nrows=header_row[0], engine="calamine")

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

    # print(
    #     f"--ОСВ список прочитанных колонок в файле {str(Path(filename).resolve())}: {lisc}, \nколичество колонок: {len(lisc)}"
    # )
    print(
        f"--ОСВ прочитанных колонок в файле {str(Path(filename).resolve())}: {len(lisc)}"
    )
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
            print(f'\n--ОСВ Ошибка. Не нашел колонку "{li}" в списке колонок')
            sys.exit()

    if only_selected:
        res = res[resl]

    res.rename(
        columns=renmd,
        inplace=True,
    )
    if drop_un:
        pattern = r"_[A-z:\d{1,2} ]+"
        print(rf"--ОСВ удаление из имени колонок по шаблону '{pattern}'")
        res = res.rename(columns=lambda x: re.sub(pattern, "", x))
        # сбросить дубликаты колонок по именам, сохранив первую
        res = res.loc[:, ~res.columns.duplicated()]

    res["Версия"] = res4
    res["Версия2"] = pd.to_datetime(str(res4).strip()[-10:], format="%d.%m.%Y")

    return res


def pdread_c1(DATA_C1: str) -> tuple[pd.DataFrame, str]:
    """Для использования с joblib. Загрузка файла ОСВ"""
    print("--выбираем файл с остатками ОСВ---")
    mol_file = find_latest_file(DATA_C1, "*.xlsx")

    print(f"--читаем ОСВ файл: {mol_file}")

    if not mol_file:
        print("Не удалось найти необходимые файлы данных. Выход.")
        sys.exit(0)

    res = (
        load_mol_excel(
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
        ),
        mol_file,
    )
    print(f"--ОСВ файл прочитан: {mol_file}")
    return res


def readparallel():

    # выгрузка sap. 2 последних файла с суффиксами {от} и {до}
    DATA_SAP = "SAP_in"
    # выгрузка 1С
    DATA_C1 = "C1_in"

    if (latest_sap_file := find_latest2_file("SAP_in", "*.xlsx")) is None or len(
        latest_sap_file
    ) != 2:
        print("Не удалось найти необходимые файлы данных. Выход.")
        sys.exit(0)
    print(f"Найденные файлы в папке {DATA_SAP}: {latest_sap_file}")

    num_workers = os.cpu_count()

    # 2. Очередь для асинхронных результатов
    async_results = []

    # 3. Отправляем задачи в пул по одной вручную
    with Pool(processes=num_workers) as pool:
        task = pool.apply_async(pdread_sap, args=(latest_sap_file[1],))
        async_results.append(task)
        task = pool.apply_async(pdread_c1, args=(DATA_C1,))
        async_results.append(task)
        task = pool.apply_async(pdread_sap, args=(latest_sap_file[0],))
        async_results.append(task)

        # Закрываем пул для новых задач и ждем выполнения запущенных
        pool.close()

        # Ожидаем завершения и собираем результаты
        # print("Ожидание завершения процессов...")
        results = [task.get() for task in async_results]

    return results


# --- ТОЧКА ВХОДА (ОБЯЗАТЕЛЬНА ДЛЯ MULTIPROCESSING) ---
if __name__ == "__main__":
    startTime = timer(name="Начало чтения входных файлов")
    res = readparallel()
    timer("Чтение завершено.", startTime)

    print("[Главный поток] Оба процесса успешно отработали. Программа завершена.")
