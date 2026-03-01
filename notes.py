
def examples():
    """
    хранение примеров
    """
    # pd.StringDtype(storage="pyarrow")
    # string_pyarrow = pd.ArrowDtype(pa.string())
    # string_pyarrow = pd.StringDtype("pyarrow")
    # print(string_pyarrow)

    # df["Количество"] = df["Количество"].astype("Float64")

    # dft = df.select_dtypes("str")
    # df[dft.columns] = dft.apply(lambda x: x.fillna(""))

    # dft = df.select_dtypes("number")
    # df[dft.columns] = dft.apply(lambda x: x.fillna(0.0))

    # fill_value = pd.Timestamp("1970-01-01")
    # dft = df.select_dtypes("datetime")
    # df[dft.columns] = dft.apply(lambda x: x.fillna(fill_value))

    # print("--databases-----------------------")
    # result = client.query("SHOW DATABASES")
    # for row in result.result_set:
    #     print(row[0])
    # print("-------------------------")

    # print(f"--tables in database:{db_name}-----------------------")
    # result = client.query("SHOW TABLES")
    # for row in result.result_set:
    #     print(row[0])  # Print the table name from the first column of each row
    # print("-------------------------")

    return 0

