import pandas as pd

if __name__ == "__main__":
    res = pd.read_parquet("c1_ost.parquet")
    df_right = pd.read_excel("дпм/02_28.02.2026.xlsx", engine="calamine")

    # сформировать ключ для слияния
    # res["key"] = res["Код склада SAP"] + res["КСМ"].astype("string") + res["Партия SAP"]

    df_right["key"] = (
        df_right["КодСклада"]
        + df_right["Код КСМ"].astype("string")
        + df_right["Партия"]
    )

    pd.set_option("display.max_rows", None)
    pd.set_option("display.max_columns", None)
    # pd.reset_option("display.max_rows")
    # pd.reset_option("display.max_columns")

    print(res.dtypes)
    print("===================")
    print(df_right.dtypes)
    df_right = (
        # df_right[["key", "ДатПервПст"]]  # pyright: ignore[reportCallIssue]
        df_right.sort_values(by="ДатПервПст", ascending=False).drop_duplicates(
            subset="key", keep="first"
        )
    )

    df_right.to_excel("output_02_28.02.2026.xlsx", index=False)

    res = pd.merge(
        left=res,
        right=df_right,
        how="left",
        left_on="key",
        right_on="key",
    )
    res.to_excel("res.xlsx", index=False)

    print("ok")
