# import clickhouse_connect
import pandas as pd
from sqlalchemy import create_engine, text


def main():
    print("\n\nHello from pp!")


def cl_ins(client) -> int:

    # create_table_query = """
    # CREATE TABLE IF NOT EXISTS users (
    # id UInt32,
    # name String,
    # age UInt8
    # ) ENGINE MergeTree
    # ORDER BY id
    # """

    # client.command(create_table_query)

    df = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Joe"],
            "age": [25, 30, 28],
        }
    )

    client.insert_df("users2", df)
    return 0


def cl_st(client):
    result = client.query("SHOW TABLES")

    # The result is a QueryResult object
    # You can access the table names in different ways:

    # Method 1: Print all rows in the result set
    print("Tables in the current database:")
    for row in result.result_rows:
        print(row[0])

    # Method 2: Get column names (useful for clarity)
    print(f"\nColumn names: {result.column_names}")

    # Method 3: Convert the results to a list of named results (e.g., list of dictionaries)
    # named_results() returns a list of dictionaries, where keys are column names
    tables_list = result.named_results()
    print("\nTables using named_results():")
    for table in tables_list:
        print(table["name"])  # assuming the column name is 'name'

    result = client.query("SHOW DATABASES")
    print("\ndatabases:")
    for row in result.result_rows:
        print(row[0])


def cl_sqla():

    create_table_query = """
    CREATE TABLE IF NOT EXISTS users (
    id UInt32,
    name String,
    age UInt8
    ) ENGINE MergeTree
    ORDER BY id
    """

    # "clickhousedb://user:password@host:8123/mydb?compression=zstd"
    engine = create_engine("clickhousedb://default:asd123@192.168.5.17:8123/default")

    with engine.connect() as connection:
        connection.execute(text(create_table_query))
        connection.commit()

    df = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Joe"],
            "age": [25, 30, 28],
        }
    )
    # df.to_sql("autot", con=engine, index=False, if_exists="replace")
    df.to_sql("autot", con=engine, index=False, if_exists="append")

    with engine.begin() as conn:
        rows = conn.execute(text("SELECT version()"))
        print(rows.scalar())


if __name__ == "__main__":
    main()

    # client = clickhouse_connect.get_client(host="192.168.5.44")

    # cl_ins()
    # cl_st(client)

    # query = "SELECT * FROM users2"
    # df2 = cl.query_df(query)
    # print(df2)

    cl_sqla()
