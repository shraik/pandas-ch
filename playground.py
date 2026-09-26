from clickhouse_connect import driver as ch_driver
from clickhouse_connect.driver.exceptions import DatabaseError

from shared_chouse import timer
import clickhouse_connect
import sys


def contc(
    dbname="default",
    hostip="192.168.203.128",
    port=8123,
    username="default",
    password="",
) -> ch_driver.Client:
    try:
        client = clickhouse_connect.get_client(
            host=hostip,
            port=port,
            username=username,
            password=password,  # Your password, if any
            database=dbname,
        )
    except DatabaseError as e:
        print("Caught a ClickHouse DatabaseError:")
        print(f"Error Code: {e.args[0]}")
        if "Code: 81" in str(e):
            print(
                f"This is the specific 'Database {dbname} doesn\\'t exist' error (Code 81)."
            )
            client = clickhouse_connect.get_client(
                host=hostip,
                port=port,
                username="default",
                password="",  # Your password, if any
                database="default",
            )
            client.command(f"CREATE DATABASE {dbname}")
            client = clickhouse_connect.get_client(
                host=hostip,
                port=port,
                username="default",
                password="",  # Your password, if any
                database=dbname,
            )
        else:
            print("This is a different type of DatabaseError.")
            sys.exit(0)
    return client


if __name__ == "__main__":
    # 1. Connect to the public ClickHouse playground
    client = clickhouse_connect.get_client(
        host="play.clickhouse.com",
        port=443,
        secure=True,
        username="play",
        password="",
    )

    # 2. Run a sample query on the New York taxi dataset
    query_str = """
    SELECT 
        passenger_count, 
        ceil(avg(fare_amount)) AS avg_fare,
        count() AS total_rides
    FROM trips
    WHERE passenger_count > 0
    GROUP BY passenger_count
    ORDER BY passenger_count ASC
    """

    result = client.query(query_str)

    # 3. Print the data rows
    print("Passenger | Avg Fare ($) | Total Rides")
    print("-" * 40)
    for row in result.result_rows:
        print(f"{row[0]:<9} | {row[1]:<12} | {row[2]}")
