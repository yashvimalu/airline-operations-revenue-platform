
import os
import sys

# Explicitly tell Spark which Python interpreter to use.
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession


def main():
    python_executable = sys.executable
    print("Python executable:", python_executable)

    spark = (
        SparkSession.builder
        .appName("AirlineDataPlatform")
        .master("local[2]")
        .config("spark.driver.memory", "4g")
        .config("spark.pyspark.python", python_executable)
        .config("spark.pyspark.driver.python", python_executable)
        .getOrCreate()
    )

    print("Spark Version:", spark.version)

    data = [
        ("AI101", "DEL", "BOM", 120),
        ("AI102", "BOM", "BLR", 45),
        ("AI103", "DEL", "HYD", 0),
        ("AI104", "BLR", "DEL", 90),
    ]

    columns = [
        "flight_number",
        "origin",
        "destination",
        "departure_delay",
    ]

    df = spark.createDataFrame(data, columns)

    print("Total flights:", df.count())
    df.show()

    spark.stop()


if __name__ == "__main__":
    main()
