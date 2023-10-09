from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

def test_pyspark_setup():
    # Initialize a Spark session
    spark = SparkSession.builder \
        .appName("PySpark Test") \
        .getOrCreate()

    # Create a simple DataFrame
    schema = StructType([
        StructField("name", StringType(), True),
        StructField("city", StringType(), True)
    ])
    data = [("Alice", "New York"), ("Bob", "Los Angeles")]
    df = spark.createDataFrame(data, schema=schema)

    # Show the DataFrame
    print("Original DataFrame:")
    df.show()

    # Export the DataFrame to a Parquet file
    parquet_path = "test_data.parquet"
    df.write.mode('overwrite').parquet(parquet_path)
    print(f"DataFrame written to {parquet_path}")

    # Read the Parquet file into a new DataFrame
    df_from_parquet = spark.read.parquet(parquet_path)

    # Show the DataFrame loaded from the Parquet file
    print("DataFrame loaded from Parquet:")
    df_from_parquet.show()

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    test_pyspark_setup()
