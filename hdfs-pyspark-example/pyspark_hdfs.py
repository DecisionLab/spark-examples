from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .getOrCreate()

    # HDFS example from:
    # https://github.com/saagie/exemple-pyspark-read-and-write/blob/master/hdfs/__main__.py

    # Create dataframe with sample data
    data = [('First', 1), ('Second', 2), ('Third', 3), ('Fourth', 4), ('Fifth', 5)]
    df = spark.createDataFrame(data)


    # Write into HDFS
    df.write.csv("hdfs://cluster/user/hdfs/test/example.csv")

    # Read from HDFS
    df_load = spark.read.csv('hdfs://cluster/user/hdfs/test/example.csv')
    df_load.show()
