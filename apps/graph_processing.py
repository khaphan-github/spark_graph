from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import time

spark = SparkSession.builder \
    .appName("SimpleGraphProcessing") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.executor.memory", "1g") \
    .config("spark.executor.cores", "2") \
    .getOrCreate()
def subgraph2vec(spark_session):
    schema = StructType([
        StructField("source", IntegerType(), True),
        StructField("target", IntegerType(), True),
        StructField("edge_type", StringType(), True)
    ])

    df = spark_session.read.option("delimiter", "|").option("header", "false") \
        .schema(schema).csv("/opt/spark/data/large_graph.txt")
    # df.cache()
    
    # TODO:
    '''
    '''
    def process_partition(iterator):
        result = []
        for row in iterator:
            # bạn có thể thực hiện tính toán ở đây
            time.sleep(0.0001)  # Giả lập tính toán nặng
            result.append(row)  
        return iter(result)

    processed_rdd = df.rdd.mapPartitions(process_partition)
    for row in processed_rdd.collect():
        print(row)
    return df


# Run processing
if __name__ == "__main__":
    graph_df = subgraph2vec(spark)
    print("\n✅ Processing completed!")
    spark.stop()