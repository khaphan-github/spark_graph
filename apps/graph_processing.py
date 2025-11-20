from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import time

# Create Spark session
spark = SparkSession.builder \
    .appName("SimpleGraphProcessing") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()

def process_graph():
    """Simple graph processing from large_graph.txt"""
    
    print("🚀 Processing Large Graph")
    print("=" * 40)
    
    # Define schema
    schema = StructType([
        StructField("source", IntegerType(), True),
        StructField("target", IntegerType(), True),
        StructField("edge_type", StringType(), True)
    ])
    
    # Read the large graph file
    print("📖 Reading: /opt/spark/data/large_graph.txt")
    start_time = time.time()
    
    df = spark.read \
        .option("delimiter", "|") \
        .option("header", "false") \
        .schema(schema) \
        .csv("/opt/spark/data/10m_graph.txt")
    
    # Cache for faster calculations
    df.cache()
    
    # Get total count
    total_edges = df.count()
    read_time = time.time() - start_time
    
    print(f"✅ Loaded {total_edges:,} edges in {read_time:.4f}s")
    print(f"📊 Read rate: {total_edges/read_time:,.0f} edges/second")
    
    # Show sample data
    print(f"\n📋 Sample Data:")
    df.show(5)
    
    # Calculate for each edge type
    print(f"\n🔢 Edge Calculations:")
    
    start_time = time.time()
    edge_counts = df.groupBy("edge_type").count().collect()
    calc_time = time.time() - start_time
    
    print(f"Edge counts (calculated in {calc_time:.6f}s):")
    for row in sorted(edge_counts, key=lambda x: x['count'], reverse=True):
        print(f"   {row['edge_type']}: {row['count']:,} edges")
    
    # Show the edge counts DataFrame for verification
    print(f"\n📊 Edge Counts DataFrame:")
    edge_counts_df = df.groupBy("edge_type").count().orderBy("count", ascending=False)
    edge_counts_df.show()
    
    # Quick stats
    print(f"\n📈 Quick Stats:")
    
    # Unique nodes
    start_time = time.time()
    unique_nodes = df.select("source").union(df.select("target")).distinct().count()
    stat_time = time.time() - start_time
    print(f"   Unique nodes: {unique_nodes:,} (calculated in {stat_time:.6f}s)")
    
    # Show sample of unique nodes
    print(f"\n🔗 Sample Unique Nodes:")
    unique_nodes_df = df.select("source").union(df.select("target")).distinct().orderBy("source")
    unique_nodes_df.show(10)
    
    # Loop through each edge and count
    print(f"\n🔄 Loop-based Edge Counting:")
    start_time = time.time()
    
    # Collect all rows to loop through them
    all_edges = df.collect()
    
    # Initialize counters
    edge_type_counts = {}
    total_loop_count = 0
    
    # Loop through each edge
    for edge in all_edges:
        total_loop_count += 1
        edge_type = edge['edge_type']
        
        # Count edge types
        if edge_type in edge_type_counts:
            edge_type_counts[edge_type] += 1
        else:
            edge_type_counts[edge_type] = 1
    
    loop_time = time.time() - start_time
    
    print(f"   Total edges counted in loop: {total_loop_count:,}")
    print(f"   Loop processing time: {loop_time:.4f}s")
    print(f"   Loop rate: {total_loop_count/loop_time:,.0f} edges/second")
    print(f"   Edge type counts from loop:")
    
    # Sort and display loop results
    for edge_type, count in sorted(edge_type_counts.items(), key=lambda x: x[1], reverse=True):
        print(f"      {edge_type}: {count:,} edges")
    
    # Fastest operations
    print(f"\n⚡ Fast Operations:")
    
    operations = [
        ("Count edges", lambda: df.count()),
        ("Take first", lambda: df.take(1)),
        ("Is empty", lambda: df.rdd.isEmpty())
    ]
    
    fastest_time = float('inf')
    fastest_op = ""
    
    for op_name, operation in operations:
        start_time = time.time()
        result = operation()
        op_time = time.time() - start_time
        print(f"   {op_name}: {op_time:.6f}s - Result: {result}")
        
        if op_time < fastest_time:
            fastest_time = op_time
            fastest_op = op_name
    
    print(f"\n🏆 Fastest: {fastest_op} in {fastest_time:.6f}s")
    
    # Final summary output
    print(f"\n📋 Processing Summary:")
    print(f"   Total edges processed: {total_edges:,}")
    print(f"   Total edges from loop: {total_loop_count:,}")
    print(f"   Unique nodes found: {unique_nodes:,}")
    print(f"   Edge types: {len(edge_counts)}")
    print(f"   Data read time: {read_time:.4f}s")
    print(f"   DataFrame calculations time: {calc_time:.4f}s")
    print(f"   Loop processing time: {loop_time:.4f}s")
    print(f"   Stats time: {stat_time:.4f}s")
    print(f"   Total processing time: {time.time() - start_time + read_time + calc_time + stat_time + loop_time:.4f}s")
    
    # Verify counts match
    print(f"\n🔍 Verification:")
    print(f"   DataFrame count == Loop count: {total_edges == total_loop_count}")
    
    return df

# Run processing
if __name__ == "__main__":
    graph_df = process_graph()
    print("\n✅ Processing completed!")
    spark.stop()