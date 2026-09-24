"""
Practical 7: Performance Optimization Using Caching and Partitioning
AIM: Performance Optimization Using Caching and Partitioning
Problem Definition:
Scenario: You are a Big Data Engineer at MovieFlix Technologies, a movie recommendation
company serving millions of users worldwide.
The recommendation engine repeatedly processes movie ratings data to generate personalized
suggestions. Due to the increasing volume of user interactions, query execution time has
become a major concern.
Management wants to optimize the Spark application using Caching and Partitioning
techniques to reduce execution time and improve scalability.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count
import time

def create_spark_session():
    """Create and return Spark Session"""
    spark = SparkSession.builder \
        .appName("Performance Optimization - Caching and Partitioning") \
        .master("local[*]") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

def load_ratings_dataset(spark, file_path):
    """Load ratings dataset into Spark DataFrame"""
    print("=" * 60)
    print("TASK 2: Load ratings dataset into Spark DataFrame")
    print("=" * 60)
    
    df = spark.read.option("header", "true") \
        .option("inferSchema", "true") \
        .csv(file_path)
    
    print(f"Data loaded successfully. Total records: {df.count()}")
    return df

def display_dataset_schema(df):
    """Display dataset schema"""
    print("\n" + "=" * 60)
    print("TASK 3: Display dataset schema")
    print("=" * 60)
    df.printSchema()

def check_current_partitions(df):
    """Check the current number of partitions"""
    print("\n" + "=" * 60)
    print("TASK 4: Check the current number of partitions")
    print("=" * 60)
    
    num_partitions = df.rdd.getNumPartitions()
    print(f"Current number of partitions: {num_partitions}")
    print(f"Default parallelism: {df.sparkSession.sparkContext.defaultParallelism}")
    
    return num_partitions

def calculate_average_ratings(df):
    """Calculate average movie ratings"""
    print("\n" + "=" * 60)
    print("TASK 5: Calculate average movie ratings")
    print("=" * 60)
    
    avg_rating = df.agg(avg("Revenue").alias("Average_Revenue"))
    result = avg_rating.collect()[0][0]
    print(f"Average Revenue: {result:.2f}")
    
    return result

def measure_execution_time(func, *args, **kwargs):
    """Measure execution time of a function"""
    start_time = time.time()
    result = func(*args, **kwargs)
    end_time = time.time()
    execution_time = end_time - start_time
    return result, execution_time

def task_6_measure_before_optimization(df):
    """Measure execution time before optimization"""
    print("\n" + "=" * 60)
    print("TASK 6: Measure execution time before optimization")
    print("=" * 60)
    
    def compute_avg():
        return df.agg(avg("Revenue")).collect()[0][0]
    
    result, exec_time = measure_execution_time(compute_avg)
    print(f"Average Revenue: {result:.2f}")
    print(f"Execution time (before optimization): {exec_time:.4f} seconds")
    
    return exec_time

def task_7_apply_cache(df):
    """Apply cache() on the DataFrame"""
    print("\n" + "=" * 60)
    print("TASK 7: Apply cache() on the DataFrame")
    print("=" * 60)
    
    cached_df = df.cache()
    print("Cache applied successfully")
    print(f"Storage level: {cached_df.storageLevel}")
    
    return cached_df

def task_8_trigger_caching(cached_df):
    """Trigger caching using count()"""
    print("\n" + "=" * 60)
    print("TASK 8: Trigger caching using count()")
    print("=" * 60)
    
    start_time = time.time()
    count_result = cached_df.count()
    end_time = time.time()
    
    print(f"Count triggered: {count_result:,} records")
    print(f"Caching trigger time: {end_time - start_time:.4f} seconds")
    print(f"DataFrame is cached: {cached_df.is_cached}")

def task_9_recalculate_average_ratings(cached_df):
    """Recalculate average ratings after caching"""
    print("\n" + "=" * 60)
    print("TASK 9: Recalculate average ratings (after caching)")
    print("=" * 60)
    
    def compute_avg():
        return cached_df.agg(avg("Revenue")).collect()[0][0]
    
    result, exec_time = measure_execution_time(compute_avg)
    print(f"Average Revenue: {result:.2f}")
    print(f"Execution time (after caching): {exec_time:.4f} seconds")
    
    return exec_time

def task_10_compare_caching_performance(time_before, time_after):
    """Compare performance before and after caching"""
    print("\n" + "=" * 60)
    print("TASK 10: Compare performance before and after caching")
    print("=" * 60)
    
    speedup = time_before / time_after if time_after > 0 else float('inf')
    improvement = ((time_before - time_after) / time_before) * 100 if time_before > 0 else 0
    
    print(f"Time before caching: {time_before:.4f} seconds")
    print(f"Time after caching:  {time_after:.4f} seconds")
    print(f"Speedup factor: {speedup:.2f}x")
    print(f"Performance improvement: {improvement:.2f}%")

def task_11_repartition_dataset(cached_df, num_partitions=4):
    """Repartition the dataset"""
    print("\n" + "=" * 60)
    print("TASK 11: Repartition the dataset")
    print("=" * 60)
    
    print(f"Repartitioning to {num_partitions} partitions...")
    repartitioned_df = cached_df.repartition(num_partitions)
    
    print("Repartitioning completed")
    return repartitioned_df

def task_12_display_updated_partition_count(repartitioned_df):
    """Display updated partition count"""
    print("\n" + "=" * 60)
    print("TASK 12: Display updated partition count")
    print("=" * 60)
    
    new_partitions = repartitioned_df.rdd.getNumPartitions()
    print(f"Updated number of partitions: {new_partitions}")
    
    return new_partitions

def task_13_compare_partitioning_performance(original_df, repartitioned_df):
    """Compare performance with different partitioning"""
    print("\n" + "=" * 60)
    print("TASK 13: Compare performance before and after repartitioning")
    print("=" * 60)
    
    def compute_on_original():
        return original_df.agg(avg("Revenue")).collect()[0][0]
    
    def compute_on_repartitioned():
        return repartitioned_df.agg(avg("Revenue")).collect()[0][0]
    
    result_orig, time_orig = measure_execution_time(compute_on_original)
    result_repart, time_repart = measure_execution_time(compute_on_repartitioned)
    
    print(f"Original partitions - Time: {time_orig:.4f}s, Result: {result_orig:.2f}")
    print(f"Repartitioned       - Time: {time_repart:.4f}s, Result: {result_repart:.2f}")
    
    if time_repart > 0:
        speedup = time_orig / time_repart
        print(f"Repartitioning speedup: {speedup:.2f}x")
    
    return time_orig, time_repart

def task_14_generate_performance_report(original_partitions, new_partitions, 
                                         time_before_cache, time_after_cache,
                                         time_orig_part, time_repart_part):
    """Generate performance report"""
    print("\n" + "=" * 60)
    print("TASK 14: Generate performance report")
    print("=" * 60)
    
    print(f"""
╔══════════════════════════════════════════════════════════════╗
║           PERFORMANCE OPTIMIZATION REPORT                    ║
╠══════════════════════════════════════════════════════════════╣
║  DATASET INFORMATION                                         ║
║  ────────────────────                                         ║
║  Original Partitions: {original_partitions:<5}                                     ║
║  Repartitioned to:    {new_partitions:<5}                                     ║
╠══════════════════════════════════════════════════════════════╣
║  CACHING PERFORMANCE                                         ║
║  ────────────────────                                         ║
║  Before Caching:    {time_before_cache:.4f} seconds                          ║
║  After Caching:     {time_after_cache:.4f} seconds                          ║
║  Speedup:           {(time_before_cache/time_after_cache if time_after_cache > 0 else 0):.2f}x                                              ║
║  Improvement:       {((time_before_cache-time_after_cache)/time_before_cache*100 if time_before_cache > 0 else 0):.2f}%                                             ║
╠══════════════════════════════════════════════════════════════╣
║  REPARTITIONING PERFORMANCE                                  ║
║  ─────────────────────────                                   ║
║  Original Partitions: {time_orig_part:.4f} seconds                          ║
║  Repartitioned:       {time_repart_part:.4f} seconds                          ║
║  Speedup:             {(time_orig_part/time_repart_part if time_repart_part > 0 else 0):.2f}x                                              ║
╠══════════════════════════════════════════════════════════════╣
║  RECOMMENDATIONS                                             ║
║  ────────────────                                             ║
║  1. Use cache() for DataFrames accessed multiple times       ║
║  2. Repartition based on cluster cores for optimal parallelism║
║  3. Monitor storage memory to avoid OOM errors               ║
║  4. Use coalesce() instead of repartition() for reducing     ║
║     partitions (avoids full shuffle)                         ║
║  5. Consider persist() with MEMORY_AND_DISK for large data   ║
╚══════════════════════════════════════════════════════════════╝
""")

def main():
    file_path = "/home/heet18/Coding-Workspace/Futuristic/Heet/Github/Projects/Python-DataScience/apache_spark/dataset/sales_data.csv"
    
    spark = create_spark_session()
    
    try:
        print("=" * 60)
        print("TASK 1: Create Spark Session")
        print("=" * 60)
        print("Spark Session created successfully")
        print(f"Spark Version: {spark.version}")
        
        df = load_ratings_dataset(spark, file_path)
        display_dataset_schema(df)
        original_partitions = check_current_partitions(df)
        
        calculate_average_ratings(df)
        
        time_before_cache = task_6_measure_before_optimization(df)
        
        cached_df = task_7_apply_cache(df)
        
        task_8_trigger_caching(cached_df)
        
        time_after_cache = task_9_recalculate_average_ratings(cached_df)
        
        task_10_compare_caching_performance(time_before_cache, time_after_cache)
        
        repartitioned_df = task_11_repartition_dataset(cached_df, num_partitions=4)
        
        new_partitions = task_12_display_updated_partition_count(repartitioned_df)
        
        time_orig_part, time_repart_part = task_13_compare_partitioning_performance(
            cached_df, repartitioned_df)
        
        task_14_generate_performance_report(
            original_partitions, new_partitions,
            time_before_cache, time_after_cache,
            time_orig_part, time_repart_part)
        
        cached_df.unpersist()
        print("\n✅ Practical 7 completed successfully!")
        
    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()