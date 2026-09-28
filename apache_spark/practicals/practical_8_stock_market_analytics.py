"""
Practical 8: Stock Market Analytics Using Window-Based Analysis
AIM: Stock Market Analytics Using Window-Based Analysis
Problem Definition:
Scenario: You are a Data Analytics Engineer at FinEdge Securities Ltd., a financial services
company that provides stock market insights to investors.
The company continuously receives stock price updates from multiple exchanges. Investors
want to understand stock performance trends and identify the best investment opportunities.
Traditional data processing systems struggle to handle the continuously growing volume of
financial data. Therefore, the organization has adopted Apache Spark to perform large-scale
stock analytics.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max as spark_max, min as spark_min, sum as spark_sum, count, desc, row_number, lag, lead
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, DateType
import time

def create_spark_session():
    """Create and return Spark Session"""
    spark = SparkSession.builder \
        .appName("Stock Market Analytics - Window-Based Analysis") \
        .master("local[*]") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

def load_stock_dataset(spark, file_path):
    """Load stock dataset into Spark DataFrame"""
    print("=" * 60)
    print("TASK 2: Load stock dataset into Spark DataFrame")
    print("=" * 60)
    
    df = spark.read.option("header", "true") \
        .option("inferSchema", "true") \
        .csv(file_path)
    
    print(f"Data loaded successfully. Total records: {df.count()}")
    return df

def display_schema_and_records(df):
    """Display schema and stock records"""
    print("\n" + "=" * 60)
    print("TASK 3: Display schema and stock records")
    print("=" * 60)
    
    print("\n--- Schema ---")
    df.printSchema()
    
    print("\n--- Sample Records (First 20) ---")
    df.show(20, truncate=False)
    
    print("\n--- Distinct Stock Symbols ---")
    df.select("Symbol").distinct().show(20, truncate=False)
    
    return df

def group_records_by_symbol(df):
    """Group records based on stock symbol"""
    print("\n" + "=" * 60)
    print("TASK 4: Group records based on stock symbol")
    print("=" * 60)
    
    symbol_counts = df.groupBy("Symbol").agg(count("*").alias("Record_Count")) \
        .orderBy(desc("Record_Count"))
    
    print("Records per Symbol (Top 20):")
    symbol_counts.show(20, truncate=False)
    
    return symbol_counts

def calculate_average_stock_price(df):
    """Calculate average stock price per symbol"""
    print("\n" + "=" * 60)
    print("TASK 5: Calculate average stock price")
    print("=" * 60)
    
    avg_price = df.groupBy("Symbol") \
        .agg(avg("Close").alias("Average_Close_Price")) \
        .orderBy(desc("Average_Close_Price"))
    
    print("Average Stock Price per Symbol (Top 20):")
    avg_price.show(20, truncate=False)
    
    return avg_price

def calculate_maximum_stock_price(df):
    """Calculate maximum stock price per symbol"""
    print("\n" + "=" * 60)
    print("TASK 6: Calculate maximum stock price")
    print("=" * 60)
    
    max_price = df.groupBy("Symbol") \
        .agg(spark_max("High").alias("Max_Price")) \
        .orderBy(desc("Max_Price"))
    
    print("Maximum Stock Price per Symbol (Top 20):")
    max_price.show(20, truncate=False)
    
    return max_price

def calculate_minimum_stock_price(df):
    """Calculate minimum stock price per symbol"""
    print("\n" + "=" * 60)
    print("TASK 7: Calculate minimum stock price")
    print("=" * 60)
    
    min_price = df.groupBy("Symbol") \
        .agg(spark_min("Low").alias("Min_Price")) \
        .orderBy("Min_Price")
    
    print("Minimum Stock Price per Symbol (Top 20):")
    min_price.show(20, truncate=False)
    
    return min_price

def generate_stock_summary_report(df):
    """Generate stock summary report"""
    print("\n" + "=" * 60)
    print("TASK 8: Generate stock summary report")
    print("=" * 60)
    
    summary = df.groupBy("Symbol") \
        .agg(
            avg("Close").alias("Avg_Close"),
            spark_max("High").alias("Max_High"),
            spark_min("Low").alias("Min_Low"),
            avg("Volume").alias("Avg_Volume"),
            count("*").alias("Trading_Days")
        ) \
        .orderBy(desc("Avg_Close"))
    
    print("Comprehensive Stock Summary Report (Top 20):")
    summary.show(20, truncate=False)
    
    return summary

def identify_best_performing_stocks(df):
    """Identify best-performing stocks using window functions"""
    print("\n" + "=" * 60)
    print("TASK 9: Identify best-performing stocks (Window Analysis)")
    print("=" * 60)
    
    window_spec = Window.partitionBy("Symbol").orderBy("Date")
    
    df_with_returns = df.withColumn("Prev_Close", lag("Close", 1).over(window_spec)) \
        .withColumn("Daily_Return", 
            (col("Close") - col("Prev_Close")) / col("Prev_Close") * 100)
    
    performance = df_with_returns.filter(col("Daily_Return").isNotNull()) \
        .groupBy("Symbol") \
        .agg(
            avg("Daily_Return").alias("Avg_Daily_Return_%"),
            spark_max("Daily_Return").alias("Max_Daily_Gain_%"),
            spark_min("Daily_Return").alias("Max_Daily_Loss_%"),
            spark_sum("Volume").alias("Total_Volume"),
            count("*").alias("Trading_Days")
        ) \
        .orderBy(desc("Avg_Daily_Return_%"))
    
    print("Best Performing Stocks by Average Daily Return (Top 20):")
    performance.show(20, truncate=False)
    
    return performance

def window_based_analysis(df):
    """Perform window-based analysis for trends"""
    print("\n" + "=" * 60)
    print("TASK 10: Window-Based Analysis - Moving Averages & Trends")
    print("=" * 60)
    
    window_5 = Window.partitionBy("Symbol").orderBy("Date").rowsBetween(-4, 0)
    window_20 = Window.partitionBy("Symbol").orderBy("Date").rowsBetween(-19, 0)
    
    df_with_ma = df.withColumn("MA_5", avg("Close").over(window_5)) \
        .withColumn("MA_20", avg("Close").over(window_20)) \
        .withColumn("Price_vs_MA5", (col("Close") - col("MA_5")) / col("MA_5") * 100) \
        .withColumn("Price_vs_MA20", (col("Close") - col("MA_20")) / col("MA_20") * 100)
    
    latest = df_with_ma.filter(col("MA_20").isNotNull()) \
        .withColumn("rn", row_number().over(Window.partitionBy("Symbol").orderBy(desc("Date")))) \
        .filter(col("rn") == 1) \
        .select("Symbol", "Date", "Close", "MA_5", "MA_20", "Price_vs_MA5", "Price_vs_MA20") \
        .orderBy(desc("Price_vs_MA20"))
    
    print("Latest Moving Average Analysis (Top 20):")
    latest.show(20, truncate=False)
    
    return latest

def interpret_market_trends(performance, latest_ma):
    """Interpret market trends"""
    print("\n" + "=" * 60)
    print("TASK 10 (cont): Interpret market trends")
    print("=" * 60)
    
    top_performer = performance.first()
    worst_performer = performance.orderBy("Avg_Daily_Return_%").first()
    
    bullish = latest_ma.filter(col("Price_vs_MA20") > 0).count()
    bearish = latest_ma.filter(col("Price_vs_MA20") < 0).count()
    
    print(f"\n--- MARKET TREND INTERPRETATION ---")
    print(f"\n1. TOP PERFORMER:")
    print(f"   Symbol: {top_performer['Symbol']}")
    print(f"   Avg Daily Return: {top_performer['Avg_Daily_Return_%']:.4f}%")
    print(f"   Max Daily Gain: {top_performer['Max_Daily_Gain_%']:.4f}%")
    print(f"   Max Daily Loss: {top_performer['Max_Daily_Loss_%']:.4f}%")
    
    print(f"\n2. WORST PERFORMER:")
    print(f"   Symbol: {worst_performer['Symbol']}")
    print(f"   Avg Daily Return: {worst_performer['Avg_Daily_Return_%']:.4f}%")
    
    print(f"\n3. MARKET SENTIMENT (based on MA20):")
    print(f"   Bullish (Price > MA20): {bullish} stocks")
    print(f"   Bearish (Price < MA20): {bearish} stocks")
    
    return bullish, bearish

def generate_investment_recommendations(performance, latest_ma):
    """Generate investment recommendations"""
    print("\n" + "=" * 60)
    print("TASK 11: Generate investment recommendations")
    print("=" * 60)
    
    strong_buy = performance.filter(col("Avg_Daily_Return_%") > 0.1) \
        .join(latest_ma.filter(col("Price_vs_MA20") > 1), "Symbol") \
        .select("Symbol", "Avg_Daily_Return_%", "Price_vs_MA20") \
        .orderBy(desc("Avg_Daily_Return_%"))
    
    print("\n--- STRONG BUY CANDIDATES (Positive returns + Price > MA20) ---")
    strong_buy.show(10, truncate=False)
    
    print(f"""
╔══════════════════════════════════════════════════════════════╗
║           INVESTMENT RECOMMENDATIONS                         ║
╠══════════════════════════════════════════════════════════════╣
║  1. STRONG BUY: Stocks with consistent positive returns     ║
║     and trading above 20-day moving average                 ║
║                                                              ║
║  2. BUY: Stocks with positive average returns               ║
║                                                              ║
║  3. HOLD: Stocks near moving averages with low volatility   ║
║                                                              ║
║  4. SELL: Stocks with negative returns below MA20           ║
║                                                              ║
║  5. RISK MANAGEMENT:                                        ║
║     - Diversify across sectors                              ║
║     - Set stop-loss at 5-10% below entry                    ║
║     - Monitor volume for confirmation                       ║
║     - Rebalance portfolio monthly                           ║
╚══════════════════════════════════════════════════════════════╝
""")

def main():
    file_path = "/home/heet18/Coding-Workspace/Futuristic/Heet/Github/Projects/Python-DataScience/apache_spark/dataset/stock market.csv"
    
    spark = create_spark_session()
    
    try:
        print("=" * 60)
        print("TASK 1: Create Spark Session")
        print("=" * 60)
        print("Spark Session created successfully")
        print(f"Spark Version: {spark.version}")
        
        df = load_stock_dataset(spark, file_path)
        df = display_schema_and_records(df)
        
        group_records_by_symbol(df)
        
        avg_price = calculate_average_stock_price(df)
        
        max_price = calculate_maximum_stock_price(df)
        
        min_price = calculate_minimum_stock_price(df)
        
        summary = generate_stock_summary_report(df)
        
        performance = identify_best_performing_stocks(df)
        
        latest_ma = window_based_analysis(df)
        
        interpret_market_trends(performance, latest_ma)
        
        generate_investment_recommendations(performance, latest_ma)
        
        print("\n✅ Practical 8 completed successfully!")
        
    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()