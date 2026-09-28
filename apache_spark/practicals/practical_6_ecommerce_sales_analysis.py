"""
Practical 6: E-Commerce Sales Analysis Using Spark DataFrames
AIM: E-Commerce Sales Analysis Using Spark DataFrames
Problem Definition:
Scenario: You are a Data Analyst at ShopSmart Analytics Pvt. Ltd. The company operates a
large-scale e-commerce platform that processes thousands of transactions every day.
Management wants to identify high-performing product categories, top-selling products, and
revenue contribution by city.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, desc, avg, count
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType
import time

def create_spark_session():
    """Create and return Spark Session"""
    spark = SparkSession.builder \
        .appName("E-Commerce Sales Analysis") \
        .master("local[*]") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

def load_sales_data(spark, file_path):
    """Load sales data into Spark DataFrame"""
    print("=" * 60)
    print("TASK 1: Load sales data into a Spark DataFrame")
    print("=" * 60)
    
    df = spark.read.option("header", "true") \
        .option("inferSchema", "true") \
        .csv(file_path)
    
    print(f"Data loaded successfully. Total records: {df.count()}")
    return df

def display_schema(df):
    """Display the schema of the dataset"""
    print("\n" + "=" * 60)
    print("TASK 2: Display the schema of the dataset")
    print("=" * 60)
    df.printSchema()

def validate_data_types(df):
    """Validate data types of all columns"""
    print("\n" + "=" * 60)
    print("TASK 3: Validate data types of all columns")
    print("=" * 60)
    
    print("\nColumn Name\t\tData Type\t\tNullable")
    print("-" * 50)
    for field in df.schema.fields:
        print(f"{field.name:<25}\t{field.dataType}\t\t{field.nullable}")
    
    print(f"\nTotal columns: {len(df.columns)}")
    print(f"Column names: {df.columns}")

def create_revenue_column(df):
    """Create a new Revenue column using Quantity × Price"""
    print("\n" + "=" * 60)
    print("TASK 4: Create a new Revenue column using Quantity × Price")
    print("=" * 60)
    
    df_with_revenue = df.withColumn("Revenue", col("Quantity_Sold") * col("Unit_Price"))
    
    print("Revenue column created successfully")
    df_with_revenue.select("Product_ID", "Quantity_Sold", "Unit_Price", "Revenue").show(10, truncate=False)
    
    return df_with_revenue

def calculate_category_wise_revenue(df):
    """Calculate category-wise revenue"""
    print("\n" + "=" * 60)
    print("TASK 5: Calculate category-wise revenue")
    print("=" * 60)
    
    category_revenue = df.groupBy("Product_Category") \
        .agg(spark_sum("Revenue").alias("Total_Revenue")) \
        .orderBy(desc("Total_Revenue"))
    
    print("Category-wise Revenue:")
    category_revenue.show(truncate=False)
    
    return category_revenue

def identify_top_selling_products(df):
    """Identify top-selling products based on quantity sold"""
    print("\n" + "=" * 60)
    print("TASK 6: Identify top-selling products based on quantity sold")
    print("=" * 60)
    
    top_products = df.groupBy("Product_ID", "Product_Category") \
        .agg(spark_sum("Quantity_Sold").alias("Total_Quantity_Sold")) \
        .orderBy(desc("Total_Quantity_Sold"))
    
    print("Top 10 Selling Products:")
    top_products.show(10, truncate=False)
    
    return top_products

def calculate_city_wise_revenue(df):
    """Calculate city-wise revenue (using Region as city proxy)"""
    print("\n" + "=" * 60)
    print("TASK 7: Calculate city-wise revenue")
    print("=" * 60)
    
    city_revenue = df.groupBy("Region") \
        .agg(spark_sum("Revenue").alias("Total_Revenue")) \
        .orderBy(desc("Total_Revenue"))
    
    print("Region-wise Revenue:")
    city_revenue.show(truncate=False)
    
    return city_revenue

def sort_categories_by_revenue(df):
    """Sort categories based on revenue generated"""
    print("\n" + "=" * 60)
    print("TASK 8: Sort categories based on revenue generated")
    print("=" * 60)
    
    sorted_categories = df.groupBy("Product_Category") \
        .agg(spark_sum("Revenue").alias("Total_Revenue"),
             spark_sum("Quantity_Sold").alias("Total_Quantity"),
             avg("Revenue").alias("Avg_Revenue_Per_Transaction")) \
        .orderBy(desc("Total_Revenue"))
    
    print("Categories sorted by Revenue (Descending):")
    sorted_categories.show(truncate=False)
    
    return sorted_categories

def display_analytical_summaries(df):
    """Display analytical summaries"""
    print("\n" + "=" * 60)
    print("TASK 9: Display analytical summaries")
    print("=" * 60)
    
    print("\n--- Overall Summary Statistics ---")
    df.select("Revenue", "Quantity_Sold", "Unit_Price", "Discount").describe().show()
    
    print("\n--- Revenue by Customer Type ---")
    df.groupBy("Customer_Type").agg(spark_sum("Revenue").alias("Total_Revenue")).show()
    
    print("\n--- Revenue by Payment Method ---")
    df.groupBy("Payment_Method").agg(spark_sum("Revenue").alias("Total_Revenue")).show()
    
    print("\n--- Revenue by Sales Channel ---")
    df.groupBy("Sales_Channel").agg(spark_sum("Revenue").alias("Total_Revenue")).show()
    
    print("\n--- Top 5 Sales Representatives ---")
    df.groupBy("Sales_Rep").agg(spark_sum("Revenue").alias("Total_Revenue")) \
        .orderBy(desc("Total_Revenue")).show(5)
    
    print("\n--- Average Discount by Category ---")
    df.groupBy("Product_Category").agg(avg("Discount").alias("Avg_Discount")).show()

def interpret_business_insights(df, category_revenue, top_products, city_revenue):
    """Interpret business insights and recommendations"""
    print("\n" + "=" * 60)
    print("TASK 10: Interpret business insights and recommendations")
    print("=" * 60)
    
    total_revenue = df.agg(spark_sum("Revenue")).collect()[0][0]
    total_quantity = df.agg(spark_sum("Quantity_Sold")).collect()[0][0]
    num_categories = df.select("Product_Category").distinct().count()
    num_regions = df.select("Region").distinct().count()
    
    print(f"\n{'='*60}")
    print("BUSINESS INSIGHTS & RECOMMENDATIONS")
    print(f"{'='*60}")
    
    print(f"\n1. OVERALL PERFORMANCE:")
    print(f"   - Total Revenue: ${total_revenue:,.2f}")
    print(f"   - Total Units Sold: {total_quantity:,}")
    print(f"   - Number of Product Categories: {num_categories}")
    print(f"   - Number of Regions: {num_regions}")
    
    top_category = category_revenue.first()
    print(f"\n2. TOP PERFORMING CATEGORY:")
    print(f"   - Category: {top_category['Product_Category']}")
    print(f"   - Revenue: ${top_category['Total_Revenue']:,.2f}")
    print(f"   - Contribution: {(top_category['Total_Revenue']/total_revenue)*100:.2f}%")
    
    top_product = top_products.first()
    print(f"\n3. TOP SELLING PRODUCT:")
    print(f"   - Product ID: {top_product['Product_ID']}")
    print(f"   - Category: {top_product['Product_Category']}")
    print(f"   - Units Sold: {top_product['Total_Quantity_Sold']:,}")
    
    top_region = city_revenue.first()
    print(f"\n4. TOP PERFORMING REGION:")
    print(f"   - Region: {top_region['Region']}")
    print(f"   - Revenue: ${top_region['Total_Revenue']:,.2f}")
    print(f"   - Contribution: {(top_region['Total_Revenue']/total_revenue)*100:.2f}%")
    
    print(f"\n5. RECOMMENDATIONS:")
    print(f"   - Focus marketing efforts on '{top_category['Product_Category']}' category")
    print(f"   - Investigate growth opportunities in underperforming regions")
    print(f"   - Consider bundle offers for top-selling products")
    print(f"   - Analyze customer type preferences for targeted campaigns")
    print(f"   - Optimize inventory for high-demand products")
    
    print(f"\n{'='*60}")

def main():
    file_path = "/home/heet18/Coding-Workspace/Futuristic/Heet/Github/Projects/Python-DataScience/apache_spark/dataset/sales_data.csv"
    
    spark = create_spark_session()
    
    try:
        df = load_sales_data(spark, file_path)
        display_schema(df)
        validate_data_types(df)
        df_with_revenue = create_revenue_column(df)
        category_revenue = calculate_category_wise_revenue(df_with_revenue)
        top_products = identify_top_selling_products(df_with_revenue)
        city_revenue = calculate_city_wise_revenue(df_with_revenue)
        sorted_categories = sort_categories_by_revenue(df_with_revenue)
        display_analytical_summaries(df_with_revenue)
        interpret_business_insights(df_with_revenue, category_revenue, top_products, city_revenue)
        
        print("\n✅ Practical 6 completed successfully!")
        
    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()