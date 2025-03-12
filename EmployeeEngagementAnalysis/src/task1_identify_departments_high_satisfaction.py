from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

# Initialize Spark Session
spark = SparkSession.builder.appName("High Satisfaction Departments").getOrCreate()

# Load Employee Data
df = spark.read.csv("/workspaces/spark-structured-api-employee-engagement-analysis-saivenkatchoppara/EmployeeEngagementAnalysis/input/employee_data.csv", header=True, inferSchema=True)


# Filter employees with SatisfactionRating > 4 and EngagementLevel = 'High'
filtered_df = df.filter((col("SatisfactionRating") > 4) & (col("EngagementLevel") == "High"))

# Count total employees per department
department_counts = df.groupBy("Department").agg(count("*").alias("TotalEmployees"))

# Count employees who meet criteria
high_satisfaction_counts = filtered_df.groupBy("Department").agg(count("*").alias("HighSatisfactionCount"))

# Join counts and calculate percentage
result = department_counts.join(high_satisfaction_counts, "Department", "left").fillna(0)
result = result.withColumn("Percentage", (col("HighSatisfactionCount") / col("TotalEmployees")) * 100)

# Filter departments exceeding 50%
result = result.filter(col("Percentage") > 5).select("Department", "Percentage")

# Save output to CSV
result.write.csv("output/departments_high_satisfaction_departments", header=True)

print("Task 1 Completed. Results saved in 'output/departments_high_satisfaction_departments'.")

# Stop Spark Session
spark.stop()