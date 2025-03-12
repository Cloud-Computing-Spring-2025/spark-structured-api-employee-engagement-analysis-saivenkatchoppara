from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, avg

# Initialize Spark Session
spark = SparkSession.builder.appName("Compare Engagement Levels").getOrCreate()

# Load Employee Data
df = spark.read.csv("/workspaces/spark-structured-api-employee-engagement-analysis-saivenkatchoppara/EmployeeEngagementAnalysis/input/employee_data.csv", header=True, inferSchema=True)


# Assign numeric scores to Engagement Levels
df = df.withColumn("EngagementScore", when(col("EngagementLevel") == "Low", 1)
                                      .when(col("EngagementLevel") == "Medium", 3)
                                      .when(col("EngagementLevel") == "High", 5))

# Compute average engagement score per JobTitle
job_title_engagement = df.groupBy("JobTitle").agg(avg("EngagementScore").alias("AvgEngagementLevel"))

# Save output to CSV
job_title_engagement.orderBy(col("AvgEngagementLevel").desc()).write.csv("output/engagement_by_job", header=True)

print("Task 3 Completed. Results saved in 'output/engagement_by_job'.")

# Stop Spark Session
spark.stop()