from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark Session
spark = SparkSession.builder.appName("Valued Employees Without Suggestions").getOrCreate()

# Load Employee Data
df = spark.read.csv("/workspaces/spark-structured-api-employee-engagement-analysis-saivenkatchoppara/EmployeeEngagementAnalysis/input/employee_data.csv", header=True, inferSchema=True)


# Identify employees who feel valued (SatisfactionRating ≥ 4) but haven’t provided suggestions
valued_no_suggestions = df.filter((col("SatisfactionRating") >= 4) & (col("ProvidedSuggestions") == False))

# Count the number of such employees
valued_no_suggestions_count = valued_no_suggestions.count()
total_employees = df.count()
proportion = (valued_no_suggestions_count / total_employees) * 100

# Convert to DataFrame for saving
valued_df = spark.createDataFrame([(valued_no_suggestions_count, proportion)], ["NumEmployees", "Proportion"])

# Save output to CSV
valued_df.write.csv("output/valued_no_suggestions", header=True)

print("Task 2 Completed. Results saved in 'output/valued_no_suggestions'.")

# Stop Spark Session
spark.stop()