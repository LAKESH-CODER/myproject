import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import subprocess

spark = SparkSession.builder \
                    .master("local") \
                    .appName("job_1") \
                    .getOrCreate()

#remnoving success file
spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

#Declearing Input Parameters
input_path='gs://cloudrun_0711/Bank_application_data_20241127.CSV.txt'
output_path='gs://ingestion_bucket12/Bank_application_data_20241127.json'
custom_filename='Bank_application_data_20241127'

#reading data from the CSV file
df = spark.read.option("header", "true").csv(input_path)
print("reading file was successfull")

#transforming the datao0
cleaned_data = (
    df.withColumn("application_id", col("application_id").cast("string"))
    .withColumn("age", col("age").cast("int"))
    .withColumn("annual_income", col("annual_income").cast("int"))
    .withColumn("loan_amount", col("loan_amount").cast("int"))
    .withColumn("credit_score", col("credit_score").cast("int"))
    .withColumn("application_date", to_date(col("application_date"), "yyyy-MM-dd"))
    .where((col("age") > 1) & (col("annual_income") > 0) & (col("loan_amount") > 0))
    .where((col("credit_score") >= 300) & (col("credit_score") <= 850))
    .withColumn("DBI_Ratio", col("loan_amount") / col("annual_income"))
    .withColumn(
        "risk_category",
        when((col("credit_score") >= 750) & (col("credit_score") <= 850), "LowRisk")
        .when((col("credit_score") >= 650) & (col("credit_score") < 750), "MediumRisk")
        .when(col("credit_score") < 650, "HighRisk")
    )

)

print("Data cleaning is completd")

#Task3 - Writing data back to GCS bucket
cleaned_data.coalesce(1).write.mode("overwrite").json(output_path)

print("job completed")



# Define your GCS directory and custom filename


# List files in the GCS directory
result = subprocess.run(
    ["gsutil", "ls", output_path], capture_output=True, text=True
)

# Find the file that starts with 'part-'
files = result.stdout.splitlines()
part_file = next((file for file in files if "part-" in file), None)

if part_file:
    # Move the file to a custom name
    subprocess.run(
        ["gsutil", "mv", part_file, f"gs://ingestion_bucket12/{custom_filename}.json"]
    )
else:
    print("No part files found in the output directory.")



