from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg, col, lit
import os

# Create SparkSession
spark = SparkSession.builder \
    .appName("TaxiDataPipeline") \
    .getOrCreate()

# Define data paths
yellow_taxi_path = os.path.join(os.getcwd(), "/home/parallels/vscode_projects/python/nurcihan/RawData/yellow_tripdata_2021-01.csv")
green_taxi_path = os.path.join(os.getcwd(), "/home/parallels/vscode_projects/python/nurcihan/RawData/green_tripdata_2021-01.csv")
bronze_dir = os.path.join(os.getcwd(), "/home/parallels/vscode_projects/python/nurcihan/PipelineData/Bronze")
silver_dir = os.path.join(os.getcwd(), "/home/parallels/vscode_projects/python/nurcihan/PipelineData/Silver")
gold_dir = os.path.join(os.getcwd(), "/home/parallels/vscode_projects/python/nurcihan/PipelineData/Gold")

# housekeeping, delete all files from the previous run
try:
    # if the directory contains files, remove them
    if os.path.exists(bronze_dir) and os.path.isdir(bronze_dir):
        os.system(f"rm -rf {bronze_dir}/*")
    
    if os.path.exists(silver_dir) and os.path.isdir(silver_dir):
        os.system(f"rm -rf {silver_dir}/*")

    if os.path.exists(gold_dir) and os.path.isdir(gold_dir):
        os.system(f"rm -rf {gold_dir}/*")

except Exception as e:
    print(f"Error cleaning in target directories: {e}")
    exit(1)

# step 1 starts here
try:

    # Read yellow and green taxi data
    yellow_df = spark.read.csv(yellow_taxi_path, header=True)
    green_df = spark.read.csv(green_taxi_path, header=True)

except Exception as e:
    print(f"Error reading data in step1: {e}")
    exit(1)

# save the raw data as parquet files
try:

    yellow_df.write.parquet(f"{bronze_dir}/yellow_tripdata.parquet")
    green_df.write.parquet(f"{bronze_dir}/green_tripdata.parquet")

except Exception as e:
    print(f"Error writing data in step1: {e}")
    exit(1)

# step 1 completed

# step 2 starts here
try:

    # rename columns in green taxi data and yellow taxi data to match the silver table schema
    yellow_df = yellow_df.withColumnRenamed("VendorID", "VendorId") \
        .withColumnRenamed("tpep_pickup_datetime", "PickUpDateTime") \
        .withColumnRenamed("tpep_dropoff_datetime", "DropOffDateTime") \
        .withColumnRenamed("PULocationID", "PickUpLocationId") \
        .withColumnRenamed("DOLocationID", "DropOffLocationId") \
        .withColumnRenamed("passenger_count", "PassengerCount") \
        .withColumnRenamed("trip_distance", "TripDistance") \
        .withColumnRenamed("tip_amount", "TipAmount") \
        .withColumnRenamed("total_amount", "TotalAmount")
    
    green_df = green_df.withColumnRenamed("VendorID", "VendorId") \
        .withColumnRenamed("lpep_pickup_datetime", "PickUpDateTime") \
        .withColumnRenamed("lpep_dropoff_datetime", "DropOffDateTime") \
        .withColumnRenamed("PULocationID", "PickUpLocationId") \
        .withColumnRenamed("DOLocationID", "DropOffLocationId") \
        .withColumnRenamed("passenger_count", "PassengerCount") \
        .withColumnRenamed("trip_distance", "TripDistance") \
        .withColumnRenamed("tip_amount", "TipAmount") \
        .withColumnRenamed("total_amount", "TotalAmount")
    
    # Apply the unified schema to both dataframes
    yellow_df = yellow_df.select("VendorId", "PickUpDateTime", "DropOffDateTime", "PickUpLocationId", "DropOffLocationId", "PassengerCount", "TripDistance", "TipAmount", "TotalAmount")
    green_df = green_df.select("VendorId", "PickUpDateTime", "DropOffDateTime", "PickUpLocationId", "DropOffLocationId", "PassengerCount", "TripDistance", "TipAmount", "TotalAmount")
    
    # Merge dataframes
    merged_df = yellow_df.unionByName(green_df)

    # write merged data into parquet file
    merged_df.write.parquet(f"{silver_dir}/merged_tripdata.parquet")
    
except Exception as e:
    print(f"Error in step2: {e}")
    exit(1)

# step 2 completed

# step 3 starts here
try:

    # Validation rule 1: every trip must have at least 1 passenger
    valid_df = merged_df.filter(col("PassengerCount") >= 1)
    invalid_df = merged_df.filter((col("PassengerCount") < 1) | col("PassengerCount").isNull())

    # Validatation rule 2: In the valid dataset if the VendorId is NULL, update with 999
    valid_df = valid_df.fillna(999, subset=["VendorId"])

    # write the valid data into parquet file
    valid_df.write.parquet(f"{silver_dir}/yellow_tripdata_valid.parquet")

    # write the invalid data into csv file
    invalid_df.write.csv(f"{silver_dir}/yellow_tripdata_invalid.csv")


except Exception as e:
    print(f"Error in step3: {e}")
    exit(1)
# step 3 completed

# step 4 starts here
try:

    # Dedupe based on Pickup location, Pick up Time, Drop off time, drop off location and vendor id
    deduped_df = valid_df.dropDuplicates(["PickUpLocationId", "PickUpDateTime", "DropOffDateTime", "DropOffLocationId", "VendorId"])

except Exception as e:
    print(f"Error in step4: {e}")
    exit(1)
# step 4 completed

# step 5 starts here
try:
    
    # Calculate aggregations for Locations
    locations_df = deduped_df.groupBy("PickUpLocationId").agg(
        sum("TotalAmount").alias("TotalFares"),
        sum("TipAmount").alias("TotalTips"),
        avg("TripDistance").alias("AverageDistance")
    ).withColumn("LocationType", lit("PickUp")) \
     .withColumnRenamed("PickUpLocationId", "LocationId")

    # Calculate the average distance by dropoff location separately
    dropoff_avg_distance_df = deduped_df.groupBy("DropOffLocationId").agg(
        avg("TripDistance").alias("AverageDistance")
    ).withColumn("TotalFares", lit(0)) \
    .withColumn("TotalTips", lit(0)) \
    .withColumn("LocationType", lit("DropOff")) \
    .withColumnRenamed("DropOffLocationId", "LocationId") \
    .select("LocationId", "TotalFares", "TotalTips", "AverageDistance", "LocationType")
    
    # Union the two dataframes
    locations_df = locations_df.unionByName(dropoff_avg_distance_df)
        
    # Calculate aggregations for Vendors
    vendors_df = deduped_df.groupBy("VendorId").agg(
        sum("TotalAmount").alias("TotalFares"),
        sum("TipAmount").alias("TotalTips"),
        avg("TotalAmount").alias("AverageFare"),
        avg("TipAmount").alias("AverageTips")
    )

    # Save results as CSV files
    locations_df.write.csv(f"{bronze_dir}/locations.csv")
    vendors_df.write.csv(f"{bronze_dir}/vendors.csv")

except Exception as e:
    print(f"Error in step5: {e}")
    exit(1)

# step 5 completed


# Stop SparkSession (optional)
spark.stop()