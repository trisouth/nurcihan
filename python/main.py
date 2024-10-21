from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg, col, lit
import os
from custom_exception import CustomException

# Define data paths
yellow_taxi_path = os.path.join(os.getcwd(), "/home/parallels/vscode_projects/python/nurcihan/RawData/yellow_tripdata_2021-01.csv")
green_taxi_path = os.path.join(os.getcwd(), "/home/parallels/vscode_projects/python/nurcihan/RawData/green_tripdata_2021-01.csv")
bronze_dir = os.path.join(os.getcwd(), "/home/parallels/vscode_projects/python/nurcihan/PipelineData/Bronze")
silver_dir = os.path.join(os.getcwd(), "/home/parallels/vscode_projects/python/nurcihan/PipelineData/Silver")
gold_dir = os.path.join(os.getcwd(), "/home/parallels/vscode_projects/python/nurcihan/PipelineData/Gold")

yellow_tripdata_parquet = f"{bronze_dir}/yellow_tripdata.parquet"
green_tripdata_parquet = f"{bronze_dir}/green_tripdata.parquet"
merged_tripdata_parquet = f"{silver_dir}/merged_tripdata.parquet"

yellow_tripdata_valid_parquet = f"{silver_dir}/yellow_tripdata_valid.parquet"
yellow_tripdata_invalid_parquet = f"{silver_dir}/yellow_tripdata_invalid.csv"

locations_csv = f"{gold_dir}/locations.csv"
vendors_csv = f"{gold_dir}/vendors.csv"

spark = None

def create_spark_session():

    # Create SparkSession
    try:
        return SparkSession.builder.appName("TaxiDataPipeline").master("local").getOrCreate()
    except Exception as e:
        print(f"Error creating Spark session: {e}")
        exit(1)

def housekeeping():
    try:
        # if the directory contains files, remove them
        if os.path.exists(bronze_dir) and os.path.isdir(bronze_dir):
            os.system(f"rm -rf {bronze_dir}/*")
        
        if os.path.exists(silver_dir) and os.path.isdir(silver_dir):
            os.system(f"rm -rf {silver_dir}/*")

        if os.path.exists(gold_dir) and os.path.isdir(gold_dir):
            os.system(f"rm -rf {gold_dir}/*")

    except Exception as e:
        print(f"Error cleaning target directories: {e}")
        exit(1)

def read_csv_files(spark, file_path):

    try:

        # read the csv file and return the dataframe 
        return spark.read.csv(file_path, header=True)
    
    except Exception as e:
        print(f"Error reading data in step1: {e}")
        exit(1)

def save_df_as_parquet(df, target_destination):

    try:

        # write the data in dataframes into target parquet files
        df.write.parquet(target_destination)

    except Exception as e:
        print(f"Error writing data in parquet format to : {e}")
        exit(1)

def save_df_as_csv(df, target_destination):

    try:

        # write the data in dataframes into target csv files
        df.write.csv(target_destination)

    except Exception as e:
        print(f"Error writing data in csv format to : {e}")
        exit(1)

def rename_transform_columns(df, taxi_type):

    try:

        # rename columns for standardization
        df = df.withColumnRenamed("VendorID", "VendorId") \
            .withColumnRenamed("PULocationID", "PickUpLocationId") \
            .withColumnRenamed("DOLocationID", "DropOffLocationId") \
            .withColumnRenamed("passenger_count", "PassengerCount") \
            .withColumnRenamed("trip_distance", "TripDistance") \
            .withColumnRenamed("tip_amount", "TipAmount") \
            .withColumnRenamed("total_amount", "TotalAmount")
        
        if taxi_type == 'Y':

            df = df.withColumnRenamed("tpep_pickup_datetime", "PickUpDateTime") \
                .withColumnRenamed("tpep_dropoff_datetime", "DropOffDateTime")
            
        elif taxi_type == 'G':

            df = df.withColumnRenamed("lpep_pickup_datetime", "PickUpDateTime") \
                .withColumnRenamed("lpep_dropoff_datetime", "DropOffDateTime")
        
        else:
            
            # raise an exception of unknown taxi type
            raise CustomException(f"Unknown taxi type : {taxi_type}", 1001)
        
        return df.select("VendorId", "PickUpDateTime", "DropOffDateTime", "PickUpLocationId", "DropOffLocationId", "PassengerCount", "TripDistance", "TipAmount", "TotalAmount")
    
    except Exception as e:

        print(f"Error renaming columns : {e} for taxi_type : {taxi_type}")
        exit(1)

def merge_dataframes(first_df, second_df):

    try:

        # merge dataframes
        return first_df.unionByName(second_df)
    
    except Exception as e:

        print(f"Error merging dataframes : {e}")
        exit(1)

def filter_df(df, col):

    try:

        # filter the dataframes
        return df.filter(col)
    
    except Exception as e:

        print(f"Error filtering dataframes : {e}")
        exit(1)

def replace_null_values(df, col, value):

    try:

        # replace null values with the given value
        return df.fillna(value, subset=[col])
    
    except Exception as e:

        print(f"Error replacing null values : {e}")
        exit(1)

def deduplicate_df(df, cols):

    try:

        # deduplicate the dataframes
        return df.dropDuplicates(cols)
    
    except Exception as e:

        print(f"Error deduplicating dataframes : {e}")
        exit(1)

def aggregate_locations_df(df):

    try:
        
        # Calculate aggregations for Locations
        return df.groupBy("PickUpLocationId").agg(
            sum("TotalAmount").alias("TotalFares"),
            sum("TipAmount").alias("TotalTips"),
            avg("TripDistance").alias("AverageDistance")
        ).withColumn("LocationType", lit("PickUp")) \
        .withColumnRenamed("PickUpLocationId", "LocationId")

    except Exception as e:

        print(f"Error aggregating locations : {e}")
        exit(1)

def aggregate_dropoffs(df):

    try:

        # Calculate the average distance by dropoff location separately
        return df.groupBy("DropOffLocationId").agg(
            avg("TripDistance").alias("AverageDistance")
            ).withColumn("TotalFares", lit(0)) \
            .withColumn("TotalTips", lit(0)) \
            .withColumn("LocationType", lit("DropOff")) \
            .withColumnRenamed("DropOffLocationId", "LocationId") \
            .select("LocationId", "TotalFares", "TotalTips", "AverageDistance", "LocationType")

    except Exception as e:

        print(f"Error aggregating dropoffs : {e}")
        exit(1)

def aggregate_vendors(df):
    
    try:
        # Calculate aggregations for Vendors
        return df.groupBy("VendorId").agg(
            sum("TotalAmount").alias("TotalFares"),
            sum("TipAmount").alias("TotalTips"),
            avg("TotalAmount").alias("AverageFare"),
            avg("TipAmount").alias("AverageTips")
        )

    except Exception as e:

        print(f"Error aggregating vendors : {e}")
        exit(1)

def stop_spark_session():
    if spark:
        spark.stop()

# Main execution
if __name__ == "__main__":
    
    # create sparkSession
    spark = create_spark_session()

    # perform housekeeping, delete files from previous runs
    housekeeping()

    # step 1 - load the raw data into initial df's
    yellow_df = read_csv_files(spark=spark, file_path=yellow_taxi_path)
    green_df = read_csv_files(spark=spark, file_path=green_taxi_path)

    # step 1 - save the raw dataframes as parquet files
    save_df_as_parquet(df=yellow_df, target_destination=yellow_tripdata_parquet)
    save_df_as_parquet(df=green_df, target_destination=green_tripdata_parquet)

    # step 2 - rename and reduce columns
    yellow_df = rename_transform_columns(df=yellow_df, taxi_type='Y')
    green_df = rename_transform_columns(df=green_df, taxi_type='G')

    merged_df = merge_dataframes(first_df=yellow_df, second_df=green_df)
    save_df_as_parquet(df=merged_df, target_destination=merged_tripdata_parquet)

    # step 3 - apply validation rules

    # validation rule 1 based on the passenger count
    valid_df = filter_df(df=merged_df, col=col("PassengerCount") >= 1)
    invalid_df = filter_df(merged_df, (col("PassengerCount") < 1) | col("PassengerCount").isNull())

    # validation rule 2 based on the vendor id
    valid_df = replace_null_values(df=valid_df, col="VendorId", value=999)

    save_df_as_parquet(df=valid_df, target_destination=yellow_tripdata_valid_parquet)
    save_df_as_csv(df=invalid_df, target_destination=yellow_tripdata_invalid_parquet)

    # step 4 - deduplicate the data
    deduped_df = deduplicate_df(df=valid_df, cols=["PickUpLocationId", "PickUpDateTime", "DropOffDateTime", "DropOffLocationId", "VendorId"])

    # step 5 - apply aggregations
    locations_df = aggregate_locations_df(df=deduped_df)
    dropoff_df = aggregate_dropoffs(df=deduped_df)

    merged_df = merge_dataframes(locations_df, dropoff_df)

    vendors_df = aggregate_vendors(df=deduped_df)

    save_df_as_csv(df=merged_df, target_destination=locations_csv)
    save_df_as_csv(df=vendors_df, target_destination=vendors_csv)

    stop_spark_session()