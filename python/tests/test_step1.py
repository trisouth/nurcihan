import unittest
import os, sys
from pyspark.sql import SparkSession

# Add the src directory to the path for importing the main module
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

from main2 import read_csv_files, save_df_as_parquet
from pyspark.sql.utils import AnalysisException

class TestStep1(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Create SparkSession for testing
        cls.spark = SparkSession.builder.master("local").appName("TestTaxiDataPipeline").getOrCreate()
        
        # Define test file paths
        cls.test_yellow_taxi_path = "/home/parallels/vscode_projects/python/nurcihan/RawData/yellow_tripdata_2021-01.csv"
        cls.test_green_taxi_path = "/home/parallels/vscode_projects/python/nurcihan/RawData/green_tripdata_2021-01.csv"

        # Define parquet test paths
        cls.test_yellow_parquet_path = "/home/parallels/vscode_projects/python/nurcihan/PipelineData/Bronze/yellow_tripdata.parquet"
        cls.test_green_parquet_path = "/home/parallels/vscode_projects/python/nurcihan/PipelineData/Bronze/green_tripdata.parquet"

        # Expected schema column names for yellow and green taxi data
        cls.expected_yellow_columns = [
            "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count", "trip_distance",
            "RatecodeID", "store_and_fwd_flag", "PULocationID", "DOLocationID", "payment_type", "fare_amount",
            "extra", "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge", "total_amount", "congestion_surcharge"
        ]
        cls.expected_green_columns = [
            "VendorID", "lpep_pickup_datetime", "lpep_dropoff_datetime", "store_and_fwd_flag", "RatecodeID",
            "PULocationID", "DOLocationID", "passenger_count", "trip_distance", "fare_amount", "extra",
            "mta_tax", "tip_amount", "tolls_amount", "ehail_fee", "improvement_surcharge", "total_amount",
            "payment_type", "trip_type", "congestion_surcharge"
        ]

        # Expected row counts
        cls.expected_yellow_row_count = 1369819
        cls.expected_green_row_count = 76539

    @classmethod
    def tearDownClass(cls):
        # Stop the Spark session after tests
        cls.spark.stop()

    def test_read_csv_files_yellow(self):
        # Test reading yellow taxi data
        yellow_df = read_csv_files(spark=self.spark, file_path=self.test_yellow_taxi_path)
        
        # Assert columns
        self.assertEqual(set(yellow_df.columns), set(self.expected_yellow_columns), "Yellow taxi columns do not match expected")
        
        # Assert row count
        self.assertEqual(yellow_df.count(), self.expected_yellow_row_count, "Yellow taxi row count does not match expected")

    def test_read_csv_files_green(self):
        # Test reading green taxi data
        green_df = read_csv_files(spark=self.spark, file_path=self.test_green_taxi_path)
        
        # Assert columns
        self.assertEqual(set(green_df.columns), set(self.expected_green_columns), "Green taxi columns do not match expected")
        
        # Assert row count
        self.assertEqual(green_df.count(), self.expected_green_row_count, "Green taxi row count does not match expected")

    def test_save_df_as_parquet_yellow(self):
        # Read yellow taxi data and save as parquet
        yellow_df = read_csv_files(spark=self.spark, file_path=self.test_yellow_taxi_path)
        save_df_as_parquet(df=yellow_df, target_destination=self.test_yellow_parquet_path)
        
        # Check if parquet file exists
        self.assertTrue(os.path.exists(self.test_yellow_parquet_path), "Yellow Parquet file was not created")
        
        # Load the saved parquet file
        parquet_df = self.spark.read.parquet(self.test_yellow_parquet_path)
        
        # Assert columns
        self.assertEqual(set(parquet_df.columns), set(self.expected_yellow_columns), "Parquet yellow taxi columns do not match expected")

    def test_save_df_as_parquet_green(self):
        # Read green taxi data and save as parquet
        green_df = read_csv_files(spark=self.spark, file_path=self.test_green_taxi_path)
        save_df_as_parquet(df=green_df, target_destination=self.test_green_parquet_path)
        
        # Check if parquet file exists
        self.assertTrue(os.path.exists(self.test_green_parquet_path), "Green Parquet file was not created")
        
        # Load the saved parquet file
        parquet_df = self.spark.read.parquet(self.test_green_parquet_path)
        
        # Assert columns
        self.assertEqual(set(parquet_df.columns), set(self.expected_green_columns), "Parquet green taxi columns do not match expected")

if __name__ == '__main__':
    unittest.main()
    # utku
