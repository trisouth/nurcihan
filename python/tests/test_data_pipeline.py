import unittest
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

class TestTaxiDataPipeline(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("TaxiDataPipelineTest") \
            .master("local") \
            .getOrCreate()
        
        # Path to your data files
        cls.yellow_file_path = os.path.join(os.path.dirname(__file__), "../../RawData", "yellow_tripdata_2021-01.csv")
        cls.green_file_path = os.path.join(os.path.dirname(__file__), "../../RawData", "green_tripdata_2021-01.csv")

        # Load DataFrames from CSV files
        cls.yellow_df = cls.spark.read.csv(cls.yellow_file_path, header=True, inferSchema=True)
        cls.green_df = cls.spark.read.csv(cls.green_file_path, header=True, inferSchema=True)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_data_reading(self):
        # Check if DataFrames are created
        self.assertIsNotNone(self.yellow_df)
        self.assertIsNotNone(self.green_df)
        self.assertEqual(self.yellow_df.count(), 1369819)
        self.assertEqual(self.green_df.count(), 76539)

    def test_data_transformation(self):
        # Transform yellow data
        yellow_transformed = self.yellow_df \
            .withColumnRenamed("VendorID", "VendorId") \
            .withColumnRenamed("tpep_pickup_datetime", "PickUpDateTime") \
            .withColumnRenamed("tpep_dropoff_datetime", "DropOffDateTime") \
            .withColumnRenamed("PULocationID", "PickUpLocationId") \
            .withColumnRenamed("DOLocationID", "DropOffLocationId") \
            .withColumnRenamed("passenger_count", "PassengerCount") \
            .withColumnRenamed("trip_distance", "TripDistance") \
            .withColumnRenamed("tip_amount", "TipAmount") \
            .withColumnRenamed("total_amount", "TotalAmount")
        
        self.assertEqual(yellow_transformed.columns, ["VendorId", "PickUpDateTime", "DropOffDateTime", 
                                                      "PickUpLocationId", "DropOffLocationId", 
                                                      "PassengerCount", "TripDistance", "TipAmount", "TotalAmount"])

    # def test_data_validation(self):
    #     # Validate data based on rules
    #     merged_df = self.yellow_df.unionByName(self.green_df)

    #     valid_df = merged_df.filter(col("passenger_count") >= 1)
    #     invalid_df = merged_df.filter((col("passenger_count") < 1) | col("passenger_count").isNull())
    #     valid_df = valid_df.fillna(999, subset=["VendorID"])

    #     self.assertEqual(valid_df.count(), 3)
    #     self.assertEqual(invalid_df.count(), 2)

    # def test_data_deduplication(self):
    #     merged_df = self.yellow_df.unionByName(self.green_df)
    #     deduped_df = merged_df.dropDuplicates(["PULocationID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "DOLocationID", "VendorID"])
    #     self.assertEqual(deduped_df.count(), 5)  # Adjust the count based on your unique records

if __name__ == '__main__':
    unittest.main()
