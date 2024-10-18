import unittest
import os, sys
from pyspark.sql import SparkSession

# Add the src directory to the path for importing the main module
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

from main2 import create_spark_session

class TestMain2(unittest.TestCase):

    def test_create_spark_session(self):
        # Create a Spark session
        spark = create_spark_session()

        # Check if Spark session is created successfully
        self.assertIsInstance(spark, SparkSession, "Spark session was not created")

        # Check if the app name is correctly set
        self.assertEqual(spark.conf.get("spark.app.name"), "TaxiDataPipeline")

if __name__ == "__main__":
    unittest.main()