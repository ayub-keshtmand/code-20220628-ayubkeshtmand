#!/usr/bin/env python3

import pytest
import os
import datetime
from glob import glob
from bmi import BMICalculator

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, FloatType
from pyspark_test import assert_pyspark_df_equal


b = BMICalculator()


@pytest.fixture
def get_test_spark_session():
    return SparkSession.builder.appName("TestBMICalculator").getOrCreate()


class TestBMICalculator:

    @pytest.fixture(autouse=True)
    def _get_test_spark_session(self, get_test_spark_session):
        self._spark = get_test_spark_session


    def test_create_folders(self):
        intermediate = os.path.join("..", "data", datetime.datetime.now().strftime("%Y-%m-%d"), "intermediate")
        output = os.path.join("..", "data", datetime.datetime.now().strftime("%Y-%m-%d"), "output")
        assert intermediate
        assert output


    def test_get_spark_session(self):
        assert isinstance(b.spark, SparkSession)


    def test_profiles_empty(self):
        assert b.df_profiles is None


    def test_read_profiles(self):
        data = [("Male",171.0,96.0),("Male",161.0,85.0),("Male",180.0,77.0),("Female",166.0,62.0),("Female",150.0,70.0),("Female",167.0,82.0)]
        schema = StructType([
            StructField("Gender", StringType()),
            StructField("HeightCm", FloatType(), False),
            StructField("WeightKg", FloatType(), False),
        ])
        expected_df = self._spark.createDataFrame(data, schema)
        b.read_profiles()
        assert_pyspark_df_equal(expected_df, b.df_profiles)


    def test_read_categories(self):
        data = [("Underweight",	"18.4 and below","Malnutrition risk"),
            ("Normal weight","18.5 - 24.9","Low risk"),
            ("Overweight","25 - 29.9","Enhanced risk"),
            ("Moderately obese","30 - 34.9","Medium risk"),
            ("Severely obese","35 - 39.9","High risk"),
            ("Very severely obese","40 and above","Very high risk")]
        schema = StructType([
            StructField("BMICategory", StringType(), False),
            StructField("BMIRange", StringType(), False),
            StructField("HealthRisk", StringType(), False),
        ])
        expected_df = self._spark.createDataFrame(data, schema)
        b.read_categories()
        assert_pyspark_df_equal(expected_df, b.df_categories)


    def test_write_df_no_partition(self):
        df = self._spark.createDataFrame([("p1",2,3), ("p2",5,6)], ("col1", "col2", "col3"))
        b.write_df(df, name="test1")
        path = os.path.join("data", datetime.datetime.now().strftime("%Y-%m-%d"), "intermediate", "test1")
        files = glob(os.path.join(path, "*.parquet"))
        assert os.path.exists(path)
        assert len(files) > 0


    def test_write_df_with_partition(self):
        df = self._spark.createDataFrame([("p1",2,3), ("p2",5,6)], ("col1", "col2", "col3"))
        b.write_df(df, name="test2", partitionBy="col1")
        path = os.path.join("data", datetime.datetime.now().strftime("%Y-%m-%d"), "intermediate", "test2")
        partition1, partition2 = os.path.join(path, "col1=p1"), os.path.join(path, "col1=p2")
        files1, files2 = glob(os.path.join(partition1, "*.parquet")), glob(os.path.join(partition2, "*.parquet"))
        assert os.path.exists(path)
        assert len(files1) > 0
        assert len(files2) > 0


    def test_write_df_to_reject(self):
        df = self._spark.createDataFrame([("p1",2,3), ("p2",5,6)], ("col1", "col2", "col3"))
        b.write_df(df, name="test1", reject=True)
        path = os.path.join("data", datetime.datetime.now().strftime("%Y-%m-%d"), "reject", "test1")
        files = glob(os.path.join(path, "*.parquet"))
        assert os.path.exists(path)
        assert len(files) > 0


    def test_calculate_bmi(self):
        assert round(b.calculate_bmi(mass=60, height=1.5), 2) == 26.67
        assert round(b.calculate_bmi(mass=66, height=1.76), 2) == 21.31
        assert round(b.calculate_bmi(mass=73, height=1.78), 2) == 23.04 
        assert round(b.calculate_bmi(mass=81, height=1.6), 2) == 31.64
        assert round(b.calculate_bmi(mass=90, height=1.85), 2) == 26.30
        assert round(b.calculate_bmi(mass=100, height=1.82), 2) == 30.19


    def test_add_bmi_to_profiles(self):
        data = [
            ("Male", 171.0, 96.0, 1.71, 32.8),
            ("Male", 161.0, 85.0, 1.61, 32.8),
            ("Male", 180.0, 77.0, 1.8, 23.8),
            ("Female", 166.0, 62.0, 1.66, 22.5),
            ("Female", 150.0, 70.0, 1.5, 31.1),
            ("Female", 167.0, 82.0, 1.67, 29.4)
        ]
        schema = StructType([
            StructField("Gender", StringType()),
            StructField("HeightCm", FloatType()),
            StructField("WeightKg", FloatType()),
            StructField("HeightM", DoubleType()),
            StructField("BMI", DoubleType()),
        ])
        expected_df = self._spark.createDataFrame(data, schema)
        b.add_bmi_to_profiles()
        
        # Round BMI values to 2 decimal places for testing purposes
        df = b.df_profiles.alias('df')
        df = df.withColumn("BMI", F.round(F.col("BMI"), 1))

        assert_pyspark_df_equal(expected_df, df)


    def test_split_bmi_range_in_categories(self):
        pass


    def test_add_categories_to_profiles(self):
        pass


    def clean_output(self):
        pass
    

    def test_write_bmi_profiles(self):
        pass


    def test_count_bmi_profiles(self):
        pass

