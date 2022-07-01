#!/usr/bin/env python3

import os
import datetime

from customlogger import logger
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType
import pyspark.sql.functions as F


log = logger(__name__)


class BMICalculator:
    """
    This class does the following calculates the BMI for each person profile in the profiles data and saves results to file.
    Input data must be in the ./data/input directory.
    """

    def __init__(self):

        # Storing data in src folder, although can use /tmp
        # self._dir = os.getcwd() # for testing locally or in notebook
        self._dir = os.path.dirname(os.path.abspath(__file__))
        self.path_data = os.path.join(self._dir, "data", datetime.datetime.now().strftime("%Y-%m-%d"))
        
        # Input data paths
        self.path_profiles = os.path.join(self._dir, "data", "input", "profiles.json")
        self.path_categories = os.path.join(self._dir, "data", "input", "category.csv")
        
        # Intermediate data paths
        self.path_intermediate = os.path.join(self.path_data, "intermediate")
        self.path_reject = os.path.join(self.path_data, "reject")
        
        # Output data paths
        self.path_profiles_bmi = os.path.join(self.path_data, "output", "profiles_bmi")
        self.path_total = os.path.join(self.path_data, "output", "total")

        self.create_folders()
        self.spark = self.get_spark_session()
        
        self.df_profiles = None
        self.df_categories = None
        self.df_profiles_bmi = None
        self.df_total = None
    
    
    def create_folders(self):
        folders = [self.path_intermediate, self.path_reject, os.path.join(self.path_data, "output")]
        for folder in folders:
            if os.path.exists(folder):
                log.info(f"'{folder}' exists. Nothing to do.")
            else:
                log.info(f"'{folder}' does not exist. Creating folder.")
                os.makedirs(folder)

    
    def get_spark_session(self):
        """Creates and returns SparkSession object."""
        spark = SparkSession.builder.appName("BMICalculator").getOrCreate()
        log.info(f"Created SparkSession {spark}")
        return spark


    def read_profiles(self):
        """Reads the profiles JSON file to a Spark DataFrame."""
        log.debug(f"Reading '{self.path_profiles}' to DataFrame")
        schema = StructType([
            StructField("Gender", StringType()),
            StructField("HeightCm", FloatType(), False), # Using FloatType in case of values that may not be rounded to whole integer.
            StructField("WeightKg", FloatType(), False),
        ])
        self.df_profiles = self.spark.read.json(path=self.path_profiles, schema=schema, multiLine=True)
        log.debug(f"Read {self.df_profiles.count()} rows from '{self.path_profiles}'")


    def read_categories(self):
        """Reads the category CSV file to a Spark DataFrame."""
        log.debug(f"Reading '{self.path_categories}' to DataFrame")
        schema = StructType([
            StructField("BMICategory", StringType(), False),
            StructField("BMIRange", StringType(), False),
            StructField("HealthRisk", StringType(), False),
        ])
        self.df_categories = self.spark.read.csv(path=self.path_categories, schema=schema, header=True)
        log.debug(f"Read {self.df_categories.count()} rows from '{self.path_categories}'")
    

    def write_df(self, df, name, partitionBy=None, reject=False):
        """
        Saves Spark DataFrame to file in Parquet format.
        We want to write data frames to disk at each intermediate step so we can trace back the data and query it in case of any issues.
        Example scenario where we would query intermediate datasets: Number of rows in the profiles output dataset is not equal to the source profiles data.
        Parquet format used for storage efficiency.

        Args:
            df (spark DataFrame): Spark DataFrame to write to Parquet.
            name (str): File/folder name output that stores data.
            partitionBy (str, optional): Column to partition by.
            dest (str, optional): Write DataFrame to "reject" directory instead of "intermediate".
        """
        if reject:
            path = os.path.join(self.path_reject, name)
        else:
            path = os.path.join(self.path_intermediate, name)
        
        if partitionBy:
            log.debug(f"Writing DataFrame (partition by '{partitionBy}') to '{path}'")
        else:
            log.debug(f"Writing DataFrame to '{path}'")
        
        df.write.parquet(path=path, mode="overwrite", partitionBy=partitionBy)


    @staticmethod
    def calculate_bmi(mass, height):
        """Calculates the BMI (Body Mass Index) and adds it as new column to df Spark DataFrame.
        The BMI in (kg/m2) is equal to the weight in kilograms (kg) divided by your height in meters squared (m^2).
        For example, if you are 175cm (1.75m) in height and 75kg in weight, you can calculate your BMI as follows: 75kg / (1.75m2) = 24.49kg/m2
        
        Args:
            mass (str): Column name of weight in kilograms (kg)
            height (str): Column name of height in meters (m)
        
        Returns:
            pyspark.sql.column.Column: BMI value"""

        log.debug(f"Calculating BMI for using columns mass '{mass}' and height '{height}'")
        return mass / height**2


    def add_bmi_to_profiles(self):
        """Calculates BMI and adds result as a new column to the profiles DataFrame."""
        self.df_profiles = self.df_profiles\
                .withColumn("HeightM", F.col("HeightCm")/100)\
                .withColumn( "BMI", self.calculate_bmi( F.col("WeightKg") , F.col("HeightM") ) )

        self.write_df(self.df_profiles, "add_bmi_to_profiles", partitionBy="Gender")


    def split_bmi_range_in_categories(self):
        """Splits the BMI Range column in the categories Spark DataFrame into a lower bound and an upper bound."""
        log.info("Adding lower and upper bound BMI values in the categories DataFrame.")
        self.df_categories = self.df_categories\
            .withColumn("BMIRangeSplit", F.split(F.col("BMIRange"), " and | - "))\
            .withColumn("BMIRangeLower", F.col("BMIRangeSplit").getItem(0).cast(FloatType()))\
            .withColumn("BMIRangeUpper", F.col("BMIRangeSplit").getItem(1).cast(FloatType()))
            # .drop(F.col("BMIRangeSplit")) # don't drop columns until completed all transformations and ready to output results

        self.write_df(self.df_categories, "split_bmi_range_in_categories")
    

    def add_categories_to_profiles(self):
        """Adds appropriate BMICategory to each profile in the profiles DataFrame via a join."""
        log.info("Adding BMICategory for each profile")
        self.df_profiles_bmi = self.df_profiles\
            .join(b.df_categories, on=self.df_profiles.BMI.between(self.df_categories.BMIRangeLower, self.df_categories.BMIRangeUpper), how="inner")
        self.write_df(self.df_profiles_bmi, "add_categories_to_profiles", partitionBy="BMICategory")

        log.info("Writing unmatched BMICategory values to rejected directory")
        df_profiles_bmi_reject = self.df_profiles\
            .join(b.df_categories, on=self.df_profiles.BMI.between(self.df_categories.BMIRangeLower, self.df_categories.BMIRangeUpper), how="left_anti")
        self.write_df(df_profiles_bmi_reject, "add_categories_to_profiles", reject=True)


    def clean_output(self):
        """Cleans the output DataFrame by selecting appropriate columns only."""
        self.df_profiles_bmi = self.df_profiles_bmi.select("Gender", "HeightCm", "WeightKg", "BMI", "BMICategory", "HealthRisk")
        self.write_df(self.df_profiles_bmi, "clean_output", partitionBy="BMICategory")
    

    def write_bmi_profiles(self):
        """Exports profiles BMI output DataFrame to JSON."""
        log.debug(f"Writing profiles BMI output DataFrame to '{self.path_profiles_bmi}'")
        self.df_profiles_bmi.write.json(self.path_profiles_bmi, mode="overwrite")


    def count_bmi_profiles(self, group_by_col="BMICategory", filter_on=None):
        """
        Counts the total number of profiles grouped by specified column. If filter_on specified then filters on this value.
        
        Args:
            group_by_col (str): Name of column to select and group results on.
            filter_on (str, optional): Value from the group_by_col column to filter results on.
        
        Returns:
            Spark DataFrame: Stores row count grouped by specified group_by_col column ordered by count in descending order. Assigns resulting DataFrame to df_total attribute
        """
        if filter_on:
            self.df_total = self.df_profiles_bmi\
                .select(group_by_col)\
                .filter(group_by_col == filter_on)\
                .groupby(group_by_col)\
                .agg( F.count("*").alias("ProfileCount") )\
                .orderBy( F.desc("ProfileCount") )
            
            print(f"Total profiles where {group_by_col} = 'filter_on':")
            self.df_total.show()

        else:
            self.df_total = self.df_profiles_bmi\
                .select(group_by_col)\
                .groupby(group_by_col)\
                .agg( F.count("*").alias("ProfileCount") )\
                .orderBy( F.desc("ProfileCount") )
            
            print(f"Total profiles grouped by {group_by_col}:")
            self.df_total.show()


    def write_count_bmi_profiles(self):
        """Exports df_total DataFrame to JSON."""
        log.debug(f"Writing totals DataFrame to '{self.path_total}'")
        self.df_total.write.json(self.path_total, mode="overwrite")
    

    def run(self):
        self.read_profiles()
        self.add_bmi_to_profiles()
        self.read_categories()
        self.split_bmi_range_in_categories()
        self.add_categories_to_profiles()
        self.clean_output()
        self.write_bmi_profiles()
        self.count_bmi_profiles()
        self.write_count_bmi_profiles()


if __name__ == "__main__":
    
    b = BMICalculator()
    b.run()

    # df_profiles.show()

    # +------+--------+--------+-------+------------------+
    # |Gender|HeightCm|WeightKg|HeightM|               BMI|
    # +------+--------+--------+-------+------------------+
    # |  Male|   171.0|    96.0|   1.71| 32.83061454806607|
    # |  Male|   161.0|    85.0|   1.61| 32.79194475521777|
    # |  Male|   180.0|    77.0|    1.8| 23.76543209876543|
    # |Female|   166.0|    62.0|   1.66| 22.49963710262738|
    # |Female|   150.0|    70.0|    1.5| 31.11111111111111|
    # |Female|   167.0|    82.0|   1.67|29.402273297715947|
    # +------+--------+--------+-------+------------------+

    # df_categories.show()

    # +-------------------+-----------------+-----------------+
    # |       BMICategory|BMIRange|      HealthRisk|
    # +-------------------+-----------------+-----------------+
    # |        Underweight|   18.4 and below|Malnutrition risk|
    # |      Normal weight|      18.5 - 24.9|         Low risk|
    # |         Overweight|        25 - 29.9|    Enhanced risk|
    # |   Moderately obese|        30 - 34.9|      Medium risk|
    # |     Severely obese|        35 - 39.9|        High risk|
    # |Very severely obese|     40 and above|   Very high risk|
    # +-------------------+-----------------+-----------------+



    
