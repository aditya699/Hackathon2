import os
import warnings
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import pandas as pd
from pyspark.sql.functions import col

def data_analyzer(filepath:str, outputpath:str,data_type:str) -> pd.DataFrame:
    if data_type =='train':
                try:
                        # Suppress warnings
                        os.environ["PYSPARK_LOG_LEVEL"] = "ERROR"
                        warnings.filterwarnings("ignore")

                        # Create a SparkSession
                        spark = SparkSession.builder.appName("Data Analysis").getOrCreate()

                        # Define the column names
                        columns = ['MSSubClass', 'MSZoning', 'LotFrontage', 'LotArea', 'Street', 'LotShape', 'LandContour', 'Utilities',
                        'LotConfig', 'LandSlope', 'Neighborhood', 'Condition1', 'BldgType', 'HouseStyle', 'RoofStyle',
                        'RoofMatl', 'Exterior1st', 'Exterior2nd', 'MasVnrType', "MasVnrArea", "ExterQual", "ExterCond",
                        "Foundation", "BsmtQual", "BsmtCond", "BsmtExposure", "BsmtFinType1", "BsmtFinSF1", "BsmtUnfSF",
                        "TotalBsmtSF", "Heating", "HeatingQC", "CentralAir", "Electrical", "1stFlrSF", "2ndFlrSF",
                        "LowQualFinSF", "GrLivArea", "BsmtFullBath", "BsmtHalfBath", "FullBath", "HalfBath",
                        "BedroomAbvGr", "KitchenAbvGr", "KitchenQual", "TotRmsAbvGrd", "Functional", "Fireplaces",
                        "GarageType", "GarageFinish", "GarageCars", "GarageArea", "GarageQual", "GarageCond", "PavedDrive",
                        "WoodDeckSF", "OpenPorchSF", "EnclosedPorch", "3SsnPorch", "ScreenPorch", "PoolArea", "MiscVal",
                        "MoSold", "SaleType", "SaleCondition", "SalePrice", "OverallState", "Age_of_House",
                        "Age_of_Repair", "Age_of_Garage", "Age_of_House_Sold"]


                       # Define the schema for the DataFrame
                        schema = StructType([
                                StructField("MSSubClass", IntegerType(), nullable=True),
                                StructField("MSZoning", StringType(), nullable=True),
                                StructField("LotFrontage", DoubleType(), nullable=True),
                                StructField("LotArea", IntegerType(), nullable=True),
                                StructField("Street", StringType(), nullable=True),
                                StructField("LotShape", StringType(), nullable=True),
                                StructField("LandContour", StringType(), nullable=True),
                                StructField("Utilities", StringType(), nullable=True),
                                StructField("LotConfig", StringType(), nullable=True),
                                StructField("LandSlope", IntegerType(), nullable=True),
                                StructField("Neighborhood", StringType(), nullable=True),
                                StructField("Condition1", StringType(), nullable=True),
                                StructField("BldgType", StringType(), nullable=True),
                                StructField("HouseStyle", StringType(), nullable=True),
                                StructField("RoofStyle", StringType(), nullable=True),
                                StructField("RoofMatl", StringType(), nullable=True),
                                StructField("Exterior1st", StringType(), nullable=True),
                                StructField("Exterior2nd", StringType(), nullable=True),
                                StructField("MasVnrType", StringType(), nullable=True),
                                StructField("MasVnrArea", DoubleType(), nullable=True),
                                StructField("ExterQual", IntegerType(), nullable=True),
                                StructField("ExterCond", IntegerType(), nullable=True),
                                StructField("Foundation", StringType(), nullable=True),
                                StructField("BsmtQual", StringType(), nullable=True),
                                StructField("BsmtCond", StringType(), nullable=True),
                                StructField("BsmtExposure", StringType(), nullable=True),
                                StructField("BsmtFinType1", StringType(), nullable=True),
                                StructField("BsmtFinSF1", IntegerType(), nullable=True),
                                StructField("BsmtUnfSF", IntegerType(), nullable=True),
                                StructField("TotalBsmtSF", IntegerType(), nullable=True),
                                StructField("Heating", StringType(), nullable=True),
                                StructField("HeatingQC", IntegerType(), nullable=True),
                                StructField("CentralAir", StringType(), nullable=True),
                                StructField("Electrical", StringType(), nullable=True),
                                StructField("1stFlrSF", IntegerType(), nullable=True),
                                StructField("2ndFlrSF", IntegerType(), nullable=True),
                                StructField("LowQualFinSF", IntegerType(), nullable=True),
                                StructField("GrLivArea", IntegerType(), nullable=True),
                                StructField("BsmtFullBath", IntegerType(), nullable=True),
                                StructField("BsmtHalfBath", IntegerType(), nullable=True),
                                StructField("FullBath", IntegerType(), nullable=True),
                                StructField("HalfBath", IntegerType(), nullable=True),
                                StructField("BedroomAbvGr", IntegerType(), nullable=True),
                                StructField("KitchenAbvGr", IntegerType(), nullable=True),
                                StructField("KitchenQual", StringType(), nullable=True),
                                StructField("TotRmsAbvGrd", IntegerType(), nullable=True),
                                StructField("Functional", StringType(), nullable=True),
                                StructField("Fireplaces", IntegerType(), nullable=True),
                                StructField("GarageType", StringType(), nullable=True),
                                StructField("GarageFinish", StringType(), nullable=True),
                                StructField("GarageCars", IntegerType(), nullable=True),
                                StructField("GarageArea", IntegerType(), nullable=True),
                                StructField("GarageQual", StringType(), nullable=True),
                                StructField("GarageCond", StringType(), nullable=True),
                                StructField("PavedDrive", StringType(), nullable=True),
                                StructField("WoodDeckSF", IntegerType(), nullable=True),
                                StructField("OpenPorchSF", IntegerType(), nullable=True),
                                StructField("EnclosedPorch", IntegerType(), nullable=True),
                                StructField("3SsnPorch", IntegerType(), nullable=True),
                                StructField("ScreenPorch", IntegerType(), nullable=True),
                                StructField("PoolArea", IntegerType(), nullable=True),
                                StructField("MiscVal", IntegerType(), nullable=True),
                                StructField("MoSold", IntegerType(), nullable=True),
                                StructField("SaleType", StringType(), nullable=True),
                                StructField("SaleCondition", StringType(), nullable=True),
                                StructField("SalePrice", IntegerType(), nullable=True),
                                StructField("OverallState", IntegerType(), nullable=True),
                                StructField("Age_of_House", IntegerType(), nullable=True),
                                StructField("Age_of_Repair", IntegerType(), nullable=True),
                                StructField("Age_of_Garage", IntegerType(), nullable=True),
                                StructField("Age_of_House_Sold", IntegerType(), nullable=True)
                        ])
                        # Load the processed data from the CSV file
                        data = spark.read.csv(filepath, header=True,schema=schema)

                        # Assign the column names to the DataFrame
                        data = data.toDF(*columns)

                        # Capturing Lot_Component
                        data = data.withColumn('Lot_Component', (col('LotFrontage') + col('LotArea')) / 2)

                        # Dropping the other Lot Columns and other columns whose variation has been captured.
                        # The columns are dropped since similar columns are already in the dataset 
                        lot_columns_to_drop = ['LotFrontage', 'LotArea', 'LotShape', 'LotConfig','HouseStyle','RoofMatl','Exterior2nd','MasVnrType',"BsmtQual","BsmtCond","BsmtExposure","BsmtFinType1","BsmtFinSF1","BsmtUnfSF","HeatingQC","GarageType","GarageFinish","GarageCars","GarageCond"]
                        data = data.drop(*lot_columns_to_drop)



                        # Save the DataFrame as CSV
                        return   data.toPandas().to_csv(outputpath, index=False)

                except Exception as e:
                        print(e)


    else:
        try:
                        # Suppress warnings
                        os.environ["PYSPARK_LOG_LEVEL"] = "ERROR"
                        warnings.filterwarnings("ignore")

                        # Create a SparkSession
                        spark = SparkSession.builder.appName("Data Analysis").getOrCreate()

                        # Define the column names
                        #columns = ["MSSubClass","MSZoning","LotFrontage","LotArea","Street","LotShape",
                                   #"LandContour","Utilities","LotConfig","LandSlope","Neighborhood",
                                   #"Condition1","BldgType","HouseStyle","RoofStyle","RoofMatl","Exterior1st"
                                  # ,"Exterior2nd","MasVnrType","MasVnrArea","ExterQual","ExterCond",
                                   #"Foundation","BsmtQual","BsmtCond","BsmtExposure","BsmtFinType1",
                                   #"BsmtFinSF1","BsmtUnfSF","TotalBsmtSF","Heating","HeatingQC","CentralAir",
                                   #"Electrical","1stFlrSF","2ndFlrSF","LowQualFinSF","GrLivArea","BsmtFullBath",
                                   #"BsmtHalfBath","FullBath","HalfBath","BedroomAbvGr","KitchenAbvGr","KitchenQual",
                                   #"TotRmsAbvGrd","Functional","Fireplaces","GarageType","GarageFinish","GarageCars",
                                   #"GarageArea","GarageQual","GarageCond","PavedDrive","WoodDeckSF","OpenPorchSF",
                                   #"EnclosedPorch","3SsnPorch","ScreenPorch","PoolArea","MiscVal","MoSold","SaleType",
                                   #"SaleCondition","OverallState","Age_of_House","Age_of_Repair","Age_of_Garage",
                                   #"Age_of_House_Sold"]

                        # Define the schema for the DataFrame
                        #schema = StructType([StructField(name, StringType(), nullable=True) for name in columns])

                        # Load the processed data from the CSV file
                        data = spark.read.csv(filepath, header=True)

                        # Assign the column names to the DataFrame
                        #data = data.toDF(*columns)

                        # Capturing Lot_Component
                        data = data.withColumn('Lot_Component', (col('LotFrontage') + col('LotArea')) / 2)

                        # Dropping the other Lot Columns
                        lot_columns_to_drop = ['LotFrontage', 'LotArea', 'LotShape', 'LotConfig','HouseStyle','RoofMatl','Exterior2nd','MasVnrType',"BsmtQual","BsmtCond","BsmtExposure","BsmtFinType1","BsmtFinSF1","BsmtUnfSF","HeatingQC","GarageType","GarageFinish","GarageCars","GarageCond"]
                        data = data.drop(*lot_columns_to_drop)



                        # Save the DataFrame as CSV
                        return  data.toPandas().to_csv(outputpath, index=False)

        except Exception as e:
                       return e

