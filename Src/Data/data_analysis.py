import os
import warnings
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
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
                        print(len(columns))

                       # Define the schema for the DataFrame
                        schema = StructType([StructField(name, StringType(), nullable=True) for name in columns])

                        # Load the processed data from the CSV file
                        data = spark.read.csv(filepath, header=True)

                        # Assign the column names to the DataFrame
                        data = data.toDF(*columns)

                        # Capturing Lot_Component
                        data = data.withColumn('Lot_Component', (col('LotFrontage') + col('LotArea')) / 2)

                        # Dropping the other Lot Columns and other columns whose variation has been captured.
                        # The columns are dropped since similar columns are already in the dataset 
                        lot_columns_to_drop = ['LotFrontage', 'LotArea', 'LotShape', 'LotConfig','HouseStyle','RoofMatl','Exterior2nd','MasVnrType',"BsmtQual","BsmtCond","BsmtExposure","BsmtFinType1","BsmtFinSF1","BsmtUnfSF","HeatingQC","GarageType","GarageFinish","GarageCars","GarageCond"]
                        data = data.drop(*lot_columns_to_drop)

                        #

                        # Printing the column names
                        print(data.columns)

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


                        # Printing the column names
                        print(data.columns)

                        # Save the DataFrame as CSV
                        return  data.toPandas().to_csv(outputpath, index=False)

        except Exception as e:
                       return e

data_analyzer('Data/Ingested/train_kafka.csv','Data/Train/model_train.csv',"train")