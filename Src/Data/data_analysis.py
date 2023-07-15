from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Data Analysis") \
    .getOrCreate()

# Define the column names
columns = ['MSSubClass','MSZoning','LotFrontage','LotArea','Street','LotShape','LandContour','Utilities','LotConfig','LandSlope','Neighborhood','Condition1','BldgType','HouseStyle','RoofStyle','RoofMatl','Exterior1st','Exterior2nd','MasVnrType',"MasVnrArea","ExterQual","ExterCond","Foundation","BsmtQual","BsmtCond","BsmtExposure","BsmtFinType1","BsmtFinSF1","BsmtUnfSF","TotalBsmtSF","Heating","HeatingQC","CentralAir","Electrical","1stFlrSF","2ndFlrSF","LowQualFinSF","GrLivArea","BsmtFullBath","BsmtHalfBath","FullBath","HalfBath","BedroomAbvGr","KitchenAbvGr","KitchenQual","TotRmsAbvGrd","Functional","Fireplaces","GarageType","GarageFinish","GarageCars","GarageArea","GarageQual","GarageCond","PavedDrive","WoodDeckSF","OpenPorchSF","EnclosedPorch","3SsnPorch","ScreenPorch","PoolArea","MiscVal","MoSold","SaleType","SaleCondition","SalePrice","OverallState","Age_of_House","Age_of_Repair","Age_of_Garage","Age_of_House_Sold"]

# Define the schema for the DataFrame
schema = StructType([StructField(name, StringType(), nullable=True) for name in columns])

# Load the processed data from the CSV file
data = spark.read.csv("data/Ingested/train_kafka.csv", header=False, schema=schema)

# Assign the column names to the DataFrame
data = data.toDF(*columns)

summary_stats = data.describe().toPandas()
print(summary_stats)
