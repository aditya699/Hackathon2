# This is POC For Understanding the testing
import numpy as np
import pandas as pd
from data_loader import get_data
from data_preprocessing import preprocess_data
from data_analysis import data_analyzer
data_analyzer('Data/Ingested/train_kafka.csv','Data/Train/model_train.csv',"train")
data_analyzer('Data/Processed/test_cleaned.csv','Data/Train/model_test.csv',"test")
   
#Here the model will served data will be fetched (model_test_csv) and later consumed