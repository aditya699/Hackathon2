# This is POC For Understanding the testing
import numpy as np
import pandas as pd
from data_loader import get_data
from data_preprocessing import preprocess_data
from data_analysis import data_analyzer
from model import predict_output
preprocess_data('Data/Raw/test.csv','Data/Processed/test_cleaned.csv')
data_analyzer('Data/Processed/test_cleaned.csv','Data/Train/model_test.csv',"test")
predict_output('Data/Train/model_train.csv','Data/Train/model_test.csv')
   