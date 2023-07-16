from data_loader import get_data
from pycaret.regression import *
import pandas as pd
import numpy as np

def predict_output(filepath_train:str,filepath_test:str) -> pd.DataFrame :
        data_train=get_data(filepath_train)
        data_test=get_data(filepath_test)
        s = setup(data_train, target = 'SalePrice', session_id = 123)
        best = compare_models()
        predictions = predict_model(best, data=data_test)
        predictions=pd.DataFrame(predictions)
        return predictions.to_csv("Data/Predictions/output.csv",index=False)

