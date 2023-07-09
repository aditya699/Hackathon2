from data_loader import get_data
import pandas as pd
import numpy as np
data=get_data("Data/Raw/train.csv")
def preprocess_data(data : pd.DataFrame) ->pd.DataFrame :
    '''
    The function will return the preprocessed data

    Input-
    Pandas DataFrame

    Output-
    Pandas DataFrame

    '''
    # Add Preprocessing Steps Here

    return data