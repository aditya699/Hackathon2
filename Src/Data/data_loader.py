import numpy as np
import pandas as pd

def get_data(filepath : str) -> pd.DataFrame :
    '''
    This function will return data read from the desired file path.

    Parameters:
    Filepath(str)

    Returns
    DataFrame(pd.DataFrame)
    '''
    try:
        data=pd.read_csv(filepath)
        return data
    except Exception as e:
        return e
    