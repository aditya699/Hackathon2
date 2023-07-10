from data_loader import get_data
import pandas as pd
import numpy as np
data = get_data("Data/Raw/train.csv")


def preprocess_data(data: pd.DataFrame)->pd.DataFrame:
    '''
    The function will return the preprocessed data

    Input-
    Pandas data

    Output-
    Pandas data

    '''
    try:
        # Add Preprocessing Steps Here
        # Stroring the Id Variable in a variable
        id = data.Id
        # Droping the same
        data = data.drop('Id', axis=1)
        # Droping Columns where % of nulls > 40 %
        data_meta = pd.DataFrame(
            data.isnull().sum()/len(data)).reset_index().rename(columns={0: "Nulls"})
        data_meta = data_meta[data_meta['Nulls'] >= 0.40]
        for i in data_meta['index']:
            data.drop(i, inplace=True, axis=1)
        # Droping Condition 2 since already condition 1 is there which tell us about Proximity to various conditions
        data.drop('Condition2', inplace=True, axis=1)
        # Filling Missing Value's with mean for numerical and mode for categorical
        for i in data.columns:
            if data[i].dtype == "object" and data[i].isnull().sum() > 0:
                mode = data[i].mode()[0]
                data[i].fillna(mode, inplace=True)
            if data[i].dtype == "float64" and data[i].isnull().sum() > 0:
                mean = data[i].mean()
                data[i].fillna(mean, inplace=True)
        #Check if any values are there --Remove it in Prod
        data_meta=pd.DataFrame(data.isnull().sum()/len(data)).reset_index().rename(columns={0:"Nulls"})
        data_meta=data_meta[data_meta['Nulls']>0]
        if len(data_meta)==0: print("No Nulls found in data")
        #Check what is the shape of the data --Remove it in Prod
        print("The shape of data is ",data.shape)
        return data
    except Exception as e:
        return e
    
preprocess_data(data)
