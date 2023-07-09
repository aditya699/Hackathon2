from data_loader import get_data
import pandas as pd
import numpy as np
data=get_data("Data/Raw/train.csv")
def preprocess_data(data : pd.DataFrame) :
    '''
    The function will return the preprocessed data

    Input-
    Pandas DataFrame

    Output-
    Pandas DataFrame

    '''
    try:
        #Add Preprocessing Steps Here
        #Stroring the Id Variable in a variable 
        id=data.Id
        #Droping the same
        data=data.drop('Id',axis=1)
        #Droping Columns where % of nulls > 70 %
        data_meta=pd.DataFrame(data.isnull().sum()/len(data)).reset_index().rename(columns={0:"Nulls"})
        data_meta=data_meta[data_meta['Nulls']>=0.70]
        for i in data_meta['index']:
            data.drop(i,inplace=True,axis=1)
        #Droping Condition 2 since already condition 1 is there which tell us about Proximity to various conditions
        data.drop('Condition2',inplace=True,axis=1)
        print(data.shape)
        return data
    except Exception as e:
        return e
preprocess_data(data)