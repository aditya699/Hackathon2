from data_loader import get_data
import pandas as pd
import numpy as np
from datetime import datetime
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
        #Utilities modelling

        utilities_mapping={
            "AllPub":4,
            "NoSewr":3,
            "NoSeWa":2,
            "ELO":1
        }
        data['Utilities'].replace(utilities_mapping,inplace=True)
        #LandSlope Modelling
        landslope_modelling ={
            "Gtl":0,
            "Mod":1,
             "Sev":2
        }
        data['LandSlope'].replace(landslope_modelling,inplace=True)
        #OverallQual Modelling
        rating_overall_qual = {
            'Very Excellent': 10,
            'Excellent': 9,
            'Very Good': 8,
            'Good': 7,
            'Above Average': 6,
            'Average': 5,
            'Below Average': 4,
            'Fair': 3,
            'Poor': 2,
            'Very Poor': 1
        }


        data['OverallQual'].replace(rating_overall_qual,inplace=True)
        #OverallCond Modelling (Will use the same Previous Dictionary)
        data['OverallCond'].replace(rating_overall_qual,inplace=True)
        #Creating a new column as overall state
        data['OverallState']=(data['OverallQual']+data['OverallCond'])//2
        #Droping OverallQual and QverallCond
        data.drop(['OverallQual','OverallCond'],inplace=True,axis=1)
        #Computing age of house  using YearBuilt: Original construction date
        current_year = datetime.now().year
        data['Age_of_House'] = current_year - data['YearBuilt']
        #Droping Year Bulit Column
        data.drop('YearBuilt',inplace=True,axis=1)
        #The distinct value in Utilities are --Remove in Prod
        print("The distinct value in Utilities are ",data['Utilities'].unique())
        #The distinct value in land slope are --Remove in Prod
        print("The distinct value in Land Slope are ",data['LandSlope'].unique())
        #The distinct value in OverallState --Remove in Prod
        print("The distinct value in OverallState are ",data['OverallState'].unique())
        #The distinct value in age of House
        print("The distinct value in age of House is ",data['Age_of_House'].unique())
        #Check if any values are there --Remove it in Prod
        data_meta=pd.DataFrame(data.isnull().sum()/len(data)).reset_index().rename(columns={0:"Nulls"})
        data_meta=data_meta[data_meta['Nulls']>0]
        if len(data_meta)==0: print("No Nulls found in data")
        #Check what is the shape of the data --Remove it in Prod
        print("The shape of data is ",data.shape)
        return data
    except Exception as e:
        return e
    
print(preprocess_data(data))
