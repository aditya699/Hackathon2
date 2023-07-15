from data_loader import get_data
import pandas as pd
import numpy as np
from datetime import datetime
data = get_data("Data/Raw/train.csv")

def preprocess_data(filepath:str,filedumppath:str)-> bool :
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
        data = get_data(filepath)
        id = data.Id
        # Droping the same
        data = data.drop('Id', axis=1)
        # Droping Columns where % of nulls > 40 %
        #Creating a list of columns to drop for test_data
        list_of_columns_to_drop =[]
        data_meta = pd.DataFrame(
            data.isnull().sum()/len(data)).reset_index().rename(columns={0: "Nulls"})
        data_meta = data_meta[data_meta['Nulls'] >= 0.40]
        for i in data_meta['index']:
            list_of_columns_to_drop.append (i)
        data.drop(list_of_columns_to_drop, inplace=True, axis=1)
 
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

        #Computing the years since the house was last modified
        data['Age_of_Repair']=current_year - data['YearRemodAdd']

        
        #Droping the YearRemodAdd
        data.drop('YearRemodAdd',inplace=True,axis=1)

        
        #ExterQual Mapping
        extra_qual_mapping={
            "Ex":5,
            "Gd":4,
            "TA":3,
            "Fa":2,
            "Po":1
        }

        #Mapping to ExterQual Mapping
        data['ExterQual'].replace(extra_qual_mapping,inplace=True)

        
        #Appling the same mapping to ExterCond
        data['ExterCond'].replace(extra_qual_mapping,inplace=True)

        
        #Mapping Height of Basement
        base_height_map={
            "Ex":5,
            "Gd":4,
            "TA":3,
            "Fa":2,
            "Po":1,
            "NA":0
        }
        #Applying Mappaing to BsmtQual
        data['BsmtQual'].replace(base_height_map,inplace=True)

        #Applying Same Mapping to BsmtCond
        data['BsmtCond'].replace(base_height_map,inplace=True)

        #Mapping for BsmtExposure
        bstm_mapping_exposure={
            'Gd':5,
            'Av':4,
            'Mn':3,
            'No':2,
            'NA':1
        }
        #Applying the same Mapping to BsmtExposure
        data['BsmtExposure'].replace(bstm_mapping_exposure,inplace=True)

        #Mapping for BsmtFinType1
        bstm_mapping_exposure_fin={
            "GLQ":7,
            "ALQ":6,
            "BLQ":5,
            "Rec":4,
            "LwQ":3,
            "Unf":2,
            "NA":1
        }
        #Applying the same Mapping to BsmtFinType1
        data['BsmtFinType1'].replace(bstm_mapping_exposure_fin,inplace=True)
        
        
        #Droping BsmtFinType2 since already a 2 features related to same are there
        data.drop(['BsmtFinType2'],axis=1,inplace=True)

        #Droping 2nd Basement based_columns
        
        data.drop(['BsmtFinSF2'],axis=1,inplace=True)
      
        
        #Mapping HeatingQC
        HeatingQC_mapping={
            "Ex":5,
            "Gd":4,
            "TA":3,
            "Fa":2,
            "Po":1
        }
        #Applying the mapping HeatingQC
        data['HeatingQC'].replace(HeatingQC_mapping,inplace=True)
 
        
        #Mapping for CentralAir
        mapping_ac={
            "Y":1,
            "N":0
        }
        #Applying the mapping
        data['CentralAir'].replace(mapping_ac,inplace=True)

        
        #KitchenQual mapping
        kitchen_qual_mapping={
            "Ex":5,
            "Gd":4,
            "TA":3,
            "Fa":2,
            "Po":1
        }
        #Kicthen Quality Mapping
        data['KitchenQual'].replace(kitchen_qual_mapping,inplace=True)
     
        #Functional Mapping
        functional_mapping={
            "Typ":8,
            "Min1":7,
            "Min2":6,
            "Mod":5,
            "Maj1":4,
            "Maj2":3,
            "Sev":2,
            "Sal":1
        }
        #Applying the functional mapping
        data['Functional'].replace(functional_mapping,inplace=True)

        
        #Age of garbage
        data['Age_of_Garage']=current_year-data['GarageYrBlt']

        
        #Droping GarageYrBlt
        data.drop('GarageYrBlt',axis=1,inplace=True)

        
        #Mapping for GarageFinish
        map_for_garage_finish={
            'Fin':4,
            'RFn':3,
            'Unf':2,
            'NA':1
        }
        #Applying mapping in the same GarageFinish
        data['GarageFinish'].replace(map_for_garage_finish,inplace=True)
       
        
        #Applying mapping in Garage quality
        data['GarageQual'].replace(base_height_map,inplace=True)
     
        
        #Applying mapping in GarageCond
        data['GarageCond'].replace(base_height_map,inplace=True)
       
        #Computing an extra column age sold
        data['Age_of_House_Sold']=current_year-data['YrSold']

        #Droping the Yrsold columns
        data.drop('YrSold',axis=1,inplace=True)

        data.to_csv(filedumppath,index=False)
        return True
    except Exception as e:
        return e
    