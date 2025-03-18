import gc
import ethnicolr as ec
import dask.dataframe as dd



def load_to_daskDf(file_path):
    return dd.read_csv(file_path,sep='\t',dtype={'birthYear': 'object', 'deathYear': 'object'},blocksize='100MB')

def split_primary_name(df):
    # Split the primaryName into firstName 
    df['firstName'] = df['primaryName'].str.split(' ').str[0]
    # split lastname from primaryNam
    df['lastName'] = df['primaryName'].str.split(' ').str[-1]
    # remmove all the white spaces from last name 
    df['lastName'] = df['lastName'].str.replace(' ', '')
    
    return df

def classify_partition(partition):
    partition = split_primary_name(partition)
    classified = ec.pred_wiki_name(partition, 'lastName', 'firstName')
    gc.collect()
    return classified




