import os 
import requests

def download_data(url):
    # use requests to download data
    response = requests.get(url)
    # save data to file
    with open('data/raw/name.basics.tsv.gz', 'wb') as file:
        file.write(response.content)
    # uncompress the .gz file and save it
    os.system('gunzip data/raw/name.basics.tsv.gz')
    # check if the file was saved
    if os.path.exists('data/raw/name.basics.tsv'):
        print('Data downloaded successfully')
    else:
        print('Error downloading data')

