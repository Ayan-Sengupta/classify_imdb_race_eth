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

#split data into multiple files (100 files total) each file maintains the headers 
# of the original file
def split_data():
    # read the original file
    with open('data/raw/name.basics.tsv') as file:
        # read the headers
        headers = file.readline()
        # read the rest of the file
        data = file.readlines()
        # calculate the number of lines per file
        lines_per_file = len(data) // 100
        # split the data into 100 files
        for i in range(100):
            # open a new file
            with open(f'data/raw/name.basics_{i}.tsv', 'w') as new_file:
                # write the headers
                new_file.write(headers)
                # write the data
                new_file.writelines(data[i*lines_per_file:(i+1)*lines_per_file])
    print('Data split successfully')
    