import logging
import os 
import time
from classify import load_to_pd, classify_partition
from download_split import download_data, split_data
from concurrent.futures import ProcessPoolExecutor



logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def process_file(i):
    
    try:
        logger.info(f"Processing file: {i}")
        df = load_to_pd(f'data/raw/name.basics_{i}.tsv')

  
        df = classify_partition(df)
       

        df.to_csv(f'data/results/name.basics_{i}.csv', index=False)
        logger.info(f"File {i} saved successfully...")
    except Exception as e:
        logger.error(f"Error processing file {i}: {e}")

if __name__ == '__main__':

    download_data('https://datasets.imdbws.com/name.basics.tsv.gz')
    logger.info("Data downloaded successfully...")

    #split data into multiple files (100 files total)
    split_data()
    logger.info("Data split successfully...")

    #if data split is succesful delete the original file
    if os.path.exists('data/raw/name.basics.tsv'):
        os.remove('data/raw/name.basics.tsv')
       
    
    num_workers = 2
    logger.info(f"Starting processing with: {num_workers} workers")

    # track how long overall processing takes
    start = time.time()
    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        executor.map(process_file, range(100))
    end = time.time()
    
    logger.info(f"Processing completed in {end-start} seconds")
    


   
    
        

