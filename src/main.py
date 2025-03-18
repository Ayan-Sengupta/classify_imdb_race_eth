import logging
import os 
import gc
from classify import load_to_daskDf, classify_partition
from dask.distributed import Client,LocalCluster
from download_dataSet import download_data



logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def process_and_write_chunk(df_chunk, output_file):
    logger.info("Classifying data")
    classified_df = classify_partition(df_chunk)

    logger.info("Appending results to file")
    if not os.path.exists(output_file):
        classified_df.to_csv(output_file, index=False)
    else:
        classified_df.to_csv(output_file, mode='a', header=False, index=False)

if __name__ == '__main__':

    download_data('https://datasets.imdbws.com/name.basics.tsv.gz')
    cluster = LocalCluster(n_workers=1,threads_per_worker=1,processes=True,memory_limit='4GB')
    with Client(cluster) as client:
       
        try: 
            logger.info("Loading data")
            df = load_to_daskDf('data/raw/name.basics.tsv')

            output_file = 'data/results/classified_data.csv'
            os.makedirs(os.path.dirname(output_file), exist_ok=True)

            logger.info("Classifying data firt 500000 rows")
            first_chunk = df.head(500000)
            process_and_write_chunk(first_chunk, output_file)

           
            gc.collect()
           
            
            
         
            
          
        except Exception as e:
            logger.error(f"An error occurred: {e}")
        finally:
            logger.info("Cleaning up...")
           