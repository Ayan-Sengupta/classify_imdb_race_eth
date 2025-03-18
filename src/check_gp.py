import tensorflow as tf

def check_gpu():
    print("Num GPUs Available: ", len(tf.config.list_physical_devices('GPU')))

check_gpu()