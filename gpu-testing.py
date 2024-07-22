!pip install tensorflow[and-cuda]
!pip install keras
!pip install -U "jax[cuda12]"

import os
os.environ["KERAS_BACKEND"] = "tensorflow"
import keras

print(keras.__version__)

from tensorflow.python.client import device_lib
print(device_lib.list_local_devices())

import tensorflow as tf
print("Num GPUs Available: ", len(tf.config.list_physical_devices('GPU')))
