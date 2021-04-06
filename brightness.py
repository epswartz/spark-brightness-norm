import pyspark
import numpy as np
from PIL import Image, ImageEnhance
from io import BytesIO

import sys

input_dir = sys.argv[1]
output_dir = sys.argv[2]
target_mean = int(sys.argv[3])

def normalize_brightness(rawdata):
    im = Image.open(BytesIO(rawdata))
    img_mean = np.asarray(im).mean()
    br = ImageEnhance.Brightness(im)
    im = br.enhance(target_mean / img_mean)
    return list(np.asarray(im).flatten())

if __name__ == "__main__":
    print("-" * 100)
    print("Beginning Brightness Normalization.")
    print("-" * 100)
    sc = pyspark.SparkContext()
    images = sc.binaryFiles(input_dir)
    images = images.values().map(normalize_brightness)
    images.saveAsTextFile(output_dir)

    print("-" * 100)
    print("Brightness Normalization Complete.")
    print("-" * 100)