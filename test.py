import time
import os
import pandas as pd

import utils
import newDB
import oldDB


# Check if file already exists
print("Generating data...")
if os.path.exists('original.csv'):
    df = pd.read_csv('original.csv')
else:
    df = utils.generate_data(100000)
    df.to_csv('original.csv', index=False)

# OLD METHOD
print("OLD METHOD - Saving to file...")
start_time = time.time()
oldDB.save_users_to_binary_var_length('original.bin', df)
original_duration = time.time() - start_time

original_file_size = os.stat('original.bin').st_size

# NEW METHOD
print("NEW METHOD - Saving to file...")
start_time = time.time()
# Save to file
newDB.save_heapfile_to_binary('optimized.bin', df)
heapfile_duration = time.time() - start_time

heapfile_size = os.stat('optimized.bin').st_size

print(f"Original method: {original_duration:.6f} seconds, {original_file_size} bytes")
print(f"Optimized HeapFile method: {heapfile_duration:.6f} seconds, {heapfile_size} bytes")

print("Reading from file...")
print(newDB.load_records_from_heapfile('optimized.bin'))

read_speedup = original_duration / heapfile_duration
size_reduction = (original_file_size - heapfile_size) / original_file_size * 100

print(f"Read/Write speedup: {read_speedup:.2f}x")
print(f"File size reduction: {size_reduction:.2f}%")
