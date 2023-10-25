import time
import csv
import os
from typing import List
from database import Controller
import utils


def cast_row_based_on_schema(row, schema):
    casted_row = []
    for value, data_type in zip(row, schema):
        if data_type == 'int':
            casted_row.append(int(value))
        else:
            casted_row.append(value)
    return tuple(casted_row)


def read_csv_to_list(filepath: str, num_rows: int = None) -> List[List[str]]:
    with open(filepath, 'r') as f:
        reader = csv.reader(f)
        data = list(reader)
        return data[:num_rows]


def test_controller(filepath: str, csv_file: str, num_rows: int):
    controller = Controller(filepath)
    csv_data = read_csv_to_list(csv_file, num_rows)

    # Insertion and Timing
    start_time = time.time()
    for row in csv_data[1:]:
        casted_row = cast_row_based_on_schema(row, user_schema)
        controller.insert(casted_row, user_schema)

    controller.commit()
    end_time = time.time()

    write_time = end_time - start_time
    writes_per_second = num_rows / write_time

    # Reading and Timing
    start_time = time.time()
    for i, original_row in enumerate(csv_data[1:]):
        casted_original_row = cast_row_based_on_schema(original_row, user_schema)
        read_row = controller.read(i)
        read_row = utils.decode_record(read_row, user_schema)
        assert read_row == casted_original_row, f"Mismatch: Original: {casted_original_row}, Read: {read_row}"

    controller.commit()
    end_time = time.time()

    read_time = end_time - start_time
    reads_per_second = num_rows / read_time

    total_time = write_time + read_time

    print(f"Inserted {num_rows} records.")
    print(f"Database Size: {os.path.getsize(filepath)}")
    print(f"Writes per second: {writes_per_second}")
    print(f"Reads per second: {reads_per_second}")
    print(f"Total completion time: {total_time} seconds")


if __name__ == "__main__":
    user_schema = ['int', 'var_str', 'var_str', 'var_str', 'var_str', 'var_str', 'int', 'int', 'var_str', 'var_str']
    num_rows = 100
    filepath = "database.bin"
    csv_file = "fake_users.csv"

    test_controller(filepath, csv_file, num_rows)
