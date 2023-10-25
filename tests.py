import time
import csv
import os
import random
import pandas as pd
from typing import List
from database import Controller
import utils
from faker import Faker

USER_SCHEMA = ['int', 'var_str', 'var_str', 'var_str', 'var_str', 'var_str', 'int', 'int', 'var_str', 'var_str']


def cast_row_based_on_schema(row, schema):
    return tuple(int(val) if dt == 'int' else val for val, dt in zip(row, schema))


def read_csv_to_list(filepath: str, num_rows: int = None) -> List[List[str]]:
    with open(filepath, 'r') as f:
        return list(csv.reader(f))[:num_rows]


def test_inserts_and_reads(filepath: str, csv_file: str, num_rows: int):
    print("===Test Inserts & Reads===")
    if os.path.exists(filepath):
        os.remove(filepath)

    controller = Controller(filepath)
    csv_data = read_csv_to_list(csv_file, num_rows)

    start_time = time.time()
    for row in csv_data[1:]:
        controller.insert(cast_row_based_on_schema(row, USER_SCHEMA), USER_SCHEMA)
    controller.commit()

    end_time = time.time()
    write_time = end_time - start_time
    writes_per_second = num_rows / write_time

    start_time = time.time()
    for i, original_row in enumerate(csv_data[1:]):
        read_row = utils.decode_record(controller.read(i), USER_SCHEMA)
        assert read_row == cast_row_based_on_schema(original_row, USER_SCHEMA)

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


def test_updates(filepath: str, csv_file: str, num_rows: int):
    print("===Test Updates===")
    controller = Controller(filepath)
    faker = Faker()
    df = pd.read_csv(csv_file)

    for i in range(num_rows // 2):
        action = random.choice(['equal', 'smaller', 'larger'])
        if action == 'smaller':
            df.at[i, 'name'] = df.at[i, 'name'][:-1]
        else:
            df.at[i, 'email'] = faker.name()
            df.at[i, 'email'] = faker.email()
            df.at[i, 'company'] = faker.company()

    start_time = time.time()
    for i in range(num_rows // 2):
        controller.update(int(df.iloc[i]['id']), tuple(df.iloc[i]), USER_SCHEMA)
    controller.commit()
    end_time = time.time()
    update_time = end_time - start_time
    updates_per_second = (num_rows // 2) / update_time

    for i in range(num_rows // 2):
        read_row = utils.decode_record(controller.read(i), USER_SCHEMA)
        assert read_row == tuple(df.iloc[i])

    print(f"Updated {num_rows // 2} records.")
    print(f"Updates per second: {updates_per_second}")
    print(f"Total completion time: {update_time} seconds")


def test_deletes(filepath: str, num_rows: int):
    print("===Test Deletes===")
    controller = Controller(filepath)

    start_time = time.time()
    for i in range(num_rows // 2):
        controller.delete(i)
    controller.commit()
    end_time = time.time()
    delete_time = end_time - start_time
    deletes_per_second = (num_rows // 2) / delete_time

    for i in range(num_rows // 2):
        try:
            controller.read(i)
            assert False, "Record was found!"
        except ValueError:
            pass

    print(f"Deleted {num_rows // 2} records.")
    print(f"Deletes per second: {deletes_per_second}")
    print(f"Total completion time: {delete_time} seconds")


if __name__ == "__main__":
    num_rows = 10000
    filepath = "database.bin"
    csv_file = "fake_users.csv"

    # Create fake users data
    if not os.path.exists(csv_file):
        utils.generate_data(csv_file, num_rows)

    # Insert num_rows of csv into database and check if everything is correct
    test_inserts_and_reads(filepath, csv_file, num_rows)
    # Update the first half of the database and check if everything is correctly updates
    test_updates(filepath, csv_file, num_rows)
    # Deletes the first half of the database
    test_deletes(filepath, num_rows)
