import struct
from faker import Faker
import pandas as pd
import random
import csv

from typing import List


def encode_var_string(s: str):
    return [len(s)] + list(s.encode('UTF-8'))


def encode_field(value, field_type: str):
    if field_type == 'var_str':
        return encode_var_string(value)
    elif field_type == 'int':
        # 4-byte int
        return list(struct.pack("<I", value))
    elif field_type == 'short':
        # 2-byte int
        return list(struct.pack("<H", value))
    elif field_type == 'byte':
        # 1-byte int
        return [value]
    else:
        raise ValueError(f"Unknown field_type {field_type}")


def decode_field(byte_array, start_idx, field_type):
    if field_type == 'var_str':
        str_len = byte_array[start_idx]
        return str(byte_array[start_idx + 1: start_idx + 1 + str_len], 'utf-8'), start_idx + 1 + str_len
    elif field_type == 'int':
        return struct.unpack("<I", byte_array[start_idx:start_idx + 4])[0], start_idx + 4
    elif field_type == 'short':
        return struct.unpack("<H", byte_array[start_idx:start_idx + 2])[0], start_idx + 2
    elif field_type == 'byte':
        return byte_array[start_idx], start_idx + 1
    else:
        raise ValueError(f"Unknown field_type {field_type}")


def encode_record(record, schema: List[str]):
    encoded_fields = []
    for value, field_type in zip(record, schema):
        encoded_fields.extend(encode_field(value, field_type))
    return bytearray(encoded_fields)


def decode_record(byte_array, schema: List[str]):
    decoded_fields = []
    start_idx = 0
    for field_type in schema:
        value, start_idx = decode_field(byte_array, start_idx, field_type)
        decoded_fields.append(value)
    return tuple(decoded_fields)


def generate_data(file_path: str, rows: int):
    user_columns = ['id', 'name', 'email', 'phone', 'company', 'street', 'street_number', 'zipcode', 'country',
                    'birthdate']
    users = []
    fake = Faker()
    for i in range(rows):
        user = [i, fake.name(), fake.ascii_email(), fake.basic_phone_number(), fake.company(), fake.street_name(),
                random.randint(1, 1000), fake.zipcode(), fake.country(),
                f'{random.randint(1970, 2005)}-{random.randint(1, 12)}-{random.randint(1, 28)}']
        users.append(user)
    df = pd.DataFrame(users, columns=user_columns)
    df.to_csv(file_path, index=False)


if __name__ == '__main__':
    # schema = ['int', 'var_str', 'short', 'int', 'int', 'byte', 'var_str', 'var_str', 'var_str', 'var_str']
    #
    # record = (1, "Alice", 23, 12345, 987654, 4, "alice@email.com", "1234567890", "ACME", "Elm St")
    #
    # encoded = encode_record(record, schema)
    # print(len(encoded))
    # print(encoded)
    # decoded = decode_record(encoded, schema)
    # print(decoded)
    generate_data('fake_users.csv', 100000)
