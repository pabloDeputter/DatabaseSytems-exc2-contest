import struct

from typing import List


def encode_var_string(s: str):
    return [len(s)] + list(s.encode('ascii'))


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
        return str(byte_array[start_idx + 1: start_idx + 1 + str_len], 'ascii'), start_idx + 1 + str_len
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


if __name__ == '__main__':
    schema = ['int', 'var_str', 'short', 'int', 'int', 'byte', 'var_str', 'var_str', 'var_str', 'var_str']

    record = (1, "Alice", 23, 12345, 987654, 4, "alice@email.com", "1234567890", "ACME", "Elm St")

    encoded = encode_record(record, schema)
    print(len(encoded))
    print(encoded)
    decoded = decode_record(encoded, schema)
    print(decoded)
