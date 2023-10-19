import time
import pandas as pd

import utils


def save_users_to_binary_var_length(filename, df):
    """
    saves users to sorted variable-length binary file
    file layout: [N offset_t1 offset_t2... offset_tN offset_tN+1 t1 t2 ... tN]

    :param filename: binary file to save
    :param df: pandas dataframe contains all users
    :return:
    """
    start = time.time()
    number_of_slots = df.shape[0]

    with open(filename, "wb") as file:
        file.write(number_of_slots.to_bytes(4, 'little'))

        start_offset = 4 * (number_of_slots + 2)
        offsets = [start_offset]

        file.seek(start_offset)
        for user in df.itertuples(index=False):
            encoded_user = utils.encode_user_var_length(user)
            # print(encoded_user)
            file.write(encoded_user)
            start_offset += len(encoded_user)
            offsets.append(start_offset)

        file.seek(4)
        for offset in offsets:
            file.write(offset.to_bytes(4, 'little'))

    print(f'saved {number_of_slots} records to {filename}. Time: {time.time() - start}s')


def load_users_from_binary_var_length(filename):
    """
    load users from sorted variable-length binary file
    file layout: [N offset_t1 offset_t2... offset_tN offset_tN+1 t1 t2 ... tN]

    :param filename: binary file to save
    :return: pandas dataframe contains all users
    """
    start = time.time()
    # read slots
    with open(filename, "rb") as file:
        number_of_slots = int.from_bytes(file.read(4), "little")
        offsets = [int.from_bytes(file.read(4), "little") for _ in range(number_of_slots + 1)]

        users = []
        for i in range(number_of_slots):
            file.seek(offsets[i])
            user = file.read(offsets[i + 1] - offsets[i])

            user = utils.decode_user_var_length(user)
            users.append(user)

        print(f'loaded {number_of_slots} records from {filename}. Time: {time.time() - start}s')
        return pd.DataFrame(users,
                            columns=['id', 'name', 'email', 'phone', 'company', 'street', 'street_number', 'zipcode',
                                     'country_dct', 'birthdate_ts'])
