from faker import Faker
import random
import pandas as pd

user_columns = ['id', 'name', 'email', 'phone', 'company', 'street', 'street_number', 'zipcode', 'country',
                'birthdate']


# User storage: name:
# id, street_number: 8 bytes (2 x 32 bitnumbers between 0 and 2^31)
# name','email','phone','company','street','zipcode','country: strings of n characters requires n bytes: e.g. +-100 bytes
def generate_data(size):
    users = []
    fake = Faker()
    for i in range(0, size):
        user = [i, fake.name(), fake.ascii_email(), fake.basic_phone_number(), fake.company(), fake.street_name(),
                random.randint(1, 1000), fake.zipcode(), fake.country(),
                f'{random.randint(1970, 2005)}-{random.randint(1, 12)}-{random.randint(1, 28)}']
        users.append(user)
    df = pd.DataFrame(users, columns=user_columns)

    """
    Before: col country is string. range: 4-51. unique: 243
    After: country "id" encoded using dictionary and store as 16 bit integer
    """
    value_to_code_countries = encode_dictionary(df, 'country')

    """
    col birthdate is string. range: 8-10. unique: 7971
      -> Convert to timestamp  with 32bits
    """
    df['birthdate_ts'] = df['birthdate'].apply(lambda s: pd.to_datetime(s, format='%Y-%m-%d'))
    df['birthdate_ts'] = df['birthdate_ts'].astype(int) / 10 ** 9

    df = df[['id', 'name', 'email', 'phone', 'company', 'street', 'street_number', 'zipcode', 'country_dct',
             'birthdate_ts']]
    new_user_columns = list(df.columns.values)
    return df


def encode_dictionary(df, col):
    """
    Creates column df[col + '_dct'] containing dictionary value
    :param df: pandas dataframe
    :param col: column to apply dictionary encoding
    :return mapping between value and code
    """
    unique_values = sorted(df[col].unique())
    value_to_code = {value: i for i, value in enumerate(unique_values)}
    df[f'{col}_dct'] = df[col].apply(lambda x: value_to_code[x])
    return value_to_code


def encode_var_string(s):
    return [len(s)] + list(s.encode('ascii'))


def encode_user_var_length(user):
    '''
    Assuming user has columns
    ['id', 'name', 'email', 'phone', 'company', 'street', 'street_number', 'zipcode', 'country_dct', 'birthdate_ts']

    encode user object:
    id, street_number, zipcode, birthdate_ts, country_dct
      -> to integer between 1 and 4 bytes depending on range values
    name, email, phone, company, street
      -> to variable-length string, e.g. "helloworld" -> (8,"helloworld") instead of using padding, e.g."0000000helloworld"
    '''
    int_list = []
    int_list.extend(int(user[0]).to_bytes(4, 'little'))
    int_list.extend(int(user[6]).to_bytes(2, 'little'))  # max street number < 65536 (or 2^16)
    int_list.extend(int(user[7]).to_bytes(4, 'little'))
    int_list.extend(int(user[9]).to_bytes(4, 'little'))
    int_list.extend(int(user[8]).to_bytes(1, 'little'))  # max country < 256 (or 2^8)
    int_list.extend(encode_var_string(user[1]))
    int_list.extend(encode_var_string(user[2]))
    int_list.extend(encode_var_string(user[3]))
    int_list.extend(encode_var_string(user[4]))
    int_list.extend(encode_var_string(user[5]))
    return bytearray(int_list)


def decode_user_var_length(byte_array):
    '''
    decode variable-length tuple representing user (see encode_user_var_length)
    '''
    get_int = lambda x: int.from_bytes(x, 'little')
    get_string = lambda x: str(x, encoding='ascii')

    id = get_int(byte_array[:4])
    street_number = get_int(byte_array[4:6])
    zipcode = get_int(byte_array[6:10])
    bd = get_int(byte_array[10:14])
    country_dct = get_int(byte_array[14:15])

    # Precompute repeated index calculations
    idx_name_start = 16
    idx_name_len = byte_array[15]
    idx_name_end = idx_name_start + idx_name_len

    name = get_string(byte_array[idx_name_start:idx_name_end])

    idx_email_start = idx_name_end + 1
    idx_email_len = byte_array[idx_name_end]
    idx_email_end = idx_email_start + idx_email_len

    email = get_string(byte_array[idx_email_start:idx_email_end])

    idx_phone_start = idx_email_end + 1
    idx_phone_len = byte_array[idx_email_end]
    idx_phone_end = idx_phone_start + idx_phone_len

    phone = get_string(byte_array[idx_phone_start:idx_phone_end])

    idx_company_start = idx_phone_end + 1
    idx_company_len = byte_array[idx_phone_end]
    idx_company_end = idx_company_start + idx_company_len

    company = get_string(byte_array[idx_company_start:idx_company_end])

    idx_street_start = idx_company_end + 1
    idx_street_len = byte_array[idx_company_end]
    idx_street_end = idx_street_start + idx_street_len

    street = get_string(byte_array[idx_street_start:idx_street_end])

    return (id, name, email, phone, company, street, street_number, zipcode, country_dct, bd)
