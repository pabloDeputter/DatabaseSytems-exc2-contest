import pandas as pd

import utils


class SlottedPage:
    def __init__(self, page_size=4096):
        self.page_size = page_size
        self.data = bytearray()
        self.offset_table = []
        self.free_space = page_size

    def add_record(self, record):
        record_size = len(record)
        if self.free_space < record_size:
            return False

        self.offset_table.append(len(self.data))
        self.data += record
        self.free_space -= record_size
        return True


class HeapFile:
    def __init__(self):
        self.pages = [SlottedPage()]

    def add_record(self, record):
        for page in self.pages:
            if page.add_record(record):
                return
        new_page = SlottedPage()
        new_page.add_record(record)
        self.pages.append(new_page)


def save_heapfile_to_binary(filename, df):
    # Encoding the DataFrame records into a list first
    encoded_users = [utils.encode_user_var_length(user) for _, user in df.iterrows()]

    # Create heapfile
    heapfile = HeapFile()
    for encoded_user in encoded_users:
        heapfile.add_record(encoded_user)


    with open(filename, "wb") as f:
        f.write(len(heapfile.pages).to_bytes(4, 'little'))
        for page in heapfile.pages:
            f.write(page.free_space.to_bytes(4, 'little'))
            f.write(len(page.data).to_bytes(4, 'little'))  # Write the length of data
            f.write(len(page.offset_table).to_bytes(4, 'little'))  # Write the length of offset table
            f.write(page.data)  # Write actual data
            for offset in page.offset_table:
                f.write(offset.to_bytes(4, 'little'))  # Write each offset


def load_heapfile_from_binary(filename):
    heapfile = HeapFile()
    heapfile.pages.clear()
    with open(filename, "rb") as f:
        num_pages = int.from_bytes(f.read(4), 'little')
        for _ in range(num_pages):
            page = SlottedPage()
            page.free_space = int.from_bytes(f.read(4), 'little')
            data_len = int.from_bytes(f.read(4), 'little')
            offset_table_len = int.from_bytes(f.read(4), 'little')
            page.data = f.read(data_len)
            for _ in range(offset_table_len):
                offset = int.from_bytes(f.read(4), 'little')
                page.offset_table.append(offset)
            heapfile.pages.append(page)
    return heapfile


def load_records_from_heapfile(filename) -> pd.DataFrame:
    heapfile = load_heapfile_from_binary(filename)
    records = []
    for page in heapfile.pages:
        for i in range(len(page.offset_table)):
            start_offset = page.offset_table[i]
            end_offset = page.offset_table[i + 1] if i + 1 < len(page.offset_table) else len(page.data)
            record = utils.decode_user_var_length(page.data[start_offset:end_offset])
            records.append(record)
    return pd.DataFrame(records,
                        columns=['id', 'name', 'email', 'phone', 'company', 'street', 'street_number', 'zipcode',
                                 'country_dct', 'birthdate_ts'])
