import time
from typing import List

import utils
from database import HeapFile
from external_merge_sort import two_way_external_merge_sort


class Controller:
    def __init__(self, filepath):
        self.heap_file = HeapFile(filepath)

    def insert(self, data, schema: List[str]):
        self.heap_file.insert_record(utils.encode_record(data, schema))

    def update(self, id_: int, data, schema: List[str]):
        self.heap_file.update_record(utils.encode_record([id_], ['int']), utils.encode_record(data, schema))

    def read(self, id_: int):
        byte_id = utils.encode_record([id_], ['int'])
        return self.heap_file.read_record(byte_id)

    def delete(self, id_: int):
        (page, slot_id) = self.heap_file.find_record(utils.encode_record([id_], ['int']))
        if page is None:
            print('Record not found!')
            return
        page.delete_record(slot_id)

    def commit(self):
        self.heap_file.close()

    def sort(self):
        pages = []
        i = 1
        while page := self.heap_file.find_page(i):
            pages.append(page)
            i += 1
        two_way_external_merge_sort(pages, 0)


if __name__ == '__main__':
    # # create file database.bin
    # with open('database.bin', 'r+b') as file:
    #     file.seek(1024)
    #     data = file.read(PAGE_SIZE)
    #
    #     file.close()
    start = time.time()
    orm = Controller('database.bin')
    orm.sort()

    # if os.path.exists('users.csv'):
    #     df = pd.read_csv('users.csv')
    # else:
    #     df = utils.generate_data(1)
    #     df.to_csv('users.csv', index=False)
    #

    # schema = ['int', 'var_str', 'int', 'int', 'int', 'byte', 'var_str', 'var_str', 'var_str', 'var_str']
    # record = (1, "Alice", 23, 12345, 987654, 4, "alice@email.com", "1234567890", "ACME", "Elm St")
    # smallrecord = (60, "A", 2, 1, 987654, 4, "", "", "", "")
    # record = (47, 'Brian Green', 'michaelfarrell@yahoo.com', '9306399309', 'Cruz LLC', 'Berry Cove', 707, 76486, 'Guam',
    #           '1981-1-9')
    # schema = ['int', 'var_str', 'var_str', 'var_str', 'var_str', 'var_str', 'int', 'int', 'var_str', 'var_str']
    # orm.insert(record, schema)
    # for i in range(8):
    #     record = (i,) + record[1:]
    #     orm.insert(record, schema)
    # orm.insert(smallrecord, schema)

    # orm.insert(record, schema)

    # orm.update(2, (2, "AAAAAAAAAAAAAAAA", 23, 12345, 987654, 4, "a", "1", "ACME", "Elm St"), schema)
    # orm.delete(5)
    # orm.heap_file.page_directories[0].pages[1].dump()
    # orm.update(9990, (9990, 'Amanda Robin', 'gonzalezamber@hotmail.com', '(696)381-0879', 'Ramirez LLC', 'Katie Ford', 701, 70144, 'Bhutan', '1992-2-4'), schema)
    # orm.delete(0)
    # orm.delete(9990)

    # print(utils.decode_record(orm.read(9990), schema))

    # for i in range(20, 23):
    #     # orm.insert(i.to_bytes(2, 'little'))
    #     orm.insert(bytearray(i.to_bytes(2, byteorder='little')))
    #     orm.commit()

    # orm.heap_file.page_directories[0].pages[1].dump()
    # print(f"pages: {orm.heap_file.page_directories[0]}")
    # print(f"free space first pd: {orm.heap_file.page_directories[0].free_space()}")
    # print(list(orm.heap_file.page_directories[0].pages.values())[-1].free_space())
    # orm.commit()
    # print(f"time taken: {time.time() - start}")
    # print(os.path.getsize('database.bin'))