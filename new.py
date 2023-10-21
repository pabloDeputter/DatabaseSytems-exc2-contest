import os
from typing import Optional, List, Tuple
import pandas as pd

import utils

# Page Constants
PAGE_SIZE = 512  # Database Page is normally between 512B and 16KB
OFFSET_SIZE = 4
LENGTH_SIZE = 4
SLOT_ENTRY_SIZE = OFFSET_SIZE + LENGTH_SIZE

FREE_SPACE_POINTER_SIZE = 4
NUMBER_SLOTS_SIZE = 4
FOOTER_SIZE = FREE_SPACE_POINTER_SIZE + NUMBER_SLOTS_SIZE

# PageDirectory Constants
PAGE_NUM_SIZE = 3
FREE_SPACE_SIZE = 3


class PageFooter:
    def __init__(self, data: bytearray):
        # Pointer to free space
        self.free_space_pointer = int.from_bytes(data[-FREE_SPACE_POINTER_SIZE:], 'little')
        # Number of slots
        slot_count = int.from_bytes(data[-FREE_SPACE_SIZE:-FREE_SPACE_POINTER_SIZE], 'little')

        # Contains pairs (offset to beginning of record, length of record), if length == 0, then record is deleted
        self.slot_dir = []
        for i in range(slot_count):
            slot = data[-FOOTER_SIZE - (i + 1) * SLOT_ENTRY_SIZE:-FOOTER_SIZE - i * SLOT_ENTRY_SIZE]
            offset, length = slot[:OFFSET_SIZE], slot[LENGTH_SIZE:]
            self.slot_dir.append((int.from_bytes(offset, 'little'), int.from_bytes(length, 'little')))

    def slot_count(self):
        return len(self.slot_dir)

    def data(self) -> bytearray:
        return bytearray(
            len(self.slot_dir).to_bytes(NUMBER_SLOTS_SIZE, byteorder='little') + self.free_space_pointer.to_bytes(
                FREE_SPACE_POINTER_SIZE, byteorder='little'))


class Page:
    def __init__(self, data: bytearray = bytearray(PAGE_SIZE)):
        self.page_footer = PageFooter(data)
        # Use padding to achieve fixed size
        self.data = data
        page_footer_data = self.page_footer.data()
        self.data[-len(page_footer_data):] = page_footer_data

    def update_header(self):
        page_footer_data = self.page_footer.data()
        self.data[-len(page_footer_data):] = page_footer_data

    def free_space(self):
        # 512 100 - (x * 8) - 4 = 508
        # Page header grows from bottom up, records grow top down.
        # Free space pointer - space occupied by page header - 4 bytes for free space pointer
        return PAGE_SIZE - self.page_footer.free_space_pointer - (
                len(self.page_footer.slot_dir) * SLOT_ENTRY_SIZE) - FREE_SPACE_POINTER_SIZE

    @staticmethod
    def calculate_slot_offset(slot_id):
        """
        Calculate the offset of a slot in bytes, this is the location it starts, so write to right to left.

        :param slot_id: Slot id
        :return: Offset in bytes
        """
        return (PAGE_SIZE - FREE_SPACE_POINTER_SIZE * 2) - (SLOT_ENTRY_SIZE * (slot_id + 1))

    def insert_record(self, record: bytearray):
        """
        If there is not enough free space -> try to compact data, and use this free space, otherwise record can't be stored
        First check if there is a slot with 0 as length, to overwrite this
        :param record:
        :return:
        """
        needed_space = len(record) + SLOT_ENTRY_SIZE
        if needed_space > self.free_space():
            return False

        # Write data
        self.data[self.page_footer.free_space_pointer:self.page_footer.free_space_pointer + len(record)] = record

        # Check if page is packed, meaning no deleted records
        if self.is_packed():
            index = self.page_footer.slot_count()
        else:
            index = 0
            for i, (_, length) in enumerate(self.page_footer.slot_dir):
                if length == 0:
                    index = i

        # Update slots
        new_slot_offset = Page.calculate_slot_offset(index)

        # (offset, length)
        self.data[new_slot_offset: new_slot_offset + OFFSET_SIZE] = self.page_footer.free_space_pointer.to_bytes(
            OFFSET_SIZE, 'little')
        self.data[new_slot_offset + OFFSET_SIZE: new_slot_offset + SLOT_ENTRY_SIZE] = len(record).to_bytes(LENGTH_SIZE,
                                                                                                           'little')

        # Update page footer
        if self.is_packed():
            self.page_footer.slot_dir.append((self.page_footer.free_space_pointer, len(record)))
        else:
            self.page_footer.slot_dir[index] = (self.page_footer.free_space_pointer, len(record))

        # Update free space pointer
        self.page_footer.free_space_pointer += len(record)
        self.update_header()

        return True

    def delete_record(self, slot_id):
        offset, length = self.page_footer.slot_dir[slot_id]
        self.page_footer.slot_dir[slot_id] = (offset, 0)
        new_slot_offset = Page.calculate_slot_offset(slot_id)
        number = 0
        self.data[new_slot_offset + OFFSET_SIZE:new_slot_offset + SLOT_ENTRY_SIZE] = number.to_bytes(LENGTH_SIZE,
                                                                                                     'little')
        # Fix fragmentation
        self.compact_page()

    def read_record(self, slot_id):
        offset, length = self.page_footer.slot_dir[slot_id]
        return self.data[offset: offset + length]

    def update_record(self, slot_id, new_record):
        offset, length = self.page_footer.slot_dir[slot_id]
        # If new record size is equal, just overwrite
        if len(new_record) == length:
            self.data[offset:offset + length] = new_record
            return True
        # If new record is smaller, we need to compact the page to avoid fragmentation
        elif len(new_record) < length:
            self.data[offset:offset + len(new_record)] = new_record
            new_slot_offset = Page.calculate_slot_offset(slot_id)
            self.page_footer.slot_dir[slot_id] = (offset, len(new_record))
            self.data[new_slot_offset + OFFSET_SIZE:new_slot_offset + SLOT_ENTRY_SIZE] = len(new_record).to_bytes(
                LENGTH_SIZE, 'little')
            self.compact_page()
            return True
        # New record is lager, we can just insert the record
        else:
            # Delete record, length will be set to -1
            self.delete_record(slot_id)
            # If returns True, enough free space on the page and slot_id stays the same, else we need to find a new page
            return self.insert_record(new_record)

    def find_record(self, byte_id: bytearray) -> int:
        for slot_id, (offset, length) in enumerate(self.page_footer.slot_dir):
            # some record, we assume the first field is the id and an int
            record = self.data[offset: offset + length]
            if byte_id == record[:4]:
                return slot_id

    def is_full(self):
        return self.free_space() <= 0

    def is_packed(self):
        """
        Check if page is packed, meaning no deleted records.
        """
        return all(length != 0 for offset, length in self.page_footer.slot_dir)

    def compact_page(self):
        """
        Reclaim unused space so that records are contiguous and limit fragmentation.

        Eager -> compact page when a record is deleted (we do this)
        Lazy -> compact page when page is full
        """
        write_ptr = 0

        for i, (offset, length) in enumerate(self.page_footer.slot_dir):
            # Skip deleted records
            if length != 0:
                if offset != write_ptr:
                    self.data[write_ptr:write_ptr + length] = self.data[offset:offset + length]
                self.page_footer.slot_dir[i] = (write_ptr, length)
                # Update slots in bytes
                new_slot_offset = Page.calculate_slot_offset(i)
                self.data[new_slot_offset: new_slot_offset + OFFSET_SIZE] = write_ptr.to_bytes(OFFSET_SIZE, 'little')
                self.data[new_slot_offset + OFFSET_SIZE: new_slot_offset + SLOT_ENTRY_SIZE] = length.to_bytes(
                    SLOT_ENTRY_SIZE, 'little')
                write_ptr += length

        self.page_footer.free_space_pointer = write_ptr
        self.update_header()

    def dump(self):
        print("=== Data Byte Dump ===")

        for i, byte in enumerate(self.data):
            if i % 16 == 0:
                print()
            print(f"{byte:02x} ", end='')
        print("\n")

        print("=== Footer Dump ===")
        print(f"Free Space Pointer: {self.page_footer.free_space_pointer}")
        print(
            f"Free Space Pointer (bytes): {int.from_bytes(self.data[-FREE_SPACE_POINTER_SIZE:], 'little')}")

        print(f"Number of slots: {self.page_footer.slot_count()}")
        print(
            f"Number of slots (bytes): {int.from_bytes(self.data[PAGE_SIZE - FREE_SPACE_POINTER_SIZE * 2:PAGE_SIZE - FREE_SPACE_POINTER_SIZE], 'little')}")
        print("\n")

        print("=== Record Dump ===")
        print("Slot: offset | length")
        print("==========")
        for i, (offset, length) in enumerate(self.page_footer.slot_dir):
            if offset == -1:
                print(f"Record {i}: Deleted")
                continue
            record_bytes = self.data[offset:offset + length]
            print(f"Slot {i}: {offset} | {length}")

            slot_offset = -(FREE_SPACE_POINTER_SIZE * 2) - (i + 1) * SLOT_ENTRY_SIZE
            record_offset = self.data[slot_offset: slot_offset + OFFSET_SIZE]
            record_length = self.data[slot_offset + OFFSET_SIZE: slot_offset + SLOT_ENTRY_SIZE]
            print(
                f"Slot (bytes): {int.from_bytes(record_offset, 'little')} | {int.from_bytes(record_length, 'little')}")
            print(f"Record {i} (bytes): {record_bytes}")
            print(f"Record {i}: {int.from_bytes(record_bytes, 'little')}")


class PageDirectory(Page):
    def __init__(self, file_path: str = None, data: bytearray = bytearray(PAGE_SIZE)):
        self.pages = {}  # Dictionary to store page information
        self.file_path = file_path
        super().__init__(data)

    def find_page(self, page_number) -> Optional[Page]:
        if page_number in self.pages:
            return self.pages[page_number]

        for offset, length in self.page_footer.slot_dir:
            data = self.data[offset:offset + length]
            if int.from_bytes(data[:PAGE_NUM_SIZE], 'little') == page_number:
                assert self.file_path is not None
                with open(self.file_path, "rb") as db:
                    db.seek(page_number * PAGE_SIZE)
                    page = Page(bytearray(db.read(PAGE_SIZE)))
                    self.pages[page_number] = page
                    return page

    def find_or_create_data_page_for_insert(self, needed_space):
        """
        Creates a new data page and adds it to the directory.
        :param:page_number The page number to create.
        Prints a message indicating the new page was created successfully.
        """
        page_num = 0
        for slot_id in range(self.page_footer.slot_count()):
            # loop over slot, read in tuple(record) -> should contain (page num, free space)
            record = self.read_record(slot_id)
            page_num, free_space = int.from_bytes(record[:PAGE_NUM_SIZE], 'little'), int.from_bytes(
                record[FREE_SPACE_SIZE:], 'little')
            if needed_space <= free_space:
                break

        else:
            # no page found make new one
            # save new page in footer, TODO check if there is still space available to link to next page directory
            page = Page()
            page_num += 1
            byte_array = bytearray(
                page_num.to_bytes(PAGE_NUM_SIZE, 'little') + page.free_space().to_bytes(FREE_SPACE_SIZE, 'little'))
            # add data page info to page directory
            super().insert_record(byte_array)
            self.pages[page_num] = page
            return

        assert self.file_path is not None
        with open(self.file_path, "rb") as db:
            db.seek(page_num * PAGE_SIZE)
            page = Page(bytearray(db.read(PAGE_SIZE)))

        self.pages[page_num] = page

    def delete_data_page(self, page_number):
        # Mark a data page as free in the directory
        if page_number in self.pages:
            self.pages[page_number]['status'] = 'free'
            print(f"Deleted data page {page_number}")

    def insert_record(self, data: bytearray):
        for page in self.pages.values():
            if page.is_full():
                # self.full_pages.append(self.pages.pop(page_number))
                continue

            elif page.insert_record(data):
                return True  # Tuple written successfully
        # All existing pages are full, create a new page and write the tuple
        self.find_or_create_data_page_for_insert(len(data) + + SLOT_ENTRY_SIZE)
        return self.insert_record(data)

    def list_free_pages(self):
        return [page for page, info in self.pages.items() if info['status'] == 'free']


class HeapFile:
    def __init__(self, file_path):
        self.file_path = file_path
        if os.path.isfile(file_path):
            with open(file_path, 'rb') as db:
                pd = PageDirectory(file_path, bytearray(db.read(PAGE_SIZE)))
        else:
            pd = PageDirectory()
        self.page_directories: list[PageDirectory] = [pd]

    def delete_record(self, page_id, slot_id):
        if page_id < len(self.pages):
            self.pages[page_id].delete_record(slot_id)

    def update_record(self, byte_id: bytearray, data):
        page, slot_id = self.find_record(byte_id)
        if page.update_record(slot_id, data):
            return True
        else:
            # Not enough free space on page, try to find a new page
            self.page_directories[0].insert_record(data)

    def insert(self, data):
        self.page_directories[0].insert_record(data)

    def find_record(self, byte_id: bytearray) -> (int, int):
        for page_dir in self.page_directories:
            for offset, length in page_dir.page_footer.slot_dir:
                page_number = int.from_bytes(page_dir.data[offset: offset + 3], 'little')
                page: Page = page_dir.find_page(page_number)
                return page, page.find_record(byte_id)

    def read(self, byte_id: bytearray):
        page, slot_id = self.find_record(byte_id)
        return page.read_record(slot_id)

    def find_page(self, page_number):
        for page_directory in self.page_directories:
            if page := page_directory.find_page(page_number):
                return page

    def close(self):
        with open(self.file_path, 'wb') as file:
            for page_dir in self.page_directories:
                file.write(page_dir.data)
                for page in page_dir.pages.values():
                    file.write(page.data)


class Controller:
    def __init__(self, filepath):
        self.heap_file = HeapFile(filepath)

    def insert(self, data, schema: List[str]):
        self.heap_file.insert(utils.encode_record(data, schema))

    def update(self, id_: int, data, schema: List[str]):
        self.heap_file.update_record(utils.encode_record([id_], ['int']), utils.encode_record(data, schema))

    def read(self, id_: int):
        byte_id = utils.encode_record([id_], ['int'])
        return self.heap_file.read(byte_id)

    def delete(self, id_: int):
        (page, slot_id) = self.heap_file.find_record(utils.encode_record([id_], ['int']))
        page.delete_record(slot_id)

    def commit(self):
        self.heap_file.close()


if __name__ == '__main__':
    # create file database.bin
    with open('database.bin', 'wb') as file:
        file.close()
    orm = Controller('database.bin')

    # if os.path.exists('users.csv'):
    #     df = pd.read_csv('users.csv')
    # else:
    #     df = utils.generate_data(1)
    #     df.to_csv('users.csv', index=False)
    #

    schema = ['int', 'var_str', 'short', 'int', 'int', 'byte', 'var_str', 'var_str', 'var_str', 'var_str']
    record = (1, "Alice", 23, 12345, 987654, 4, "alice@email.com", "1234567890", "ACME", "Elm St")
    for i in range(8):
        record = (i,) + record[1:]
        orm.insert(record, schema)

    # orm.update(2, (2, "AAAAAAAAAAAAAAAA", 23, 12345, 987654, 4, "a", "1", "ACME", "Elm St"), schema)
    # orm.delete(5)
    # orm.heap_file.page_directories[0].pages[1].dump()

    # print(utils.decode_record(orm.read(2), schema))

    # for i in range(20, 23):
    #     # orm.insert(i.to_bytes(2, 'little'))
    #     orm.insert(bytearray(i.to_bytes(2, byteorder='little')))
    #     orm.commit()

    orm.heap_file.page_directories[0].pages[1].dump()
    orm.commit()
