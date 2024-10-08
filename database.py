import os
import time
from collections import deque
from typing import Optional, List, Tuple
import pandas as pd

import utils

# Page Constants
PAGE_SIZE = 4096  # Database Page is normally between 512B and 16KB
# (offset, length) in slot dir
OFFSET_SIZE = 2
LENGTH_SIZE = 2
SLOT_ENTRY_SIZE = OFFSET_SIZE + LENGTH_SIZE

FREE_SPACE_POINTER_SIZE = 2
NUMBER_SLOTS_SIZE = 2
FOOTER_SIZE = FREE_SPACE_POINTER_SIZE + NUMBER_SLOTS_SIZE

# PageDirectory Constants
PAGE_NUM_SIZE = 3
FREE_SPACE_SIZE = 3
CACHE_SIZE = 10


class PageFooter:
    def __init__(self, data: bytearray = None):
        data = bytearray(PAGE_SIZE) if data is None else data
        # Pointer to free space
        self.free_space_pointer = int.from_bytes(data[-FREE_SPACE_POINTER_SIZE:], 'little')
        # Number of slots
        slot_count = int.from_bytes(data[-FOOTER_SIZE:-FREE_SPACE_POINTER_SIZE], 'little')

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
    def __init__(self, data=None):
        self.data = bytearray(PAGE_SIZE) if data is None else data
        self.page_footer = PageFooter(self.data)
        page_footer_data = self.page_footer.data()
        self.data[-len(page_footer_data):] = page_footer_data

    def update_header(self):
        page_footer_data = self.page_footer.data()
        self.data[-len(page_footer_data):] = page_footer_data

    def free_space(self):
        # 512 - 100 - (x * 8) - 4 = 508
        # Page header grows from bottom up, records grow top down.
        # Free space pointer - space occupied by page header - 4 bytes for free space pointer
        return PAGE_SIZE - self.page_footer.free_space_pointer - (
                len(self.page_footer.slot_dir) * SLOT_ENTRY_SIZE) - FREE_SPACE_POINTER_SIZE - NUMBER_SLOTS_SIZE

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

    def sort(self, index: int):
        """
        :param index: tuple index for comparison
        :return:
        """
        records = []
        for slot_id in range(self.page_footer.slot_count()):
            records.append(self.read_record(slot_id))

        return sorted(records, key=lambda x: (x[index], x[0]))

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
                    LENGTH_SIZE, 'little')
                write_ptr += length

            assert int.from_bytes(self.data[4092:4094], 'little') != 0, i

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
    def __init__(self, file_path: str = None, data: bytearray = None, current_number: int = None):
        self.data = bytearray(PAGE_SIZE) if data is None else data
        self.pages = {}  # Dictionary to store page information
        self.file_path = file_path
        super().__init__(self.data)
        # Information about page directories
        if data is None and current_number is None:
            self.pd_number = 0
            self.next_dir = 0
            byte_array = bytearray(
                self.pd_number.to_bytes(PAGE_NUM_SIZE, 'little') + self.next_dir.to_bytes(FREE_SPACE_SIZE, 'little'))
            # First slot points to record --> (current_pd_number, next_pd_number)
            super().insert_record(byte_array)
        elif current_number is not None:
            self.pd_number = current_number + 1
            self.next_dir = 0
            byte_array = bytearray(
                self.pd_number.to_bytes(PAGE_NUM_SIZE, 'little') + self.next_dir.to_bytes(FREE_SPACE_SIZE, 'little'))
            # First slot points to record --> (current_pd_number, next_pd_number)
            super().insert_record(byte_array)
        else:
            record = super().read_record(0)
            self.pd_number, self.next_dir = int.from_bytes(record[:PAGE_NUM_SIZE], 'little'), int.from_bytes(
                record[FREE_SPACE_SIZE:], 'little')

    def find_page(self, page_number) -> Optional[Page]:

        if page_number in self.pages:
            return self.pages[page_number]

        for offset, length in self.page_footer.slot_dir[1:]:
            data = self.data[offset:offset + length]
            if int.from_bytes(data[:PAGE_NUM_SIZE], 'little') == page_number:
                # TODO - reading from record that was inserted while file was open and doesn't exist yet gives error
                assert self.file_path is not None
                with open(self.file_path, "rb") as db:
                    db.seek(page_number * PAGE_SIZE)
                    page = Page(bytearray(db.read(PAGE_SIZE)))
                    self.pages[page_number] = page
                    return page

    def find_record(self, byte_id: bytearray) -> (int, int):
        for offset, length in self.page_footer.slot_dir[1:]:
            page_number = int.from_bytes(self.data[offset: offset + PAGE_NUM_SIZE], 'little')
            page: Page = self.find_page(page_number)
            record = page.find_record(byte_id)
            if record is not None:
                return page, record
        return False

    def find_or_create_data_page_for_insert(self, needed_space):

        page_num = 0
        # TODO NOW - minus one since first slot references to page dir. info
        for slot_id in range(self.page_footer.slot_count() - 1):
            # loop over slot, read in tuple(record) -> should contain (page num, free space)
            # TODO NOW - increase slot_id with 1 since first slot will reference page dir. info
            offset, length = self.page_footer.slot_dir[slot_id + 1]
            record = self.data[offset: offset + length]
            page_num, free_space = int.from_bytes(record[:PAGE_NUM_SIZE], 'little'), int.from_bytes(
                record[FREE_SPACE_SIZE:], 'little')
            if needed_space <= free_space:
                break

        else:
            # no page found make new one
            # save new page in footer, TODO check if there is still space available to link to next page directory
            # Check if there is enough free space in page dir. --> (page_nr, free_space) + slot size
            if (PAGE_NUM_SIZE + FREE_SPACE_SIZE) + SLOT_ENTRY_SIZE > self.free_space():
                return False

            page = Page()
            # Find the max. current page number
            page_num = int.from_bytes(self.read_record(len(self.page_footer.slot_dir) - 1)[:PAGE_NUM_SIZE],
                                      'little') + 1
            byte_array = bytearray(
                page_num.to_bytes(PAGE_NUM_SIZE, 'little') + page.free_space().to_bytes(FREE_SPACE_SIZE, 'little'))
            # add data page info to page directory
            super().insert_record(byte_array)
            self.pages[page_num] = page
            return True

        # Gets executed when space is left in Page
        assert self.file_path is not None
        with open(self.file_path, "rb") as db:
            db.seek(page_num * PAGE_SIZE)
            page = Page(bytearray(db.read(PAGE_SIZE)))

        self.pages[page_num] = page
        return True

    def delete_data_page(self, page_number):
        # Mark a data page as free in the directory
        if page_number in self.pages:
            self.pages[page_number]['status'] = 'free'
            print(f"Deleted data page {page_number}")

    def insert_record(self, data: bytearray):
        for nr, page in self.pages.items():
            if page.is_full():
                # self.full_pages.append(self.pages.pop(page_number))
                continue

            elif page.insert_record(data):
                self.update_free_space(nr, page.free_space())
                return True  # Tuple written successfully
        # All existing pages are full, create a new page and write the tuple
        if not self.find_or_create_data_page_for_insert(len(data) + SLOT_ENTRY_SIZE):
            return False
        return self.insert_record(data)

    def update_free_space(self, page_nr, free_space):
        # TODO NOW - Calculate relative page_nr inside page dir.
        page_nr = page_nr - self.pd_number
        offset, length = self.page_footer.slot_dir[page_nr]
        self.data[offset + PAGE_NUM_SIZE:offset + PAGE_NUM_SIZE + FREE_SPACE_SIZE] = free_space.to_bytes(
            FREE_SPACE_SIZE, 'little')

    def list_free_pages(self):
        return [page for page, info in self.pages.items() if info['status'] == 'free']


class HeapFile:
    def __init__(self, file_path):
        self.file_path = file_path
        if os.path.isfile(file_path):
            with open(file_path, 'rb') as db:
                pd = PageDirectory(file_path=file_path, data=bytearray(db.read(PAGE_SIZE)))
        else:
            pd = PageDirectory()
        self.page_directories: list[PageDirectory] = [pd]

    def read_page_dir(self, pd: PageDirectory) -> PageDirectory:
        if new_pd := list(filter(lambda pgd: pgd.pd_number == pd.next_dir, self.page_directories)):
            return new_pd[0]

        with open(self.file_path, 'rb') as db:
            db.seek(pd.next_dir * PAGE_SIZE)
            new_pd = PageDirectory(file_path=self.file_path, data=bytearray(db.read(PAGE_SIZE)))
        self.page_directories.append(new_pd)
        return new_pd

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

    def insert_record(self, data):
        pd: PageDirectory = self.page_directories[0]

        # Iterate over all page dir., if full move to the next one
        while not (inserted := pd.insert_record(data)) and pd.next_dir != 0:
            pd = self.read_page_dir(pd)

        # If last dir. is full, create new one
        if not inserted:
            # Find the max. current page number
            max_page_nr = int.from_bytes(pd.read_record(len(pd.page_footer.slot_dir) - 1)[:PAGE_NUM_SIZE], 'little')
            # Create new page directory
            new_pd = PageDirectory(file_path=self.file_path, current_number=max_page_nr)
            pd.next_dir = new_pd.pd_number
            # (current_pd_number, next_pd_number)
            pd.data[PAGE_NUM_SIZE:PAGE_NUM_SIZE + FREE_SPACE_SIZE] = pd.next_dir.to_bytes(FREE_SPACE_SIZE, 'little')
            self.page_directories.append(new_pd)
            return new_pd.insert_record(data)
        return True

    def find_record(self, byte_id: bytearray) -> (int, int):
        pd: PageDirectory = self.page_directories[0]

        while True:
            if result := pd.find_record(byte_id):
                return result
            if pd.next_dir == 0:
                break
            pd = self.read_page_dir(pd)

        return None, None

    def read_record(self, byte_id: bytearray):
        page, slot_id = self.find_record(byte_id)
        if page is None:
            print('Record not found!')
            return
        return page.read_record(slot_id)

    def find_page(self, page_number):
        for page_directory in self.page_directories:
            if page := page_directory.find_page(page_number):
                return page

    def close(self):
        # Create file if it doesn't exist
        if not os.path.exists(self.file_path):
            with open(self.file_path, 'wb') as file:
                file.close()
        print("closing")
        with open(self.file_path, 'r+b') as file:
            for page_dir in self.page_directories:
                file.seek(page_dir.pd_number * PAGE_SIZE)
                file.write(page_dir.data)
                for page_nr, page in page_dir.pages.items():
                    file.seek(page_nr * PAGE_SIZE)
                    file.write(page.data)

