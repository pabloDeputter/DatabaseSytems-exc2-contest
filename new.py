# Database Page is normally between 512B and 16KB
import random
import base64

PAGE_SIZE = 512
OFFSET_SIZE = 4
LENGTH_SIZE = 4


class PageFooter:
    def __init__(self):
        # Pointer to free space
        self.free_space_pointer = 0
        # Contains pairs (offset to beginning of record, length of record), if offset == -1, then record is deleted
        self.slot_dir = []
        self.slot_entry_size = 8
        self.free_space_pointer_size = 4

    def slot_count(self):
        return len(self.slot_dir)

    def data(self) -> bytearray:
        return bytearray(len(self.slot_dir).to_bytes(4, byteorder='little') + self.free_space_pointer.to_bytes(4,
                                                                                                               byteorder='little'))


class Page:
    def __init__(self):
        self.page_footer = PageFooter()
        # Use padding to achieve fixed size
        self.data = bytearray(PAGE_SIZE)
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
                len(self.page_footer.slot_dir) * self.page_footer.slot_entry_size) - self.page_footer.free_space_pointer_size

    def compact(self):
        pass

    def insert_record(self, record: bytearray):
        """
        If there is not enough free space -> try to compact data, and use this free space, otherwise record can't be stored
        :param record:
        :return:
        """
        needed_space = len(record) + self.page_footer.slot_entry_size
        if needed_space <= self.free_space():
            # Write data
            self.data[self.page_footer.free_space_pointer:self.page_footer.free_space_pointer + len(record)] = record

            # Add new slot
            new_slot_offset = self.page_footer.slot_count() * self.page_footer.slot_entry_size

            # Update slots                             static footer size              sizeof current slots        size of new slot
            new_slot_offset = PAGE_SIZE - self.page_footer.free_space_pointer_size * 2 - new_slot_offset - self.page_footer.slot_entry_size
            # (offset, length)
            self.data[new_slot_offset: new_slot_offset + 4] = self.page_footer.free_space_pointer.to_bytes(4, 'little')
            self.data[new_slot_offset + 4: new_slot_offset + 8] = len(record).to_bytes(4, 'little')

            # Update page footer
            self.page_footer.slot_dir.append((self.page_footer.free_space_pointer, len(record)))

            # Update free space pointer
            self.page_footer.free_space_pointer += len(record)
            self.update_header()

            return True
        return False

    def delete_record(self, slot_id):
        offset, length = self.page_footer.slot_dir[slot_id]
        self.page_footer.slot_dir[slot_id] = (length, -1)

    def is_full(self):
        return self.free_space() <= 0

    def is_packed(self):
        """
        Check if page is packed, meaning no deleted records.
        """
        return all(offset != -1 for _, offset in self.page_footer.slot_dir)

    def compact_page(self):
        """
        Reclaim unused space so that records are contiguous and limit fragmentation.

        Eager -> compact page when a record is deleted
        Lazy -> compact page when page is full
        """
        write_ptr = 0

        for i, (length, offset) in enumerate(self.page_footer.slot_dir):
            # Skip deleted records
            if offset != -1:
                if offset != write_ptr:
                    self.data[write_ptr:write_ptr + length] = self.data[offset:offset + length]
                self.page_footer.slot_dir[i] = (length, write_ptr)
                write_ptr += length

        self.page_footer.free_space_pointer = write_ptr

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
            f"Free Space Pointer (bytes): {int.from_bytes(self.data[-self.page_footer.free_space_pointer_size:], 'little')}")

        print(f"Number of slots: {self.page_footer.slot_count()}")
        print(
            f"Number of slots (bytes): {int.from_bytes(self.data[PAGE_SIZE - self.page_footer.free_space_pointer_size * 2:PAGE_SIZE - self.page_footer.free_space_pointer_size], 'little')}")
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

            slot_offset = -(self.page_footer.free_space_pointer_size * 2) - (i + 1) * self.page_footer.slot_entry_size
            record_offset = self.data[slot_offset: slot_offset + 4]
            record_length = self.data[slot_offset + 4: slot_offset + 8]
            print(
                f"Slot (bytes): {int.from_bytes(record_offset, 'little')} | {int.from_bytes(record_length, 'little')}")
            print(f"Record {i} (bytes): {record_bytes}")
            print(f"Record {i}: {int.from_bytes(record_bytes, 'little')}")


class PageDirectory(Page):
    def __init__(self):
        self.pages = {}  # Dictionary to store page information
        super().__init__()

    def create_data_page(self, page_number):
        """
        Creates a new data page and adds it to the directory.
        :param:page_number The page number to create.
        Prints a message indicating the new page was created successfully.
        """
        # Create a new data page and add it to the directory
        page = Page()
        self.pages[page_number] = page

        byte_array = bytearray()
        for num in (page_number, page.free_space()):
            # Calculate the number of bytes required to represent the integer
            num_bytes = (num.bit_length() + 7) // 8
            byte_array.extend(num.to_bytes(num_bytes, byteorder='little'))
        super().insert_record(byte_array)
        # if page_number not in self.pages:
        #     self.pages[page_number] = {'metadata': metadata, 'status': 'in_use'}
        #     print(f"Created data page {page_number} with metadata: {metadata}")

    def delete_data_page(self, page_number):
        # Mark a data page as free in the directory
        if page_number in self.pages:
            self.pages[page_number]['status'] = 'free'
            print(f"Deleted data page {page_number}")

    def insert_record(self, data: bytearray):
        page_number = 0
        for page_number, page in self.pages.items():
            if page.is_full():
                # self.full_pages.append(self.pages.pop(page_number))
                continue

            elif page.insert_record(data):
                return True  # Tuple written successfully
        # All existing pages are full, create a new page and write the tuple
        self.create_data_page(page_number + 1)
        return self.insert_record(data)

    def list_free_pages(self):
        # List all free data pages in the directory
        free_pages = [page for page, info in self.pages.items() if info['status'] == 'free']
        return free_pages


class HeapFile:
    def __init__(self, file_path):
        self.file_path = file_path
        self.page_directories: list[PageDirectory] = [PageDirectory()]

    def delete_record(self, page_id, slot_id):
        if page_id < len(self.pages):
            self.pages[page_id].delete_record(slot_id)

    def update_record(self):
        pass

    def close(self):
        with open(self.file_path, 'wb') as file:
            for page_dir in self.page_directories:
                file.write(page_dir.data)
                for page in page_dir.pages.values():
                    file.write(page.data)


class Controller:
    def __init__(self, filepath):
        self.heap_file = HeapFile(filepath)

    def insert(self, data):
        self.heap_file.page_directories[0].insert_record(data)

    def update(self, rid):
        pass

    def commit(self):
        self.heap_file.close()


if __name__ == '__main__':
    orm = Controller('database.bin')

    for i in range(20, 23):
        # orm.insert(i.to_bytes(2, 'little'))
        orm.insert(bytearray(i.to_bytes(2, byteorder='little')))
        orm.commit()

    orm.heap_file.page_directories[0].pages[1].dump()
