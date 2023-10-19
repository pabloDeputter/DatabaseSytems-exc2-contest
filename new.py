# Database Page is normally between 512B and 16KB
import random

PAGE_SIZE = 512


class PageHeader:
    def __init__(self):
        # Pointer to free space
        self.free_space_pointer = 0
        # Contains pairs (length, offset to beginning of record), if offset == -1, then record is deleted
        self.slot_dir = []
        self.slot_entry_size = 8
        self.free_space_pointer_size = 4

    def slot_count(self):
        return len(self.slot_dir)


class Page:
    def __init__(self):
        self.page_header = PageHeader()
        # Use padding to achieve fixed size
        self.data = bytearray(PAGE_SIZE)

    def free_space(self):
        # 512 100 - (x * 8) - 4 = 508
        # Page header grows from bottom up, records grow top down.
        # Free space pointer - space occupied by page header - 4 bytes for free space pointer
        return PAGE_SIZE - self.page_header.free_space_pointer - (
                len(self.page_header.slot_dir) * self.page_header.slot_entry_size) - self.page_header.free_space_pointer_size

    def compact(self):
        pass

    def insert_record(self, record):
        if len(record) + self.page_header.slot_entry_size <= self.free_space():
            self.page_header.slot_dir.append((len(record), self.page_header.free_space_pointer))
            self.data[self.page_header.free_space_pointer:self.page_header.free_space_pointer + len(record)] = record
            self.page_header.free_space_pointer += len(record)
            return True
        return False

    def delete_record(self, slot_id):
        length, offset = self.page_header.slot_dir[slot_id]
        self.page_header.slot_dir[slot_id] = (length, -1)

    def is_full(self):
        return self.free_space() <= 0

    def is_packed(self):
        """
        Check if page is packed, meaning no deleted records.
        """
        return all(offset != -1 for _, offset in self.page_header.slot_dir)

    def compact_page(self):
        """
        Reclaim unused space so that records are contiguous and limit fragmentation.
        """
        write_ptr = 0

        for i, (length, offset) in enumerate(self.page_header.slot_dir):
            if offset != -1:  # Skip deleted records
                if write_ptr != offset:  # No need to move if it's already in the right place
                    self.data[write_ptr:write_ptr + length] = self.data[offset:offset + length]
                self.page_header.slot_dir[i] = (length, write_ptr)
                write_ptr += length

        self.page_header.free_space_pointer = write_ptr


class PageDirectory(Page):
    ...


class HeapFile:
    def __init__(self, file_path):
        self.file_path = file_path
        self.full_pages = []
        self.pages = []

    def create_page(self):
        page = Page()
        self.pages.append(page)

    def delete_record(self, page_id, slot_id):
        if page_id < len(self.pages):
            self.pages[page_id].delete_record(slot_id)

    def insert_record(self, data):
        for i, page in reversed(list(enumerate(self.pages))):
            if page.is_full():
                self.full_pages.append(self.pages.pop(i))
                continue

            elif page.insert_record(data):
                return True  # Tuple written successfully
        # All existing pages are full, create a new page and write the tuple
        self.create_page()
        return self.insert_record(data)

    def close(self):
        with open(self.file_path, 'wb') as file:
            for page in self.pages:
                file.write(page.data)


if __name__ == '__main__':
    heap = HeapFile('robinisnietcool.bin')

    for i in range(10):
        heap.insert_record(bytearray("i", "UTF-8") * i)
        heap.close()
