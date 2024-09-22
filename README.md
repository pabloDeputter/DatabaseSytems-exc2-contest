### Project Name
**Heap File with 2-Way External Merge Sort**

### Contributors
- Pablo Deputter
- Robin Dillen
- Gaia Colombo

We developed a system that integrates an unordered heap file to manage variable-length records, combined with a 2-way external merge sort algorithm for efficient sorting under memory constraints. This project focuses on both efficient record management and sorting, optimized for limited memory usage.

### Data Pages & Directories

We used an unordered heap file structure to store variable-length records, with each data page sized at 4KB. Data pages are organized using a linked list of page directories to indicate which pages are full.  Instead of a header, we chose to have a footer to store the number of slots and the free space pointer. This decision was arbitrary, as Pablo simply preferred it.

![](images/page_layout.png)

Both data pages and directories share a similar footer structure:

- **Number of Slots**: 2 bytes
- **Free Space Pointer**: 2 bytes (indicates where new data can be written)

Each slot includes:
- **Offset**: 2 bytes (start of the record)
- **Length**: 2 bytes (size of the record)
- **Total Slot Size**: 4 bytes

Differences between data pages and directories:
- **Data Pages**: Store variable-length records
- **Directories**: Store fixed-size (6 bytes) records, which track the data page number (3 bytes) and available space (3 bytes)

The first slot in a directory contains metadata, such as its own page number and a pointer to the next directory, used to calculate the relative number of a data page within the directory.

### Utilities

In `utils.py`, we implemented utility methods to handle record encoding and decoding based on a given schema. 
```python
# Example Schema and Record
schema = ['int', 'var_str', 'short', 'int', 'int', 'byte', 'var_str', 'var_str', 'var_str', 'var_str']
record = (1, "Alice", 23, 12345, 987654, 4, "alice@email.com", "1234567890", "ACME", "Elm St")
utils.encode_record(schema, record)
```

### CRUD Operations

- **Create**: We search the page directories for available space in data pages. If no suitable page is found, a new data page is created in the next available directory slot. Records are inserted at the free space pointer, and the pointer and slot lengths are updated. If a deleted slot is found (zero-length slot), it is repurposed.
  
- **Read**: The record is searched by scanning all pages for the corresponding ID (assumed to be the first element). If the record is not found, we print 'not found' and return `None`.
  
- **Update**: If the new record has the same length, we overwrite the existing data. If the record is smaller, we overwrite it and compact the page. For larger records, we delete the old record and insert the new one, potentially into a different page.
  
- **Delete**: We set the record's slot length to zero and compact the page to shift remaining records to the left.

### Sorting - 2-Way External Merge Sort

We implemented a 2-way external merge sort to handle sorting under memory constraints, allowing only three pages in memory at a time (two input pages and one output page).

#### Phase 0: Initial Sorting
In this phase, individual pages are loaded into memory and sorted. Each sorted page is stored as a "run" consisting of key-value pairs, where the key is the sort key and the value is the record itself. The filenames of these runs are tracked in a list called `merged_files`.

#### Phase X: Merging Runs
Pairs of runs are merged into larger runs using the `merge_pages` function. The function compares the first entries of each run and writes the smaller one to the output. This process continues until all values are merged.

Key steps in the merging process:
- Compare the first values from each run, add the smaller one to the output.
- Advance to the next value in the corresponding run for the next comparison.
- If a run is exhausted, the remaining values from the other run are appended to the output.
- Output files are saved to disk once full, freeing memory for further merging.

### Test & Optimizations

The `test.py` file contains methods to test our CRUD operations and sorting implementation. While indexing and compression are not implemented, these areas are identified for future optimization efforts. Additionally, there are unresolved issues with the intermediate output files during the merge process, which resulted in some errors during merging.

### Performance

We tested our heap file implementation with a dataset of 10,000 records. Results:

- **Inserts**: ~16,000 per second
- **Reads**: ~1,500 per second
- **Updates**: ~2,500 per second
- **Deletes**: ~3,500 per second
- **Database Size**: 1,204,224 bytes

Reading performance suffers from the lack of indexes, as a linear scan is used to search for records by ID. The total database size remains comparable to compressed implementations, though we lose significant space due to additional information required for CRUD operations.

### References
[cs186berkeley - Disk & Files](https://cs186berkeley.net/fa20/resources/static/notes/n02-DisksFiles.pdf)
