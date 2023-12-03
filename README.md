### Contributors
- Robin Dillen
- Gaia Colombo


2-Way External Merge Sort Overview

In this implementation, we're dealing with the 2-way external merge sort algorithm. Our memory constraints allow us to handle only three pages: two for input comparison and one for output.

Phase 0

During this phase, we load one page into memory and sort it. This sorted page becomes a "run," which consists of key-value pairs. The key represents the key we sort by, and the value is the entry itself. Simultaneously, we keep track of the filenames associated with each run in a list called "merged_files."

Phase X

In this phase, we iterate over pairs of runs to merge them into a run with double the size. The "merged_files" list is updated to include the new files associated with these runs.

To merge two runs, we've implemented a function called "merge_pages." It takes two buckets as input, each containing a list of files from their respective runs. We start by opening the first two files and initiate the merging process.

Here's how the merging works:

Compare the first two values of each run and add the smaller one to the temporary run (output).
After adding a value to the output, move to the next value in the corresponding run for the next comparison.
When you reach the end of a file, remove it from disk to free up memory, as the values are now in the output.
If you reach the end of a run, mark it as empty. Now, simply add the remaining tuples from the other run to the output file.
Periodically, when the output file becomes "full," save it to disk, creating space in memory for a new output file.
This process continues until both runs have been completely merged.

By the end of this entire process, you should have a result file containing the entries sorted in order.

However, it's worth noting that our current implementation isn't working as expected. We had some issues with our intermediate output files being filled properly which resulted in some errors.
