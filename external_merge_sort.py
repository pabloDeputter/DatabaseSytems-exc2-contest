import os
import pickle
from typing import List, Tuple

from database import Page


def two_way_external_merge_sort(pages: List[Page], key: int):
    # Phase 0: Sort individual pages

    merged_files: List[Tuple[str]] = []

    for i, page in enumerate(pages[:7]):
        run = page.sort(key)
        run = [(entry[key], entry) for entry in run]
        # write to file

        with open(str(i) + '_0', 'wb') as f:
            pickle.dump(run, f)
            merged_files.append((str(i),))

    def merge_pages(bucket1: tuple[str], bucket2: tuple[str]):
        bucket1Index = 0
        bucket2Index = 0
        with open(f'{bucket1[bucket1Index]}_{len(bucket1) // 2}', 'rb') as f1, open(
                f'{bucket2[bucket2Index]}_{len(bucket1) // 2}', 'rb') as f2:
            run1 = pickle.load(f1)
            run2 = pickle.load(f2)

        name_iter = iter((*bucket1, *bucket2))
        iter1 = iter(run1)
        iter2 = iter(run2)
        a = next(iter1)
        b = next(iter2)
        empty = [False, False]
        r = max(len(run1), len(run2))
        while not all(empty):
            tmp_run = []
            for _ in range(r):
                if not empty[0] and (empty[1] or a <= b):  # empty[1] = b is empty
                    tmp_run.append(a)
                    try:
                        a = next(iter1)
                    except StopIteration:
                        os.remove(f'{bucket1[bucket1Index]}_{len(bucket1) // 2}')
                        bucket1Index += 1
                        if bucket1Index < len(bucket1):
                            with open(f'{bucket1[bucket1Index]}_{len(bucket1) // 2}', 'rb') as f1:
                                run1 = pickle.load(f1)
                                iter1 = iter(run1)
                            r = max(r, len(run1))
                        else:
                            empty[0] = True
                            a = None
                    continue
                elif not empty[1] and (empty[0] or a > b):
                    tmp_run.append(b)
                    try:
                        b = next(iter2)
                    except StopIteration:
                        os.remove(f'{bucket2[bucket2Index]}_{len(bucket1) // 2}')

                        bucket2Index += 1
                        if bucket2Index < len(bucket2):
                            with open(f'{bucket2[bucket2Index]}_{len(bucket1) // 2}', 'rb') as f2:
                                run2 = pickle.load(f2)
                                iter2 = iter(run2)
                            r = max(r, len(run2))
                        else:
                            empty[1] = True
                            b = None
                    continue

                break

            fp = next(name_iter)
            print(fp, (*bucket1, *bucket2), bucket1Index, bucket2Index)
            with open(fp + '_' + str(len(bucket1)), 'wb') as f:
                pickle.dump(tmp_run, f)

    while len(merged_files) > 1:
        merged_files_tmp: list[tuple[str]] = []
        for i in range(0, len(merged_files), 2):
            if i + 1 == len(merged_files):
                merged_files_tmp.append(merged_files[i])
                continue
            merge_pages(merged_files[i], merged_files[i + 1])
            merged_files_tmp.append(merged_files[i] + merged_files[i + 1])
        merged_files = merged_files_tmp

    with open('result', 'w') as resf:
        for file in merged_files[0]:
            with open(file, 'rb') as f:
                res = pickle.load(f)
            resf.write('\n'.join(res))

    # def phase0():
    #     for page in allPages:
    #         read(page)
    #         run = sort(page)
    #         store(run)  # where?
    #         delete(page) ??

    # def phaseX():
    #
    #     # Phase 2: Merge sorted pages
    #     while len(pages) > 1:
    #         new_pages = []
    #         for i in range(0, len(pages), 2):
    #             if i + 1 < len(pages):
    #                 merged_page = merge_pages(pages[i], pages[i + 1])
    #                 new_pages.append(merged_page)
    #             else:
    #                 new_pages.append(pages[i])
    #         pages = new_pages
    #
    #     return pages[0].records if pages else []
    #
    # def merge_pages(page1, page2):
    #     merged_records = []
    #     heap = [(record, page) for page in [page1, page2] for record in page.records]
    #     heapq.heapify(heap)
    #
    #     while heap:
    #         record, _ = heapq.heappop(heap)
    #         merged_records.append(record)
    #
    #     return Page(merged_records)
