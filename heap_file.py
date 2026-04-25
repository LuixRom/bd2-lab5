import struct
import os
import csv

RECORD_FORMAT = None
RECORD_SIZE = None

def format_record(row):
    if len(row) == 6:
        return (
            int(row[0]),
            row[1].encode().ljust(10, b'\x00'),
            row[2].encode().ljust(20, b'\x00'),
            row[3].encode().ljust(20, b'\x00'),
            row[4].encode(),
            row[5].encode().ljust(10, b'\x00')
        )
    else:
        return (
            int(row[0]),
            row[1].encode().ljust(4, b'\x00'),
            row[2].encode().ljust(10, b'\x00'),
            row[3].encode().ljust(10, b'\x00')
        )

def export_to_heap(csv_path: str, heap_path: str, record_format: str, page_size: int):
    global RECORD_FORMAT, RECORD_SIZE

    RECORD_FORMAT = record_format
    RECORD_SIZE = struct.calcsize(RECORD_FORMAT)

    records_per_page = page_size // RECORD_SIZE

    with open(csv_path, newline="", encoding="utf-8") as csvfile, open(heap_path, "wb") as heapfile:
        reader = csv.reader(csvfile)
        next(reader)

        buffer = []

        for row in reader:
            record = format_record(row)
            buffer.append(record)

            if len(buffer) == records_per_page:
                for rec in buffer:
                    heapfile.write(struct.pack(RECORD_FORMAT, *rec))
                
                written = records_per_page * RECORD_SIZE
                heapfile.write(b'\x00' * (page_size - written))
                
                buffer.clear()

        if buffer:
            for rec in buffer:
                heapfile.write(struct.pack(RECORD_FORMAT, *rec))

            written = len(buffer) * RECORD_SIZE
            heapfile.write(b'\x00' * (page_size - written))

def read_page(heap_path: str, page_id: int, page_size: int) -> list[tuple]:
    records = []
    offset = page_id * page_size

    with open(heap_path, "rb") as f:
        f.seek(0, 2)
        if offset >= f.tell():
            return []

        f.seek(offset)
        raw = f.read(page_size)

    for i in range(0, page_size, RECORD_SIZE):
        chunk = raw[i:i+RECORD_SIZE]

        if len(chunk) < RECORD_SIZE:
            continue

        if chunk != b'\x00' * RECORD_SIZE:
            records.append(struct.unpack(RECORD_FORMAT, chunk))

    return records

def write_page(heap_path: str, page_id: int, records: list[tuple], record_format: str, page_size: int):
    record_size = struct.calcsize(record_format)
    offset = page_id * page_size

    mode = "r+b" if os.path.exists(heap_path) else "w+b"

    with open(heap_path, mode) as f:
        f.seek(0, 2)
        if offset > f.tell():
            f.write(b'\x00' * (offset - f.tell()))

        f.seek(offset)

        for rec in records:
            f.write(struct.pack(record_format, *rec))

        written = len(records) * record_size
        f.write(b'\x00' * (page_size - written))

def count_pages(heap_path: str, page_size: int) -> int:
    if not os.path.exists(heap_path):
        return 0
    return os.path.getsize(heap_path) // page_size
