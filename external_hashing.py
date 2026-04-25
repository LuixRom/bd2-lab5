import os
import time
import heap_file

FIELD_INDEX = {
    "emp_no": 0,
    "dept_no": 1,
    "from_date": 2,
    "to_date": 3
}

def partition_data(heap_path: str, page_size: int, buffer_size: int, group_key: str) -> tuple[list[str], int, int]:
    B = buffer_size // page_size
    k = B - 1
    key_idx = FIELD_INDEX[group_key]
    records_per_page = page_size // heap_file.RECORD_SIZE

    partition_paths = [f"part_{i}.bin" for i in range(k)]
    for path in partition_paths:
        if os.path.exists(path):
            os.remove(path)

    page_ids = [0] * k
    buffers = [[] for _ in range(k)]
    
    total_pages = heap_file.count_pages(heap_path, page_size)
    pages_read = 0
    pages_written = 0

    for i in range(total_pages):
        records = heap_file.read_page(heap_path, i, page_size)
        pages_read += 1
        
        for rec in records:
            key_val = rec[key_idx]
            p = hash(key_val) % k
            buffers[p].append(rec)
            
            if len(buffers[p]) == records_per_page:
                heap_file.write_page(partition_paths[p], page_ids[p], buffers[p], heap_file.RECORD_FORMAT, page_size)
                pages_written += 1
                page_ids[p] += 1
                buffers[p].clear()

    for p in range(k):
        if buffers[p]:
            heap_file.write_page(partition_paths[p], page_ids[p], buffers[p], heap_file.RECORD_FORMAT, page_size)
            pages_written += 1
            page_ids[p] += 1

    return partition_paths, pages_read, pages_written


def aggregate_partitions(partition_paths: list[str], page_size: int, buffer_size: int, group_key: str) -> tuple[dict, int]:
    key_idx = FIELD_INDEX[group_key]
    result = {}
    pages_read = 0

    for path in partition_paths:
        if not os.path.exists(path):
            continue
            
        total_pages = heap_file.count_pages(path, page_size)
        
        partition_records = []
        for i in range(total_pages):
            records = heap_file.read_page(path, i, page_size)
            pages_read += 1
            partition_records.extend(records)
            
        for rec in partition_records:
            key_val = rec[key_idx]
            if isinstance(key_val, bytes):
                key_val = key_val.decode().strip('\x00')
            
            result[key_val] = result.get(key_val, 0) + 1

    return result, pages_read


def external_hash_group_by(heap_path: str, page_size: int, buffer_size: int, group_key: str) -> dict:
    start_total = time.time()

    start_phase1 = time.time()
    partition_paths, pages_read_p1, pages_written_p1 = partition_data(heap_path, page_size, buffer_size, group_key)
    time_phase1 = time.time() - start_phase1

    start_phase2 = time.time()
    result, pages_read_p2 = aggregate_partitions(partition_paths, page_size, buffer_size, group_key)
    time_phase2 = time.time() - start_phase2

    total_time = time.time() - start_total

    return {
        'result': result,
        'partitions_created': len(partition_paths),
        'pages_read': pages_read_p1 + pages_read_p2,
        'pages_written': pages_written_p1,
        'time_phase1_sec': time_phase1,
        'time_phase2_sec': time_phase2,
        'time_total_sec': total_time
    }

if __name__ == "__main__":
    PAGE_SIZE = 4096
    
    csv_path = "department_employee.csv"
    heap_path = "data/department_employee.bin"

    record_format = "i4s10s10s"

    print("=== PREPARANDO DATOS ===")
    heap_file.export_to_heap(csv_path, heap_path, record_format, PAGE_SIZE)
    total_pages_heap = heap_file.count_pages(heap_path, PAGE_SIZE)
    print(f"Páginas en heap file: {total_pages_heap}")

    print("\n=== EJECUTANDO EXPERIMENTOS ===")
    for BUFFER_SIZE in [65536, 131072, 262144]:
        print(f"\n--- BUFFER_SIZE: {BUFFER_SIZE // 1024} KB ---")
        stats = external_hash_group_by(heap_path, PAGE_SIZE, BUFFER_SIZE, "from_date")
        
        M_paginas = BUFFER_SIZE // PAGE_SIZE
        particiones = stats['partitions_created']
        t_fase1 = stats['time_phase1_sec']
        t_fase2 = stats['time_phase2_sec']
        t_total = stats['time_total_sec']
        io_total = stats['pages_read'] + stats['pages_written']
        
        print(f"M (páginas en RAM): {M_paginas}")
        print(f"Runs / Particiones: {particiones}")
        print(f"Tiempo Fase 1: {t_fase1:.4f} s")
        print(f"Tiempo Fase 2: {t_fase2:.4f} s")
        print(f"Tiempo Total: {t_total:.4f} s")
        print(f"I/O Total (páginas): {io_total}")
