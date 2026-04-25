import os
import heapq
import time
import heap_file

FIELD_INDEX = {
    "emp_no": 0,
    "birth_date": 1,
    "first_name": 2,
    "last_name": 3,
    "gender": 4,
    "hire_date": 5
}

def generate_runs(heap_path, page_size, buffer_size, sort_key):
    B = buffer_size // page_size
    total_pages = heap_file.count_pages(heap_path, page_size)
    key_idx = FIELD_INDEX[sort_key]
    records_per_page = page_size // heap_file.RECORD_SIZE  
    runs = []
    run_id = 0

    for i in range(0, total_pages, B):
        records = []
        for j in range(i, min(i+B, total_pages)):
            records.extend(heap_file.read_page(heap_path, j, page_size))

        records.sort(key=lambda r: r[key_idx])

        run_path = f"run_{run_id}.bin"
        if os.path.exists(run_path):
            os.remove(run_path)
        page_id = 0
        buffer = []

        for rec in records:
            buffer.append(rec)
            if len(buffer) == records_per_page:  
                heap_file.write_page(run_path, page_id, buffer, heap_file.RECORD_FORMAT, page_size)
                buffer = []
                page_id += 1

        if buffer:
            heap_file.write_page(run_path, page_id, buffer, heap_file.RECORD_FORMAT, page_size)

        runs.append(run_path)
        run_id += 1

    return runs

def multiway_merge(run_paths: list[str], output_path: str, page_size: int, buffer_size: int, sort_key: str):
    records_per_page = page_size // heap_file.RECORD_SIZE
    key_idx = FIELD_INDEX[sort_key]

    buffers = []
    pointers = []

    for path in run_paths:
        buffers.append(heap_file.read_page(path, 0, page_size))
        pointers.append((0, 0))

    heap = []

    for i, buf in enumerate(buffers):
        if buf:
            heapq.heappush(heap, (buf[0][key_idx], i, buf[0]))

    output_buffer = []
    page_id = 0

    if os.path.exists(output_path):
        os.remove(output_path)

    while heap:
        _, run_id, rec = heapq.heappop(heap)
        output_buffer.append(rec)

        if len(output_buffer) == records_per_page:
            heap_file.write_page(output_path, page_id, output_buffer, heap_file.RECORD_FORMAT, page_size)
            output_buffer = []
            page_id += 1

        p, r = pointers[run_id]
        r += 1

        while True:
            if r < len(buffers[run_id]):
                break
            else:
                p += 1
                buffers[run_id] = heap_file.read_page(run_paths[run_id], p, page_size)
                r = 0

                if not buffers[run_id]:
                    break

        pointers[run_id] = (p, r)

        if buffers[run_id] and r < len(buffers[run_id]):
            next_rec = buffers[run_id][r]
            heapq.heappush(heap, (next_rec[key_idx], run_id, next_rec))

    if output_buffer:
        heap_file.write_page(output_path, page_id, output_buffer, heap_file.RECORD_FORMAT, page_size)

def external_sort(heap_path: str, output_path: str, page_size: int, buffer_size: int, sort_key: str) -> dict:
    start_total = time.time()

    start = time.time()
    runs = generate_runs(heap_path, page_size, buffer_size, sort_key)
    time_phase1 = time.time() - start

    start = time.time()
    multiway_merge(runs, output_path, page_size, buffer_size, sort_key)
    time_phase2 = time.time() - start

    total_time = time.time() - start_total

    return {
        "runs_generated": len(runs),
        "time_phase1_sec": time_phase1,
        "time_phase2_sec": time_phase2,
        "time_total_sec": total_time
    }

if __name__ == "__main__":
    PAGE_SIZE = 4096

    csv_path = "employee.csv"
    heap_path = "data/employee.bin"
    sorted_path = "data/employees_sorted.bin"

    record_format = "i10s20s20sc10s"

    print("=== PREPARANDO DATOS ===")
    heap_file.export_to_heap(csv_path, heap_path, record_format, PAGE_SIZE)

    print("\n=== EXPERIMENTOS TPMMS ===")
    for BUFFER_SIZE in [65536, 131072, 262144]:
        print(f"\nBUFFER_SIZE: {BUFFER_SIZE // 1024} KB")

        stats = external_sort(
            heap_path,
            sorted_path,
            PAGE_SIZE,
            BUFFER_SIZE,
            "hire_date"
        )

        B = BUFFER_SIZE // PAGE_SIZE
        total_pages = heap_file.count_pages(heap_path, PAGE_SIZE)
        io_total = 4 * total_pages

        print(f"B (páginas RAM): {B}")
        print(f"Runs: {stats['runs_generated']}")
        print(f"Fase 1: {stats['time_phase1_sec']:.4f} s")
        print(f"Fase 2: {stats['time_phase2_sec']:.4f} s")
        print(f"Total: {stats['time_total_sec']:.4f} s")
        print(f"I/O Total (páginas): {io_total}")

    def decode_record(rec):
        return rec[5].decode().strip('\x00')

    print("\nPrimeras fechas ordenadas:")
    print("Pages sorted:", heap_file.count_pages(sorted_path, PAGE_SIZE))

    for p in range(2):
        records = heap_file.read_page(sorted_path, p, PAGE_SIZE)
        for r in records[:5]:
            print(decode_record(r))

    def check_sorted(heap_path, page_size):
        prev = None
        for p in range(heap_file.count_pages(heap_path, page_size)):
            records = heap_file.read_page(heap_path, p, page_size)
            for rec in records:
                date = rec[5].decode().strip('\x00')
                if prev and date < prev:
                    print("[ERROR]:", prev, ">", date)
                    return False
                prev = date
        print("\n[EXITO] Archivo ordenado correctamente")
        return True

    check_sorted(sorted_path, PAGE_SIZE)
