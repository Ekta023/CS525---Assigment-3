# Record Manager – CS525 Assignment 3

This Record Manager is a system designed to manage fixed-length records stored on disk. It builds on components from earlier assignments, including the Buffer Manager and Storage Manager, and adheres to the required interfaces (record_mgr.h, buffer_mgr.h, storage_mgr.h, etc.). The system supports core functionality such as creating tables, inserting/updating/deleting records, scanning with conditions, and retrieving records using their IDs.

---

## Project Structure

Directory overview:
assign3/
├── Makefile              # Optional: for automating compilation
├── buffer_mgr.c/h        # Buffer management code
├── dberror.c/h           # Error reporting
├── dt.h                  # Data types
├── expr.c/h              # Expression evaluation engine
├── record_mgr.c/h        # Core Record Manager implementation
├── rm_serializer.c/h     # (Optional) Serialization helpers
├── storage_mgr.c/h       # Disk page operations
├── tables.h              # Table and schema definitions
├── test_assign3_1.c      # Main Record Manager test suite
├── test_expr.c           # Expression evaluation tests
├── test_helper.h         # Testing macros/utilities
└── README.md             # This file

### Key Files:
- record_mgr.c/h: Implements main record operations and manages metadata and scans.
- expr.c/h: Provides expression handling for filtering during scans.
- buffer_mgr.c/h: Manages page caching and replacement strategies.
- storage_mgr.c/h: Performs low-level file and page I/O.
- test_assign3_1.c: Validates key Record Manager functionalities.
- test_expr.c: Dedicated test suite for evaluating expressions.

---

## Build & Run

Compile and run using the following commands:
make clean
make
./test_assign3    # Runs Record Manager tests
./test_expr       # Runs expression tests

---

## Implementation Overview

### Table Metadata
- Stored in page 0 (header page).
- Includes:
  - numTuples: Total number of stored records.
  - recordSize: Size of each record.
  - slotsPerPage: Number of records each page can hold.
  - May also store serialized schema information.

### Record Storage (Data Pages)
- Records begin from page 1 onward.
- Uses a slot-based storage format with a bitmap or similar method to track free and used slots.
- Ensures efficient space utilization and quick access.

### Buffer Manager Integration
- All data page operations are routed through the Buffer Manager:
  - pinPage() to bring a page into memory.
  - unpinPage() to release it when done.
  - markDirty() to mark changes.
  - forceFlushPool() to persist updates to disk.

### Record Scanning
- Managed via a ScanManager structure, which tracks:
  - Current page and slot being scanned.
  - Filtering conditions using expressions.
- Filtering logic uses the evalExpr() function to determine if a record meets a given condition.

### Expression Evaluation
- Found in expr.c, primarily under the EXPR_OP logic in evalExpr().
- Ensures the Value *result is always properly initialized before applying operations like boolAnd().

---

## Testing

- Run ./test_assign3 to validate:
  - Table creation and record insertion.
  - Updates and deletions.
  - Scanning with various conditions and schema correctness.

- Run ./test_expr to check:
  - Expression parsing.
  - Evaluation logic and boolean comparisons.

    - `valueEquals()`
    - `boolOr()`
  - Prevents segmentation faults by ensuring `result` is not NULL before use.

This implementation ensures efficient record management, optimized scanning, and safe expression evaluation.
