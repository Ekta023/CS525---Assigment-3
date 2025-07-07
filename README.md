# Record Manager Implementation (CS525 - Assignment 3)

This **Record Manager** is a simple system for handling fixed-length records on disk, using a previously developed **Buffer Manager** and **Storage Manager**. The code conforms to the interfaces specified in the assignment (`record_mgr.h`, `buffer_mgr.h`, `storage_mgr.h`, etc.). It allows creating tables, inserting/deleting/updating records, scanning with conditions, and retrieving data by record ID.

---

## Table of Contents
1. [Project Structure](#project-structure)
2. [Key Files](#key-files)
3. [Build & Run](#build--run)
4. [Testing](#testing)
5. [Implementation Details](#implementation-details)


---

## Project Structure

A typical folder layout:
📁 assign3  
├── Makefile // Optional: For automating builds
├── buffer_mgr.c 
├── buffer_mgr.h 
├── dberror.c 
├── dberror.h 
├── dt.h 
├── expr.c 
├── expr.h 
├── record_mgr.c 
├── record_mgr.h 
├── rm_serializer.c 
├── rm_serializer.h // (if needed) 
├── storage_mgr.c 
├── storage_mgr.h 
├── tables.h 
├── test_assign3_1.c // Main assignment tests (has main()) 
├── test_expr.c // Expression tests (also has main()) 
├── test_helper.h // Helper macros/functions for testing 
└── README.md // This README


- **`record_mgr.c/.h`**: The core Record Manager code (table creation, record insert/delete, scanning).
- **`buffer_mgr.c/.h`**: The Buffer Manager from a previous assignment.
- **`storage_mgr.c/.h`**: The Storage Manager from assignment 1 (reading/writing disk blocks).
- **`expr.c/.h`**: Expression evaluation code used for scanning conditions.
- **`rm_serializer.c`**: Helper functions for serializing table/record data (optional).
- **`test_assign3_1.c`**: Main test suite for the Record Manager (inserts, updates, scans).
- **`test_expr.c`**: Tests specifically for the expression framework.

---

## Key Files

1. **`record_mgr.c`**  
   Implements the public API declared in `record_mgr.h`. Key functionality includes:
   - `createTable`, `openTable`, `closeTable`, `deleteTable`
   - `insertRecord`, `deleteRecord`, `updateRecord`, `getRecord`
   - `startScan`, `next`, `closeScan`
   - Helper methods for record/schema management

2. **`expr.c`**  
   Provides logic for evaluating expressions (e.g., boolean and comparison operators). The Record Manager’s `scan` uses these to filter results.

3. **`buffer_mgr.c`** (from previous assignment)  
   Provides page caching and page replacement strategies. The Record Manager pins/unpins pages via this buffer manager.

4. **`test_assign3_1.c`**  
   The main assignment test suite. Defines `main()` with various sub-tests:
   - Creating a table and inserting data
   - Updating/deleting records
   - Scanning with conditions
   - Verifying data integrity

5. **`test_expr.c`**  
   Tests expression logic separately, also defines `main()`. Must be compiled into a separate executable than the main assignment test.

---

## Build & Run
```
make clean
make
./test_assign3
./test_expr
```

# Implementation Details

## Table Metadata
- Stored on **page 0** (the “header page”).
- Contains important information such as:
  - `numTuples`: Total number of records in the table.
  - `recordSize`: Size of each record.
  - `slotsPerPage`: Number of slots available per page.
- May include **serialized schema data** for attribute definitions.

## Data Pages
- **Start from page 1** onward, storing **fixed-length records**.
- Uses a **slot-based layout**:
  - **Bitmap or array** tracks free and occupied slots.
  - Efficient space management for inserting and deleting records.

## Buffer Manager
- **Handles all page reads/writes** via the following functions:
  - `pinPage()`: Loads a page into memory.
  - `unpinPage()`: Releases a page when done.
  - `markDirty()`: Marks a page for writing back to disk.
  - `forceFlushPool()`: Writes dirty pages to disk.

## Scanning (startScan, next, closeScan)
- A **ScanManager struct** keeps track of:
  - Current **page** and **slot**.
  - **Scan condition** for filtering records.
- Uses `evalExpr()` (from `expr.c`) to evaluate if a record **matches** the condition.

## Expressions
- Expression evaluation is handled in `evalExpr()`, specifically in the **`EXPR_OP` case**.
- Recent fixes ensure that:
  - `Value *result` is **always allocated** before calling functions like:
    - `boolAnd()`
    - `valueEquals()`
    - `boolOr()`
  - Prevents segmentation faults by ensuring `result` is not NULL before use.

This implementation ensures efficient record management, optimized scanning, and safe expression evaluation.
