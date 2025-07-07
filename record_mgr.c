#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include "record_mgr.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "dberror.h"
#include "expr.h"
#include "tables.h"

// Define constants
#define PAGE_SIZE 4096
#define HEADER_PAGE 0
#define DATA_START_PAGE 1

// Record Manager data structures
typedef struct RecordManager {
    BM_BufferPool *bufferPool;
    int numTuples;
} RecordManager;

typedef struct TableMetadata {
    int numTuples;        // Total number of tuples in the table
    int firstFreePage;    // First page with free space
    int numPages;         // Total number of pages in the table
    int recordSize;       // Size of each record
    int slotsPerPage;     // Maximum number of slots in a page
} TableMetadata;

typedef struct ScanManager {
    RID currentRID;      // Current record being scanned
    Expr *condition;     // Scan condition
    bool scanActive;     // Indicates if scan is active
    int currentPage;     // Current page being scanned
    int currentSlot;     // Current slot being scanned
    int totalPages;      // Total pages in the table
    int slotsPerPage;    // Slots per page
} ScanManager;

// Page Layout:
// Each data page has the following structure:
// [SlotBitmap][Record1][Record2]...[RecordN]
// SlotBitmap: Bit array to track occupied slots (1=occupied, 0=free)

// Helper functions for page operations
static int getSlotMapSize(int slotsPerPage) {
    // Calculate size of bitmap in bytes (rounded up to nearest byte)
    return (slotsPerPage + 7) / 8;
}

static int getRecordOffset(int slotNum, int recordSize, int mapSize) {
    // Calculate offset of a record in a page
    return mapSize + (slotNum * recordSize);
}

static bool isSlotOccupied(char *pageData, int slotNum) {
    // Check if a slot is marked as occupied in the bitmap
    int bytePos = slotNum / 8;
    int bitPos = slotNum % 8;
    return (pageData[bytePos] & (1 << bitPos)) != 0;
}

static void markSlotOccupied(char *pageData, int slotNum) {
    // Mark a slot as occupied in the bitmap
    int bytePos = slotNum / 8;
    int bitPos = slotNum % 8;
    pageData[bytePos] |= (1 << bitPos);
}

static void markSlotFree(char *pageData, int slotNum) {
    // Mark a slot as free in the bitmap
    int bytePos = slotNum / 8;
    int bitPos = slotNum % 8;
    pageData[bytePos] &= ~(1 << bitPos);
}

// Helper functions for metadata operations
static RC initializeHeader(BM_BufferPool *bm, Schema *schema, TableMetadata *metadata) {
    BM_PageHandle *pageHandle = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));
    RC pinResult = pinPage(bm, pageHandle, HEADER_PAGE);
    if (pinResult != RC_OK) {
        free(pageHandle);
        return pinResult;
    }
    
    // Write metadata to header page
    memcpy(pageHandle->data, metadata, sizeof(TableMetadata));
    
    // Calculate space needed for schema serialization
    int schemaSize = sizeof(int); // numAttr
    (void)schemaSize;
    char *current = pageHandle->data + sizeof(TableMetadata);
    
    // Write numAttr
    memcpy(current, &schema->numAttr, sizeof(int));
    current += sizeof(int);
    
    // Write attribute info
    for (int i = 0; i < schema->numAttr; i++) {
        // Write attribute name length and name
        int nameLen = strlen(schema->attrNames[i]);
        memcpy(current, &nameLen, sizeof(int));
        current += sizeof(int);
        
        memcpy(current, schema->attrNames[i], nameLen);
        current += nameLen;
        
        // Write data type
        memcpy(current, &schema->dataTypes[i], sizeof(DataType));
        current += sizeof(DataType);
        
        // Write type length
        memcpy(current, &schema->typeLength[i], sizeof(int));
        current += sizeof(int);
    }
    
    // Write key information
    memcpy(current, &schema->keySize, sizeof(int));
    current += sizeof(int);
    
    if (schema->keySize > 0) {
        memcpy(current, schema->keyAttrs, sizeof(int) * schema->keySize);
    }
    
    // Mark page as dirty and unpin
    RC markResult = markDirty(bm, pageHandle);
    if (markResult != RC_OK) {
        unpinPage(bm, pageHandle);
        free(pageHandle);
        return markResult;
    }
    
    // Create first data page
    RC forcePage = unpinPage(bm, pageHandle);
    if (forcePage != RC_OK) {
        free(pageHandle);
        return forcePage;
    }
    
    // Initialize first data page
    pinResult = pinPage(bm, pageHandle, DATA_START_PAGE);
    if (pinResult != RC_OK) {
        free(pageHandle);
        return pinResult;
    }
    
    // Clear the page data (all slots free)
    memset(pageHandle->data, 0, PAGE_SIZE);
    
    markResult = markDirty(bm, pageHandle);
    if (markResult != RC_OK) {
        unpinPage(bm, pageHandle);
        free(pageHandle);
        return markResult;
    }
    
    RC unpinResult = unpinPage(bm, pageHandle);
    free(pageHandle);
    return unpinResult;
}

static RC readHeader(BM_BufferPool *bm, TableMetadata *metadata) {
    BM_PageHandle *pageHandle = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));
    RC pinResult = pinPage(bm, pageHandle, HEADER_PAGE);
    if (pinResult != RC_OK) {
        free(pageHandle);
        return pinResult;
    }
    
    // Read metadata from header page
    memcpy(metadata, pageHandle->data, sizeof(TableMetadata));
    
    RC unpinResult = unpinPage(bm, pageHandle);
    free(pageHandle);
    return unpinResult;
}

static RC writeHeader(BM_BufferPool *bm, TableMetadata *metadata) {
    BM_PageHandle *pageHandle = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));
    RC pinResult = pinPage(bm, pageHandle, HEADER_PAGE);
    if (pinResult != RC_OK) {
        free(pageHandle);
        return pinResult;
    }
    
    // Write metadata to header page
    memcpy(pageHandle->data, metadata, sizeof(TableMetadata));
    
    // Mark page as dirty
    RC markResult = markDirty(bm, pageHandle);
    if (markResult != RC_OK) {
        unpinPage(bm, pageHandle);
        free(pageHandle);
        return markResult;
    }
    
    RC unpinResult = unpinPage(bm, pageHandle);
    free(pageHandle);
    return unpinResult;
}

static RC findFreeSlot(BM_BufferPool *bm, TableMetadata *metadata, RID *rid) {
    BM_PageHandle *pageHandle = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));
    int currentPage = metadata->firstFreePage;
    bool found = false;
    
    // Search for a free slot starting from firstFreePage
    while (currentPage < metadata->numPages && !found) {
        RC pinResult = pinPage(bm, pageHandle, currentPage);
        if (pinResult != RC_OK) {
            free(pageHandle);
            return pinResult;
        }
        
        // Check each slot in the page
        for (int i = 0; i < metadata->slotsPerPage; i++) {
            if (!isSlotOccupied(pageHandle->data, i)) {
                // Found a free slot
                rid->page = currentPage;
                rid->slot = i;
                found = true;
                break;
            }
        }
        
        RC unpinResult = unpinPage(bm, pageHandle);
        if (unpinResult != RC_OK) {
            free(pageHandle);
            return unpinResult;
        }
        
        if (!found) {
            currentPage++;
        }
    }
    
    // If no free slot found, create a new page
    if (!found) {
        // Ensure capacity for the new page
        SM_FileHandle fileHandle;
        char *fileName = bm->pageFile;
        
        // Open page file
        RC openResult = openPageFile(fileName, &fileHandle);
        if (openResult != RC_OK) {
            free(pageHandle);
            return openResult;
        }
        
        // Append empty block
        RC appendResult = appendEmptyBlock(&fileHandle);
        if (appendResult != RC_OK) {
            closePageFile(&fileHandle);
            free(pageHandle);
            return appendResult;
        }
        
        closePageFile(&fileHandle);
        
        // Update numPages
        currentPage = metadata->numPages;
        metadata->numPages++;
        
        // Initialize new page
        RC pinResult = pinPage(bm, pageHandle, currentPage);
        if (pinResult != RC_OK) {
            free(pageHandle);
            return pinResult;
        }
        
        // Clear the page data (all slots free)
        memset(pageHandle->data, 0, PAGE_SIZE);
        
        // Mark page as dirty
        RC markResult = markDirty(bm, pageHandle);
        if (markResult != RC_OK) {
            unpinPage(bm, pageHandle);
            free(pageHandle);
            return markResult;
        }
        
        RC unpinResult = unpinPage(bm, pageHandle);
        if (unpinResult != RC_OK) {
            free(pageHandle);
            return unpinResult;
        }
        
        // Set first slot in new page
        rid->page = currentPage;
        rid->slot = 0;
        
        // Update metadata
        metadata->firstFreePage = currentPage;
        writeHeader(bm, metadata);
    }
    
    free(pageHandle);
    return RC_OK;
}

// Initialize Record Manager
RC initRecordManager(void *mgmtData) {
    // Initialize storage manager
    (void)mgmtData; // Suppress unused parameter warning
    initStorageManager();
    return RC_OK;
}

// Shutdown Record Manager
RC shutdownRecordManager() {
    // Clean up any global resources if needed
    return RC_OK;
}

// Create a new table in a page file
RC createTable(char *name, Schema *schema) {
    // Create page file using storage manager
    RC result = createPageFile(name);
    if (result != RC_OK) {
        return result;
    }
    
    // Initialize buffer pool
    BM_BufferPool *bm = (BM_BufferPool *)malloc(sizeof(BM_BufferPool));
    RC initResult = initBufferPool(bm, name, 10000, RS_LRU, NULL);
    if (initResult != RC_OK) {
        free(bm);
        return initResult;
    }
    
    // Calculate record size and slots per page
    int recordSize = getRecordSize(schema);
    int slotsPerPage;
    int slotMapSize;

    // Iterate to find the maximum slotsPerPage that fits in PAGE_SIZE
    slotsPerPage = PAGE_SIZE / recordSize; // Initial maximum possible
    do {
        slotMapSize = (slotsPerPage + 7) / 8; // Slot map size in bytes
        if (slotMapSize + (slotsPerPage * recordSize) <= PAGE_SIZE) {
            break;
        }
        slotsPerPage--;
    } while (slotsPerPage > 0);

    // Check if valid slotsPerPage was found
    if (slotsPerPage <= 0) {
        return RC_INVALID_RECORD_SIZE; // Handle error appropriately
    }

    // Initialize table metadata with the correct values
    TableMetadata metadata;
    metadata.numTuples = 0;
    metadata.firstFreePage = DATA_START_PAGE;
    metadata.numPages = DATA_START_PAGE + 1; // Header page + first data page
    metadata.recordSize = recordSize;
    metadata.slotsPerPage = slotsPerPage;
    
    // Write metadata and schema to header page
    RC headerResult = initializeHeader(bm, schema, &metadata);
    if (headerResult != RC_OK) {
        shutdownBufferPool(bm);
        free(bm);
        return headerResult;
    }
    
    // Shutdown buffer pool
    RC shutdownResult = shutdownBufferPool(bm);
    if (shutdownResult != RC_OK) {
        free(bm);
        return shutdownResult;
    }
    
    free(bm);
    return RC_OK;
}

// Open an existing table
RC openTable(RM_TableData *rel, char *name) {
    // Initialize buffer pool
    BM_BufferPool *bm = (BM_BufferPool *)malloc(sizeof(BM_BufferPool));
    RC initResult = initBufferPool(bm, name, 10000, RS_LRU, NULL);
    if (initResult != RC_OK) {
        free(bm);
        return initResult;
    }
    
    // Initialize table metadata
    TableMetadata *metadata = (TableMetadata *)malloc(sizeof(TableMetadata));
    RC metadataResult = readHeader(bm, metadata);
    if (metadataResult != RC_OK) {
        shutdownBufferPool(bm);
        free(bm);
        free(metadata);
        return metadataResult;
    }
    
    // Read schema from header page
    BM_PageHandle *pageHandle = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));
    RC pinResult = pinPage(bm, pageHandle, HEADER_PAGE);
    if (pinResult != RC_OK) {
        shutdownBufferPool(bm);
        free(bm);
        free(metadata);
        free(pageHandle);
        return pinResult;
    }
    
    // Skip metadata at beginning of header page
    char *schemaData = pageHandle->data + sizeof(TableMetadata);
    
    // Read schema fields
    int numAttr, keySize;
    memcpy(&numAttr, schemaData, sizeof(int));
    schemaData += sizeof(int);
    
    // Allocate arrays for schema info
    char **attrNames = (char **)malloc(sizeof(char *) * numAttr);
    DataType *dataTypes = (DataType *)malloc(sizeof(DataType) * numAttr);
    int *typeLength = (int *)malloc(sizeof(int) * numAttr);
    
    // Read attribute information
    for (int i = 0; i < numAttr; i++) {
        // Read attribute name (null-terminated string)
        int nameLength;
        memcpy(&nameLength, schemaData, sizeof(int));
        schemaData += sizeof(int);
        
        attrNames[i] = (char *)malloc(nameLength + 1);
        memcpy(attrNames[i], schemaData, nameLength);
        attrNames[i][nameLength] = '\0';
        schemaData += nameLength;
        
        // Read data type
        memcpy(&dataTypes[i], schemaData, sizeof(DataType));
        schemaData += sizeof(DataType);
        
        // Read type length
        memcpy(&typeLength[i], schemaData, sizeof(int));
        schemaData += sizeof(int);
    }
    
    // Read key information
    memcpy(&keySize, schemaData, sizeof(int));
    schemaData += sizeof(int);
    
    int *keyAttrs = (int *)malloc(sizeof(int) * keySize);
    memcpy(keyAttrs, schemaData, sizeof(int) * keySize);
    
    // Create schema
    Schema *schema = createSchema(numAttr, attrNames, dataTypes, typeLength, keySize, keyAttrs);
    
    // Set table data
    rel->name = strdup(name);
    rel->schema = schema;
    
    // Create record manager instance
    RecordManager *mgr = (RecordManager *)malloc(sizeof(RecordManager));
    mgr->bufferPool = bm;
    mgr->numTuples = metadata->numTuples;
    
    rel->mgmtData = mgr;
    
    // Clean up
    RC unpinResult = unpinPage(bm, pageHandle);
    free(pageHandle);
    free(metadata);
    
    return unpinResult;
}

// Close a table
RC closeTable(RM_TableData *rel) {
    if (rel == NULL || rel->mgmtData == NULL) {
        return RC_FILE_HANDLE_NOT_INIT;
    }
    
    RecordManager *mgr = (RecordManager *)rel->mgmtData;
    BM_BufferPool *bm = mgr->bufferPool;
    
    if (bm == NULL) {
        //free(mgr);
        if (rel->schema != NULL) {
            //freeSchema(rel->schema);
        }
        if (rel->name != NULL) {
           // free(rel->name);
        }
        rel->mgmtData = NULL;
        return RC_OK;
    }
    
    // Force all dirty pages to disk
    RC forceResult = forceFlushPool(bm);
    if (forceResult != RC_OK) {
        // Even if force flush fails, we should still try to clean up resources
        free(bm);
        free(mgr);
        if (rel->schema != NULL) {
            freeSchema(rel->schema);
        }
        if (rel->name != NULL) {
            free(rel->name);
        }
        rel->mgmtData = NULL;
        return forceResult;
    }
    
    // Shutdown buffer pool
    RC shutdownResult = shutdownBufferPool(bm);
    
    // Clean up resources
    free(bm);
    free(mgr);
    
    if (rel->schema != NULL) {
        freeSchema(rel->schema);
    }
    
    if (rel->name != NULL) {
        free(rel->name);
    }
    
    rel->mgmtData = NULL;
    rel->schema = NULL;
    rel->name = NULL;
    
    return shutdownResult;
}

// Delete a table
RC deleteTable(char *name) {
    // Use storage manager to destroy page file
    return destroyPageFile(name);
}

// Get number of tuples in a table
int getNumTuples(RM_TableData *rel) {
    if (rel == NULL || rel->mgmtData == NULL) {
        return 0;
    }
    
    RecordManager *mgr = (RecordManager *)rel->mgmtData;
    return mgr->numTuples;
}

// Insert a record in a table
RC insertRecord(RM_TableData *rel, Record *record) {
    if (rel == NULL || rel->mgmtData == NULL || record == NULL) {
        return RC_FILE_HANDLE_NOT_INIT;
    }
    
    RecordManager *mgr = (RecordManager *)rel->mgmtData;
    BM_BufferPool *bm = mgr->bufferPool;
    
    // Read table metadata
    TableMetadata metadata;
    RC metadataResult = readHeader(bm, &metadata);
    if (metadataResult != RC_OK) {
        return metadataResult;
    }
    
    // Find a free slot for the record
    RID rid;
    RC slotResult = findFreeSlot(bm, &metadata, &rid);
    if (slotResult != RC_OK) {
        return slotResult;
    }
    
    // Set the record ID
    record->id = rid;
    
    // Get page to insert the record
    BM_PageHandle *pageHandle = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));
    RC pinResult = pinPage(bm, pageHandle, rid.page);
    if (pinResult != RC_OK) {
        free(pageHandle);
        return pinResult;
    }
    
    // Calculate slot map size and record offset
    int mapSize = getSlotMapSize(metadata.slotsPerPage);
    int offset = getRecordOffset(rid.slot, metadata.recordSize, mapSize);
    
    // Mark slot as occupied
    markSlotOccupied(pageHandle->data, rid.slot);
    
    // Copy record data to page
    memcpy(pageHandle->data + offset, record->data, metadata.recordSize);
    
    // Mark page as dirty
    RC markResult = markDirty(bm, pageHandle);
    if (markResult != RC_OK) {
        unpinPage(bm, pageHandle);
        free(pageHandle);
        return markResult;
    }
    
    // Unpin page
    RC unpinResult = unpinPage(bm, pageHandle);
    if (unpinResult != RC_OK) {
        free(pageHandle);
        return unpinResult;
    }
    
    free(pageHandle);
    
    // Update tuple count
    mgr->numTuples++;
    metadata.numTuples = mgr->numTuples;
    
    // Update metadata
    return writeHeader(bm, &metadata);
}

// Delete a record from a table
RC deleteRecord(RM_TableData *rel, RID id) {
    if (rel == NULL || rel->mgmtData == NULL) {
        return RC_FILE_HANDLE_NOT_INIT;
    }
    
    RecordManager *mgr = (RecordManager *)rel->mgmtData;
    BM_BufferPool *bm = mgr->bufferPool;
    
    // Read table metadata
    TableMetadata metadata;
    RC metadataResult = readHeader(bm, &metadata);
    if (metadataResult != RC_OK) {
        return metadataResult;
    }
    
    // Get page containing the record
    BM_PageHandle *pageHandle = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));
    RC pinResult = pinPage(bm, pageHandle, id.page);
    if (pinResult != RC_OK) {
        free(pageHandle);
        return pinResult;
    }
    
    // Check if slot is occupied
    if (!isSlotOccupied(pageHandle->data, id.slot)) {
        unpinPage(bm, pageHandle);
        free(pageHandle);
        return RC_RM_NO_MORE_TUPLES;
    }
    
    // Mark slot as free
    markSlotFree(pageHandle->data, id.slot);
    
    // Mark page as dirty
    RC markResult = markDirty(bm, pageHandle);
    if (markResult != RC_OK) {
        unpinPage(bm, pageHandle);
        free(pageHandle);
        return markResult;
    }
    
    // Unpin page
    RC unpinResult = unpinPage(bm, pageHandle);
    if (unpinResult != RC_OK) {
        free(pageHandle);
        return unpinResult;
    }
    
    free(pageHandle);
    
    // Update tuple count
    mgr->numTuples--;
    metadata.numTuples = mgr->numTuples;
    
    // Update metadata
    return writeHeader(bm, &metadata);
}

// Update a record in a table
RC updateRecord(RM_TableData *rel, Record *record) {
    if (rel == NULL || rel->mgmtData == NULL || record == NULL) {
        return RC_FILE_HANDLE_NOT_INIT;
    }
    
    RecordManager *mgr = (RecordManager *)rel->mgmtData;
    BM_BufferPool *bm = mgr->bufferPool;
    
    // Read table metadata
    TableMetadata metadata;
    RC metadataResult = readHeader(bm, &metadata);
    if (metadataResult != RC_OK) {
        return metadataResult;
    }
    
    // Get page containing the record
    BM_PageHandle *pageHandle = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));
    RC pinResult = pinPage(bm, pageHandle, record->id.page);
    if (pinResult != RC_OK) {
        free(pageHandle);
        return pinResult;
    }
    
    // Check if slot is occupied
    if (!isSlotOccupied(pageHandle->data, record->id.slot)) {
        unpinPage(bm, pageHandle);
        free(pageHandle);
        return RC_RM_NO_MORE_TUPLES;
    }
    
    // Calculate record offset
    int mapSize = getSlotMapSize(metadata.slotsPerPage);
    int offset = getRecordOffset(record->id.slot, metadata.recordSize, mapSize);
    
    // Update record data
    memcpy(pageHandle->data + offset, record->data, metadata.recordSize);
    
    // Mark page as dirty
    RC markResult = markDirty(bm, pageHandle);
    if (markResult != RC_OK) {
        unpinPage(bm, pageHandle);
        free(pageHandle);
        return markResult;
    }
    
    // Unpin page
    RC unpinResult = unpinPage(bm, pageHandle);
    if (unpinResult != RC_OK) {
        free(pageHandle);
        return unpinResult;
    }
    
    free(pageHandle);
    return RC_OK;
}

// Get a record from a table
RC getRecord(RM_TableData *rel, RID id, Record *record) {
    if (rel == NULL || rel->mgmtData == NULL || record == NULL) {
        return RC_FILE_HANDLE_NOT_INIT;
    }
    
    RecordManager *mgr = (RecordManager *)rel->mgmtData;
    BM_BufferPool *bm = mgr->bufferPool;
    
    // Read table metadata
    TableMetadata metadata;
    RC metadataResult = readHeader(bm, &metadata);
    if (metadataResult != RC_OK) {
        return metadataResult;
    }
    
    // Get page containing the record
    BM_PageHandle *pageHandle = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));
    RC pinResult = pinPage(bm, pageHandle, id.page);
    if (pinResult != RC_OK) {
        free(pageHandle);
        return pinResult;
    }
    
    // Check if slot is occupied
    if (!isSlotOccupied(pageHandle->data, id.slot)) {
        unpinPage(bm, pageHandle);
        free(pageHandle);
        return RC_RM_NO_MORE_TUPLES;
    }
    
    // Calculate record offset
    int mapSize = getSlotMapSize(metadata.slotsPerPage);
    int offset = getRecordOffset(id.slot, metadata.recordSize, mapSize);
    
    // Set record ID
    record->id = id;
    
    // Allocate memory for record data if needed
    if (record->data == NULL) {
        record->data = (char *)malloc(metadata.recordSize);
    }
    
    // Copy record data
    memcpy(record->data, pageHandle->data + offset, metadata.recordSize);
    
    // Unpin page
    RC unpinResult = unpinPage(bm, pageHandle);
    if (unpinResult != RC_OK) {
        free(pageHandle);
        return unpinResult;
    }
    
    free(pageHandle);
    return RC_OK;
}

// Start a scan operation
RC startScan(RM_TableData *rel, RM_ScanHandle *scan, Expr *cond) {
    if (rel == NULL || rel->mgmtData == NULL || scan == NULL) {
        return RC_FILE_HANDLE_NOT_INIT;
    }
    
    RecordManager *mgr = (RecordManager *)rel->mgmtData;
    BM_BufferPool *bm = mgr->bufferPool;
    
    // Read table metadata
    TableMetadata metadata;
    RC metadataResult = readHeader(bm, &metadata);
    if (metadataResult != RC_OK) {
        return metadataResult;
    }
    
    // Initialize scan manager
    ScanManager *scanMgr = (ScanManager *)malloc(sizeof(ScanManager));
    scanMgr->condition = cond;
    scanMgr->scanActive = true;
    scanMgr->currentPage = DATA_START_PAGE;  // Start scan from first data page
    scanMgr->currentSlot = 0;                // Start from first slot
    scanMgr->totalPages = metadata.numPages;
    scanMgr->slotsPerPage = metadata.slotsPerPage;
    
    // Initialize scan handle
    scan->rel = rel;
    scan->mgmtData = scanMgr;
    
    return RC_OK;
}

// Get next record that satisfies the scan condition
RC next(RM_ScanHandle *scan, Record *record) {
    if (scan == NULL || scan->mgmtData == NULL || record == NULL) {
        return RC_FILE_HANDLE_NOT_INIT;
    }
    
    ScanManager *scanMgr = (ScanManager *)scan->mgmtData;
    
    // Check if scan is active
    if (!scanMgr->scanActive) {
        return RC_RM_NO_MORE_TUPLES;
    }
    
    RecordManager *mgr = (RecordManager *)scan->rel->mgmtData;
    BM_BufferPool *bm = mgr->bufferPool;
    
    // Read table metadata
    TableMetadata metadata;
    RC metadataResult = readHeader(bm, &metadata);
    if (metadataResult != RC_OK) {
        return metadataResult;
    }
    
    BM_PageHandle *pageHandle = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));
    if (pageHandle == NULL) {
        return RC_MEM_ALLOC_FAILED;
    }
    
    RC pinResult;
    bool found = false;
    
    // Scan through pages and slots
    while (scanMgr->currentPage < scanMgr->totalPages) {
        // Pin current page
        pinResult = pinPage(bm, pageHandle, scanMgr->currentPage);
        if (pinResult != RC_OK) {
            free(pageHandle);
            return pinResult;
        }
        
        // Scan through slots in current page
        while (scanMgr->currentSlot < scanMgr->slotsPerPage) {
            // Check if slot is occupied
            if (isSlotOccupied(pageHandle->data, scanMgr->currentSlot)) {
                // Set up RID for the current record
                RID rid;
                rid.page = scanMgr->currentPage;
                rid.slot = scanMgr->currentSlot;
                
                // Move to next slot for next iteration
                scanMgr->currentSlot++;
                
                // Get the record
                record->id = rid;
                int mapSize = getSlotMapSize(metadata.slotsPerPage);
                int offset = getRecordOffset(rid.slot, metadata.recordSize, mapSize);
                
                // Allocate memory for record data if needed
                if (record->data == NULL) {
                    record->data = (char *)malloc(metadata.recordSize);
                    if (record->data == NULL) {
                        unpinPage(bm, pageHandle);
                        free(pageHandle);
                        return RC_MEM_ALLOC_FAILED;
                    }
                }
                
                // Copy record data
                memcpy(record->data, pageHandle->data + offset, metadata.recordSize);
                
                // Check condition if present
                if (scanMgr->condition != NULL) {
                    Value *result = NULL;
                    RC evalResult = evalExpr(record, scan->rel->schema, scanMgr->condition, &result);
                    if (evalResult != RC_OK) {
                        unpinPage(bm, pageHandle);
                        free(pageHandle);
                        return evalResult;
                    }
                    
                    if (result != NULL && result->v.boolV) {
                        found = true;
                        freeVal(result);
                        break;
                    }
                    
                    if (result != NULL) {
                        freeVal(result);
                    }
                } else {
                    // No condition means all records match
                    found = true;
                    break;
                }
            } else {
                // Skip empty slot
                scanMgr->currentSlot++;
            }
        }
        
        // Unpin current page before moving to next page or returning
        RC unpinResult = unpinPage(bm, pageHandle);
        if (unpinResult != RC_OK) {
            free(pageHandle);
            return unpinResult;
        }
        
        if (found) {
            break;
        }
        
        // Move to next page
        scanMgr->currentPage++;
        scanMgr->currentSlot = 0;
    }
    
    free(pageHandle);
    
    // If no more matching records
    if (!found) {
        scanMgr->scanActive = false;
        return RC_RM_NO_MORE_TUPLES;
    }
    
    return RC_OK;
}

// Close a scan operation
RC closeScan(RM_ScanHandle *scan) {
    if (scan == NULL || scan->mgmtData == NULL) {
        return RC_FILE_HANDLE_NOT_INIT;
    }
    
    // Free scan manager
    ScanManager *scanMgr = (ScanManager *)scan->mgmtData;
    free(scanMgr);
    scan->mgmtData = NULL;
    
    return RC_OK;
}

// Get the size of a record for a given schema
int getRecordSize(Schema *schema) {
    int size = 0;
    int i;
    
    for (i = 0; i < schema->numAttr; i++) {
        switch (schema->dataTypes[i]) {
            case DT_INT:
                size += sizeof(int);
                break;
            case DT_FLOAT:
                size += sizeof(float);
                break;
            case DT_BOOL:
                size += sizeof(bool);
                break;
            case DT_STRING:
                size += schema->typeLength[i];
                break;
        }
    }
    
    return size;
}

// Create a schema
Schema *createSchema(int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys) {
    Schema *schema = (Schema *)malloc(sizeof(Schema));
    
    schema->numAttr = numAttr;
    schema->attrNames = attrNames;
    schema->dataTypes = dataTypes;
    schema->typeLength = typeLength;
    schema->keySize = keySize;
    schema->keyAttrs = keys;
    
    return schema;
}

// Free schema resources
RC freeSchema(Schema *schema) {
    if (schema == NULL) {
        return RC_OK;
    }
    
    // Free attribute names
    for (int i = 0; i < schema->numAttr; i++) {
        free(schema->attrNames[i]);
    }
    
    free(schema->attrNames);
    free(schema->dataTypes);
    free(schema->typeLength);
    free(schema->keyAttrs);
    free(schema);
    
    return RC_OK;
}

// Create a new record
RC createRecord(Record **record, Schema *schema) {
    *record = (Record *)malloc(sizeof(Record));
    if (*record == NULL) {
        return RC_MEM_ALLOC_FAILED;
    }
    
    (*record)->id.page = -1;
    (*record)->id.slot = -1;
    
    int recordSize = getRecordSize(schema);
    (*record)->data = (char *)calloc(recordSize, sizeof(char));
    if ((*record)->data == NULL) {
        free(*record);
        return RC_MEM_ALLOC_FAILED;
    }
    
    return RC_OK;
}

// Free record resources
RC freeRecord(Record *record) {
    if (record == NULL) {
        return RC_OK;
    }
    
    free(record->data);
    free(record);
    
    return RC_OK;
}

// Get attribute value from a record
RC getAttr(const Record *record, Schema *schema, int attrNum, Value **value) {
    if (record == NULL || schema == NULL || attrNum < 0 || attrNum >= schema->numAttr) {
        return RC_RM_NO_MORE_TUPLES;
    }
    
    // Always allocate a new Value object
    *value = (Value *)malloc(sizeof(Value));
    if (*value == NULL) {
        return RC_MEM_ALLOC_FAILED;
    }
    
    // Initialize string pointer to NULL
    if (schema->dataTypes[attrNum] == DT_STRING) {
        (*value)->v.stringV = NULL;
    }
    
    // Set value data type
    (*value)->dt = schema->dataTypes[attrNum];
    
    // Calculate offset for the attribute in the record
    int offset = 0;
    for (int i = 0; i < attrNum; i++) {
        switch (schema->dataTypes[i]) {
            case DT_INT:
                offset += sizeof(int);
                break;
            case DT_FLOAT:
                offset += sizeof(float);
                break;
            case DT_BOOL:
                offset += sizeof(bool);
                break;
            case DT_STRING:
                offset += schema->typeLength[i];
                break;
        }
    }
    
    // Extract value based on data type
    switch (schema->dataTypes[attrNum]) {
        case DT_INT: {
            int intVal;
            memcpy(&intVal, record->data + offset, sizeof(int));
            (*value)->v.intV = intVal;
            break;
        }
        case DT_FLOAT: {
            float floatVal;
            memcpy(&floatVal, record->data + offset, sizeof(float));
            (*value)->v.floatV = floatVal;
            break;
        }
        case DT_BOOL: {
            bool boolVal;
            memcpy(&boolVal, record->data + offset, sizeof(bool));
            (*value)->v.boolV = boolVal;
            break;
        }
        case DT_STRING: {
            // For string type, we need to allocate memory for the string
            int strLen = schema->typeLength[attrNum];
            (*value)->v.stringV = (char *)malloc(strLen + 1);
            if ((*value)->v.stringV == NULL) {
                free(*value);
                *value = NULL;
                return RC_MEM_ALLOC_FAILED;
            }
            
            // Copy string data
            memcpy((*value)->v.stringV, record->data + offset, strLen);
            (*value)->v.stringV[strLen] = '\0';
            break;
        }
    }
    
    return RC_OK;
}

// Set attribute value in a record
RC setAttr(Record *record, Schema *schema, int attrNum, Value *value) {
    if (record == NULL || schema == NULL || value == NULL || attrNum < 0 || attrNum >= schema->numAttr) {
        return RC_RM_NO_MORE_TUPLES;
    }
    
    // Check if value type matches schema type
    if (value->dt != schema->dataTypes[attrNum]) {
        return RC_RM_COMPARE_VALUE_OF_DIFFERENT_DATATYPE;
    }
    
    // Calculate offset for the attribute in the record
    int offset = 0;
    for (int i = 0; i < attrNum; i++) {
        switch (schema->dataTypes[i]) {
            case DT_INT:
                offset += sizeof(int);
                break;
            case DT_FLOAT:
                offset += sizeof(float);
                break;
            case DT_BOOL:
                offset += sizeof(bool);
                break;
            case DT_STRING:
                offset += schema->typeLength[i];
                break;
        }
    }
    
    // Set value based on data type
    switch (schema->dataTypes[attrNum]) {
        case DT_INT:
            memcpy(record->data + offset, &(value->v.intV), sizeof(int));
            break;
        case DT_FLOAT:
            memcpy(record->data + offset, &(value->v.floatV), sizeof(float));
            break;
        case DT_BOOL:
            memcpy(record->data + offset, &(value->v.boolV), sizeof(bool));
            break;
        case DT_STRING:
            {
                // Declare variables at start of block
                int len = strlen(value->v.stringV);
                int maxLen = schema->typeLength[attrNum];
                
                // Rest of the code
                if (len > maxLen) {
                    len = maxLen;
                }
                
                memcpy(record->data + offset, value->v.stringV, len);
                
                // Pad with nulls if string is shorter than allocated space
                if (len < maxLen) {
                    memset(record->data + offset + len, 0, maxLen - len);
                }
            }
            break;
    }
    
    return RC_OK;
}