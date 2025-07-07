#include "storage_mgr.h"
#include "dberror.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// Initializes the storage manager
void initStorageManager(void) {
    // No initialization needed
}

// Creates a new page file initialized with zeros
RC createPageFile(char *fileName) {
    FILE *file = fopen(fileName, "wb");
    if (file == NULL) return RC_FILE_NOT_FOUND;

    char *emptyPage = (char *)calloc(PAGE_SIZE, sizeof(char));
    if (!emptyPage) {
        fclose(file);
        return RC_WRITE_FAILED;
    }

    if (fwrite(emptyPage, sizeof(char), PAGE_SIZE, file) != PAGE_SIZE) {
        free(emptyPage);
        fclose(file);
        return RC_WRITE_FAILED;
    }

    free(emptyPage);
    fclose(file);
    return RC_OK;
}

// Opens an existing page file
RC openPageFile(char *fileName, SM_FileHandle *fHandle) {
    FILE *file = fopen(fileName, "rb+");
    if (file == NULL) {
        return RC_FILE_NOT_FOUND; // Explicit error for missing files
    }

    fseek(file, 0, SEEK_END);
    long fileSize = ftell(file);
    rewind(file);

    if (fileSize % PAGE_SIZE != 0) {
        fclose(file);
        return RC_FILE_HANDLE_NOT_INIT;
    }

    fHandle->fileName = strdup(fileName);
    if (fHandle->fileName == NULL) {
        fclose(file);
        return RC_FILE_HANDLE_NOT_INIT;
    }

    fHandle->totalNumPages = fileSize / PAGE_SIZE;
    fHandle->curPagePos = 0;
    fHandle->mgmtInfo = file;

    return RC_OK;
}

// Closes an open page file
RC closePageFile(SM_FileHandle *fHandle) {
    if (fHandle == NULL || fHandle->mgmtInfo == NULL) 
        return RC_FILE_HANDLE_NOT_INIT;

    FILE *file = (FILE *)fHandle->mgmtInfo;
    if (fclose(file) != 0) return RC_FILE_HANDLE_NOT_INIT;

    free(fHandle->fileName);
    fHandle->fileName = NULL;
    fHandle->mgmtInfo = NULL;
    return RC_OK;
}

// Deletes a page file
RC destroyPageFile(char *fileName) {
    return (remove(fileName) == 0) ? RC_OK : RC_FILE_NOT_FOUND;
}

// Core read function
RC readBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
    if (fHandle == NULL || fHandle->mgmtInfo == NULL)
        return RC_FILE_HANDLE_NOT_INIT;
        
    if (pageNum < 0 || pageNum >= fHandle->totalNumPages)
        return RC_READ_NON_EXISTING_PAGE;

    FILE *file = (FILE *)fHandle->mgmtInfo;
    if (fseek(file, pageNum * PAGE_SIZE, SEEK_SET) != 0)
        return RC_READ_NON_EXISTING_PAGE;

    if (fread(memPage, sizeof(char), PAGE_SIZE, file) != PAGE_SIZE)
        return RC_READ_NON_EXISTING_PAGE;

    fHandle->curPagePos = pageNum;
    return RC_OK;
}

// Relative read operations
RC readFirstBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    return readBlock(0, fHandle, memPage);
}

RC readPreviousBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    if (fHandle->curPagePos <= 0)
        return RC_READ_NON_EXISTING_PAGE;
    return readBlock(fHandle->curPagePos - 1, fHandle, memPage);
}

RC readNextBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    if (fHandle->curPagePos >= fHandle->totalNumPages - 1)
        return RC_READ_NON_EXISTING_PAGE;
    return readBlock(fHandle->curPagePos + 1, fHandle, memPage);
}

RC readCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    return readBlock(fHandle->curPagePos, fHandle, memPage);
}

RC readLastBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    return readBlock(fHandle->totalNumPages - 1, fHandle, memPage);
}

// Core write function
RC writeBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
    if (fHandle == NULL || fHandle->mgmtInfo == NULL || memPage == NULL)
        return RC_FILE_HANDLE_NOT_INIT;

    if (pageNum < 0 || pageNum >= fHandle->totalNumPages)
        return RC_READ_NON_EXISTING_PAGE; // Fixed error code

    FILE *file = (FILE *)fHandle->mgmtInfo;
    if (fseek(file, pageNum * PAGE_SIZE, SEEK_SET) != 0)
        return RC_WRITE_FAILED;

    if (fwrite(memPage, sizeof(char), PAGE_SIZE, file) != PAGE_SIZE)
        return RC_WRITE_FAILED;

    fflush(file);
    fHandle->curPagePos = pageNum;
    return RC_OK;
}

// Append empty block (FIXED)
RC appendEmptyBlock(SM_FileHandle *fHandle) {
    if (fHandle == NULL || fHandle->mgmtInfo == NULL)
        return RC_FILE_HANDLE_NOT_INIT;

    FILE *file = (FILE *)fHandle->mgmtInfo;
    if (fseek(file, 0, SEEK_END) != 0)
        return RC_WRITE_FAILED;

    char *emptyPage = (char *)calloc(PAGE_SIZE, sizeof(char));
    if (!emptyPage) return RC_WRITE_FAILED;

    if (fwrite(emptyPage, sizeof(char), PAGE_SIZE, file) != PAGE_SIZE) {
        free(emptyPage);
        return RC_WRITE_FAILED;
    }

    free(emptyPage);
    fflush(file);
    fHandle->totalNumPages++;
    return RC_OK;
}

// Ensure capacity (FIXED)
RC ensureCapacity(int numberOfPages, SM_FileHandle *fHandle) {
    if (fHandle == NULL || fHandle->mgmtInfo == NULL)
        return RC_FILE_HANDLE_NOT_INIT;

    while (fHandle->totalNumPages < numberOfPages) {
        RC rc = appendEmptyBlock(fHandle);
        if (rc != RC_OK) return rc;
    }
    return RC_OK;
}

// Get current position
int getBlockPos(SM_FileHandle *fHandle) {
    return (fHandle != NULL) ? fHandle->curPagePos : -1;
}