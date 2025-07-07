#ifndef DBERROR_H
#define DBERROR_H

#include <stdio.h>

#define PAGE_SIZE 4096

typedef int RC;

// Base error codes
#define RC_OK 0
#define RC_FILE_NOT_FOUND 1
#define RC_FILE_HANDLE_NOT_INIT 2
#define RC_WRITE_FAILED 3
#define RC_READ_NON_EXISTING_PAGE 4
#define RC_MEM_ALLOC_FAILED 5
#define RC_PINNED_PAGES_IN_BUFFER 6
#define RC_UNKNOWN_STRATEGY 7
#define RC_NO_FREE_BUFFER_SLOTS 8    // Typo: Rename to match your code (if needed)
#define RC_PAGE_NOT_PINNED 9
#define RC_PAGE_NOT_FOUND 10

// Add the missing error codes here
#define RC_NO_FREE_BUFFER_SLOT 11    // Error when no buffer slot is free
#define RC_PAGE_NOT_IN_BUFFER 12     // Page not found in buffer
#define RC_INVALID_UNPIN 13          // Invalid unpin operation
#define RC_INVALID_RECORD_SIZE 400  // Custom error for memory allocation failure

#define RC_RM_COMPARE_VALUE_OF_DIFFERENT_DATATYPE 200
#define RC_RM_EXPR_RESULT_IS_NOT_BOOLEAN 201
#define RC_RM_BOOLEAN_EXPR_ARG_IS_NOT_BOOLEAN 202
#define RC_RM_NO_MORE_TUPLES 203
#define RC_RM_NO_PRINT_FOR_DATATYPE 204
#define RC_RM_UNKOWN_DATATYPE 205
#define RC_RM_UNKNOWN_OPERATOR 206
#define RC_RM_INVALID_ATTRIBUTE 207
#define RC_RM_INVALID_RECORD 208
#define RC_RM_INVALID_SLOT 209
#define RC_IM_KEY_NOT_FOUND 300
#define RC_IM_KEY_ALREADY_EXISTS 301
#define RC_IM_N_TO_LAGE 302
#define RC_IM_NO_MORE_ENTRIES 303

extern char *RC_message;
extern void printError(RC error);
extern char *errorMessage(RC error);

#define THROW(rc, message) { RC_message = message; return rc; }

#define CHECK(code) { \
    RC rc_internal = (code); \
    if (rc_internal != RC_OK) { \
        char *message = errorMessage(rc_internal); \
        printf("[%s-L%i-%s] ERROR: %s\n", __FILE__, __LINE__, __TIME__, message); \
        free(message); \
        exit(1); \
    } \
}

#endif