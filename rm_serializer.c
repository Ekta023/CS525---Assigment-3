#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "dberror.h"
#include "tables.h"
#include "record_mgr.h"

// Forward declarations
static RC attrOffset(Schema *schema, int attrNum, int *result);

// ================== Dynamic String Helper ==================
typedef struct VarString {
    char *buf;
    int size;
    int bufsize;
} VarString;

#define MAKE_VARSTRING(var)                \
    do {                                   \
        var = malloc(sizeof(VarString));   \
        var->size = 0;                     \
        var->bufsize = 256;                \
        var->buf = calloc(var->bufsize, 1);\
    } while (0)

#define FREE_VARSTRING(var)                \
    do {                                   \
        free(var->buf);                    \
        free(var);                         \
    } while (0)

#define ENSURE_SIZE(var, newsize)          \
    do {                                   \
        if (var->bufsize <= newsize) {     \
            int newbufsize = var->bufsize * 2; \
            while (newbufsize <= newsize)  \
                newbufsize *= 2;           \
            var->buf = realloc(var->buf, newbufsize); \
            var->bufsize = newbufsize;     \
        }                                  \
    } while (0)

#define APPEND_STRING(var, string)         \
    do {                                   \
        int len = strlen(string);          \
        ENSURE_SIZE(var, var->size + len); \
        memcpy(var->buf + var->size, string, len); \
        var->size += len;                  \
    } while (0)

#define APPEND(var, ...)                   \
    do {                                   \
        char tmp[1024];                    \
        snprintf(tmp, sizeof(tmp), __VA_ARGS__); \
        APPEND_STRING(var, tmp);           \
    } while (0)

// ================== Helper Functions ==================

static RC attrOffset(Schema *schema, int attrNum, int *result) {
    if (attrNum < 0 || attrNum >= schema->numAttr) 
        return RC_RM_INVALID_ATTRIBUTE;
    
    int offset = 0;
    for (int i = 0; i < attrNum; i++) {
        switch (schema->dataTypes[i]) {
            case DT_INT:   offset += sizeof(int); break;
            case DT_FLOAT: offset += sizeof(float); break;
            case DT_BOOL:  offset += sizeof(bool); break;
            case DT_STRING: offset += schema->typeLength[i]; break;
        }
    }
    *result = offset;
    return RC_OK;
}

// ================== Core Serialization Functions ==================

char *serializeTableInfo(RM_TableData *rel) {
    VarString *result;
    MAKE_VARSTRING(result);
    
    APPEND(result, "TABLE <%s>\n", rel->name);
    APPEND(result, "Schema: %s\n", serializeSchema(rel->schema));
    APPEND(result, "Total Tuples: %d\n", getNumTuples(rel));
    
    char *output = strdup(result->buf);
    FREE_VARSTRING(result);
    return output;
}

char *serializeSchema(Schema *schema) {
    VarString *result;
    MAKE_VARSTRING(result);
    
    APPEND(result, "Attributes[%d]: ", schema->numAttr);
    for (int i = 0; i < schema->numAttr; i++) {
        APPEND(result, "%s%s: ", (i > 0) ? ", " : "", schema->attrNames[i]);
        switch (schema->dataTypes[i]) {
            case DT_INT:    APPEND(result, "INT"); break;
            case DT_FLOAT:  APPEND(result, "FLOAT"); break;
            case DT_STRING: APPEND(result, "STRING(%d)", schema->typeLength[i]); break;
            case DT_BOOL:   APPEND(result, "BOOL"); break;
        }
    }
    
    if (schema->keySize > 0) {
        APPEND(result, " | Keys: ");
        for (int i = 0; i < schema->keySize; i++) {
            APPEND(result, "%s%s", 
                  (i > 0) ? ", " : "", 
                  schema->attrNames[schema->keyAttrs[i]]);
        }
    }
    
    char *output = strdup(result->buf);
    FREE_VARSTRING(result);
    return output;
}

char *serializeRecord(Record *record, Schema *schema) {
    VarString *result;
    MAKE_VARSTRING(result);
    
    APPEND(result, "RID(%d,%d) [", record->id.page, record->id.slot);
    for (int i = 0; i < schema->numAttr; i++) {
        char *attrStr = serializeAttr(record, schema, i);
        APPEND(result, "%s%s", (i > 0) ? ", " : "", attrStr);
        free(attrStr);
    }
    APPEND(result, "]");
    
    char *output = strdup(result->buf);
    FREE_VARSTRING(result);
    return output;
}

char *serializeAttr(Record *record, Schema *schema, int attrNum) {
    int offset;
    attrOffset(schema, attrNum, &offset);
    char *attrData = record->data + offset;
    
    VarString *result;
    MAKE_VARSTRING(result);
    
    switch (schema->dataTypes[attrNum]) {
        case DT_INT: {
            int val;
            memcpy(&val, attrData, sizeof(int));
            APPEND(result, "%s: %d", schema->attrNames[attrNum], val);
            break;
        }
        case DT_FLOAT: {
            float val;
            memcpy(&val, attrData, sizeof(float));
            APPEND(result, "%s: %.2f", schema->attrNames[attrNum], val);
            break;
        }
        case DT_STRING: {
            char *str = malloc(schema->typeLength[attrNum] + 1);
            memcpy(str, attrData, schema->typeLength[attrNum]);
            str[schema->typeLength[attrNum]] = '\0';
            APPEND(result, "%s: '%s'", schema->attrNames[attrNum], str);
            free(str);
            break;
        }
        case DT_BOOL: {
            bool val;
            memcpy(&val, attrData, sizeof(bool));
            APPEND(result, "%s: %s", schema->attrNames[attrNum], val ? "true" : "false");
            break;
        }
    }
    
    char *output = strdup(result->buf);
    FREE_VARSTRING(result);
    return output;
}

char *serializeTableContent(RM_TableData *rel) {
    VarString *result;
    MAKE_VARSTRING(result);
    
    RM_ScanHandle *scan = malloc(sizeof(RM_ScanHandle));
    Record *record = malloc(sizeof(Record));
    createRecord(&record, rel->schema);
    
    APPEND(result, "Contents of table %s:\n", rel->name);
    
    startScan(rel, scan, NULL); // Scan all records
    while(next(scan, record) == RC_OK) {
        char *recordStr = serializeRecord(record, rel->schema);
        APPEND(result, "%s\n", recordStr);
        free(recordStr);
    }
    
    closeScan(scan);
    freeRecord(record);
    free(scan);
    
    char *output = strdup(result->buf);
    FREE_VARSTRING(result);
    return output;
}

char *serializeValue(Value *val) {
    VarString *result;
    MAKE_VARSTRING(result);
    
    if (val == NULL) {
        APPEND(result, "(null)");
    } else {
        switch (val->dt) {
            case DT_INT:
                APPEND(result, "%d", val->v.intV);
                break;
            case DT_FLOAT:
                APPEND(result, "%.6f", val->v.floatV);
                break;
            case DT_STRING:
                if (val->v.stringV == NULL)
                    APPEND(result, "(null)");
                else
                    APPEND(result, "%s", val->v.stringV);
                break;
            case DT_BOOL:
                APPEND(result, "%s", val->v.boolV ? "true" : "false");
                break;
        }
    }
    
    char *output = strdup(result->buf);
    FREE_VARSTRING(result);
    return output;
}

Value *stringToValue(char *val) {
    Value *result = malloc(sizeof(Value));
    if (!result) return NULL;
    
    switch (val[0]) {
        case 'i':  // Integer
            result->dt = DT_INT;
            result->v.intV = atoi(val + 1);
            break;
        case 'f':  // Float
            result->dt = DT_FLOAT;
            result->v.floatV = atof(val + 1);
            break;
        case 's':  // String (FIXED)
            result->dt = DT_STRING;
            result->v.stringV = strdup(val + 1);  // Skip prefix and copy
            break;
        case 'b':  // Boolean
            result->dt = DT_BOOL;
            result->v.boolV = (val[1] == 't' || val[1] == '1');
            break;
        default:
            free(result);
            return NULL;
    }
    return result;
}