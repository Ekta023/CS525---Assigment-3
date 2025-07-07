#include <string.h>
#include <stdlib.h>
#include "dberror.h"
#include "record_mgr.h"
#include "expr.h"
#include "tables.h"

// Error code check macro
#define EXPR_CHECK(expr) { RC rc = (expr); if (rc != RC_OK) return rc; }//modified by mukul to remove the error of redeclaration of rc

// ================== Value Comparisons ==================
RC valueEquals(Value *left, Value *right, Value *result) {
    if (left->dt != right->dt)
        THROW(RC_RM_COMPARE_VALUE_OF_DIFFERENT_DATATYPE, 
             "Cannot compare different datatypes");

    result->dt = DT_BOOL;
    switch (left->dt) {
        case DT_INT: 
            result->v.boolV = (left->v.intV == right->v.intV);
            break;
        case DT_FLOAT:
            result->v.boolV = (left->v.floatV == right->v.floatV);
            break;
        case DT_BOOL:
            result->v.boolV = (left->v.boolV == right->v.boolV);
            break;
        case DT_STRING:
            result->v.boolV = (strcmp(left->v.stringV, right->v.stringV) == 0);
            break;
    }
    return RC_OK;
}

RC valueSmaller(Value *left, Value *right, Value *result) {
    if (left->dt != right->dt)
        THROW(RC_RM_COMPARE_VALUE_OF_DIFFERENT_DATATYPE, 
             "Cannot compare different datatypes");

    result->dt = DT_BOOL;
    switch (left->dt) {
        case DT_INT: 
            result->v.boolV = (left->v.intV < right->v.intV);
            break;
        case DT_FLOAT:
            result->v.boolV = (left->v.floatV < right->v.floatV);
            break;
        case DT_BOOL:
            // Note: false (0) is considered "smaller" than true (1)
            result->v.boolV = (left->v.boolV < right->v.boolV);
            break;
        case DT_STRING:
            result->v.boolV = (strcmp(left->v.stringV, right->v.stringV) < 0);
            break;
    }
    return RC_OK;
}

// ================== Boolean Operations ==================
RC boolNot(Value *input, Value *result) {
    if (input->dt != DT_BOOL)
        THROW(RC_RM_BOOLEAN_EXPR_ARG_IS_NOT_BOOLEAN, 
             "NOT operator requires boolean input");
    
    result->dt = DT_BOOL;
    result->v.boolV = !input->v.boolV;
    return RC_OK;
}

RC boolAnd(Value *left, Value *right, Value *result) {
    if (left->dt != DT_BOOL || right->dt != DT_BOOL)
        THROW(RC_RM_BOOLEAN_EXPR_ARG_IS_NOT_BOOLEAN, 
             "AND operator requires boolean inputs");
    
    result->dt = DT_BOOL;
    result->v.boolV = (left->v.boolV && right->v.boolV);
    return RC_OK;
}

RC boolOr(Value *left, Value *right, Value *result) {
    if (left->dt != DT_BOOL || right->dt != DT_BOOL)
        THROW(RC_RM_BOOLEAN_EXPR_ARG_IS_NOT_BOOLEAN, 
             "OR operator requires boolean inputs");
    
    result->dt = DT_BOOL;
    result->v.boolV = (left->v.boolV || right->v.boolV);
    return RC_OK;
}

// ================== Expression Evaluation ==================
RC evalExpr(Record *record, Schema *schema, Expr *expr, Value **result) {
    Value *lIn = NULL;
    Value *rIn = NULL;
    RC rc = RC_OK;

    switch (expr->type) {
        case EXPR_OP: {
            Operator *op = expr->expr.op;
            bool twoArgs = (op->type != OP_BOOL_NOT);

            // Evaluate left argument
            if ((rc = evalExpr(record, schema, op->args[0], &lIn)) != RC_OK) 
                goto cleanup;

            // Evaluate right argument if needed
            if (twoArgs && (rc = evalExpr(record, schema, op->args[1], &rIn)) != RC_OK)
                goto cleanup;

            // FIX: Allocate memory for *result before calling operator functions
            *result = (Value *) malloc(sizeof(Value));
            if (!*result) {
                rc = RC_MEM_ALLOC_FAILED;  // or RC_INVALID_RECORD_SIZE
                goto cleanup;
            }

            // Now call the operator
            switch (op->type) {
                case OP_BOOL_NOT:
                    rc = boolNot(lIn, *result);
                    break;
                case OP_BOOL_AND:
                    rc = boolAnd(lIn, rIn, *result);
                    break;
                case OP_BOOL_OR:
                    rc = boolOr(lIn, rIn, *result);
                    break;
                case OP_COMP_EQUAL:
                    rc = valueEquals(lIn, rIn, *result);
                    break;
                case OP_COMP_SMALLER:
                    rc = valueSmaller(lIn, rIn, *result);
                    break;
                default:
                    THROW(RC_RM_UNKNOWN_OPERATOR, "Unsupported operator");
            }
            break;
        }

        case EXPR_CONST:
            // Already allocates memory for *result
            *result = (Value *) malloc(sizeof(Value));
            if (!*result) {
                return RC_MEM_ALLOC_FAILED; 
            }
            CPVAL(*result, expr->expr.cons);
            break;

        case EXPR_ATTRREF:
            // Freed *result was causing a segfault or other issues, so do not free here
            CHECK(getAttr(record, schema, expr->expr.attrRef, result));
            break;
    }

cleanup:
    // Cleanup temporary values
    freeVal(lIn);
    freeVal(rIn);
    return rc;
}


// ================== Memory Management ==================
RC freeExpr(Expr *expr) {
    if (!expr) return RC_OK;

    switch (expr->type) {
        case EXPR_OP: {
            Operator *op = expr->expr.op;
            switch (op->type) {
                case OP_BOOL_NOT:
                    freeExpr(op->args[0]);
                    break;
                default:
                    freeExpr(op->args[0]);
                    freeExpr(op->args[1]);
                    break;
            }
            free(op->args);
            free(op);
            break;
        }
        case EXPR_CONST:
            freeVal(expr->expr.cons);
            break;
        case EXPR_ATTRREF:
            // No dynamic memory for attribute references
            break;
    }
    free(expr);
    return RC_OK;
}

void freeVal(Value *val) {
    if (!val) return;
    if (val->dt == DT_STRING && val->v.stringV)
        free(val->v.stringV);
    free(val);
}