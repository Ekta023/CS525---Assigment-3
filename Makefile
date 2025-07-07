# Makefile for Assignment 3 - Record Manager
CC      = gcc
CFLAGS  = -Wall -Wextra -std=c99 -g

# Test executables
TARGET_EXPR = test_expr
TARGET_ASSIGN3 = test_assign3

# Source files
COMMON_SRCS = \
    buffer_mgr.c \
    buffer_mgr_stat.c \
    dberror.c \
    expr.c \
    record_mgr.c \
    rm_serializer.c \
    storage_mgr.c

TEST_SRCS = \
    test_expr.c \
    test_assign3_1.c

# Object files
COMMON_OBJS = $(COMMON_SRCS:.c=.o)
TEST_OBJS = $(TEST_SRCS:.c=.o)

all: $(TARGET_EXPR) $(TARGET_ASSIGN3)

$(TARGET_EXPR): $(COMMON_OBJS) test_expr.o
	$(CC) $(CFLAGS) -o $@ $^

$(TARGET_ASSIGN3): $(COMMON_OBJS) test_assign3_1.o
	$(CC) $(CFLAGS) -o $@ $^

%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

clean:
	rm -f $(COMMON_OBJS) $(TEST_OBJS) $(TARGET_EXPR) $(TARGET_ASSIGN3)
	rm -f test_table_r

.PHONY: all clean
