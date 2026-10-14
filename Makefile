SHELL := /bin/bash
CC := gcc
CFLAGS := -std=c99 -O3 -Wall -Wextra -pedantic -Iinclude
LDFLAGS := -lpthread

TARGET := net-pulse-engine
SRC_DIR := src
BUILD_DIR := build
INCLUDE_DIR := include
TEST_DIR := tests
BENCH_DIR := benchmark

SRCS := $(wildcard $(SRC_DIR)/*.c)
OBJS := $(patsubst $(SRC_DIR)/%.c, $(BUILD_DIR)/%.o, $(SRCS))

TEST_SRCS := $(wildcard $(TEST_DIR)/*.c)
TEST_TARGET := $(BUILD_DIR)/test_runner

BENCH_SRCS := $(wildcard $(BENCH_DIR)/*.c)
BENCH_TARGET := $(BUILD_DIR)/bench_runner

.PHONY: all build test benchmark clean

all: build

build: $(BUILD_DIR)/$(TARGET)

$(BUILD_DIR):
	@mkdir -p $(BUILD_DIR)

$(BUILD_DIR)/%.o: $(SRC_DIR)/%.c | $(BUILD_DIR)
	@if [ -d "$(SRC_DIR)" ] && [ -n "$(wildcard $(SRC_DIR)/*.c)" ]; then \
		$(CC) $(CFLAGS) -c $< -o $@; \
	fi

$(BUILD_DIR)/$(TARGET): $(OBJS) | $(BUILD_DIR)
	@if [ -n "$(OBJS)" ]; then \
		$(CC) $(OBJS) -o $@ $(LDFLAGS); \
	else \
		echo "No source files found in $(SRC_DIR) to build $(TARGET)." >&2; \
		exit 1; \
	fi

test: $(BUILD_DIR)
	@if [ -d "$(TEST_DIR)" ] && [ -n "$(wildcard $(TEST_DIR)/*.c)" ]; then \
		echo "Building and running tests..."; \
		$(CC) $(CFLAGS) $(TEST_DIR)/*.c $(SRC_DIR)/*.c -o $(TEST_TARGET) $(LDFLAGS) && $(TEST_TARGET); \
	else \
		echo "No tests found in $(TEST_DIR). Skipping."; \
	fi

benchmark: $(BUILD_DIR)
	@if [ -d "$(BENCH_DIR)" ] && [ -n "$(wildcard $(BENCH_DIR)/*.c)" ]; then \
		echo "Building and running benchmarks..."; \
		$(CC) $(CFLAGS) $(BENCH_DIR)/*.c $(SRC_DIR)/*.c -o $(BENCH_TARGET) $(LDFLAGS) && $(BENCH_TARGET); \
	else \
		echo "No benchmarks found in $(BENCH_DIR). Skipping."; \
	fi

clean:
	@rm -rf $(BUILD_DIR)
	@echo "Cleaned build artifacts."