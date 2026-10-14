#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <time.h>
#include <math.h>

#define WARMUP_ITERATIONS 100000
#define MEASURE_ITERATIONS 5000000

// Mocking the 'net-pulse-engine' core payload & operation hook
typedef struct {
    uint64_t packet_id;
    uint32_t flags;
    uint8_t payload[64];
} NpeContext;

static inline void npe_engine_pulse(NpeContext *ctx) {
    // Simulate lightweight deterministic work representing a network pulse tick
    ctx->packet_id++;
    ctx->flags ^= (uint32_t)(ctx->packet_id & 0x0F);
    // Compiler barrier to prevent dead-code elimination
    __asm__ volatile("" : "+r" (ctx));
}

static int compare_uint64(const void *a, const void *b) {
    uint64_t fa = *(const uint64_t *)a;
    uint64_t fb = *(const uint64_t *)b;
    return (fa > fb) - (fa < fb);
}

static inline uint64_t get_nanos(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

int main(void) {
    NpeContext ctx = { .packet_id = 0, .flags = 0xA5A5 };
    
    // Allocate latency tracking buffer dynamically to avoid stack overflow
    uint64_t *latencies = malloc(MEASURE_ITERATIONS * sizeof(uint64_t));
    if (!latencies) {
        fprintf(stderr, "Failed to allocate memory for latency samples.\n");
        return 1;
    }

    // Warmup phase
    for (uint64_t i = 0; i < WARMUP_ITERATIONS; i++) {
        npe_engine_pulse(&ctx);
    }

    // Measurement phase (per-operation latency tracking)
    uint64_t start_global = get_nanos();

    for (uint64_t i = 0; i < MEASURE_ITERATIONS; i++) {
        uint64_t start_op = get_nanos();
        npe_engine_pulse(&ctx);
        uint64_t end_op = get_nanos();
        latencies[i] = end_op - start_op;
    }

    uint64_t end_global = get_nanos();
    uint64_t total_time_ns = end_global - start_global;

    // Compute Throughput
    double total_seconds = (double)total_time_ns / 1000000000.0;
    double ops_per_sec = (double)MEASURE_ITERATIONS / total_seconds;

    // Sort latencies for percentiles
    qsort(latencies, MEASURE_ITERATIONS, sizeof(uint64_t), compare_uint64);

    uint64_t p50 = latencies[(size_t)(MEASURE_ITERATIONS * 0.50)];
    uint64_t p90 = latencies[(size_t)(MEASURE_ITERATIONS * 0.90)];
    uint64_t p99 = latencies[(size_t)(MEASURE_ITERATIONS * 0.99)];
    uint64_t p999 = latencies[(size_t)(MEASURE_ITERATIONS * 0.999)];

    // Output formatted tabular metrics
    printf("\n");
    printf("=================================================================\n");
    printf("          NET-PULSE-ENGINE PERFORMANCE MICROBENCHMARK            \n");
    printf("=================================================================\n");
    printf("| Metric                 | Value             | Unit             |\n");
    printf("|------------------------|-------------------|------------------|\n");
    printf("| Total Iterations       | %-17lu | ops              |\n", (unsigned long)MEASURE_ITERATIONS);
    printf("| Total Duration         | %-17.3f | ms               |\n", total_seconds * 1000.0);
    printf("| Throughput             | %-17.2f | ops/sec          |\n", ops_per_sec);
    printf("| Latency (p50)          | %-17lu | ns               |\n", (unsigned long)p50);
    printf("| Latency (p90)          | %-17lu | ns               |\n", (unsigned long)p90);
    printf("| Latency (p99)          | %-17lu | ns               |\n", (unsigned long)p99);
    printf("| Latency (p99.9)        | %-17lu | ns               |\n", (unsigned long)p999);
    printf("=================================================================\n\n");

    free(latencies);
    return 0;
}