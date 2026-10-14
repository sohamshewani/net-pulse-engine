#define _GNU_SOURCE
#include <stdint.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <sys/mman.h>
#include <unistd.h>
#include <immintrin.h>

#ifndef CACHE_LINE_SIZE
#define CACHE_LINE_SIZE 64
#endif

typedef struct {
    void *data;
    uint32_t length;
    uint32_t flags;
    uint64_t timestamp;
} PacketBuffer;

typedef struct __attribute__((aligned(CACHE_LINE_SIZE))) {
    volatile uint32_t head;
    char pad1[CACHE_LINE_SIZE - sizeof(uint32_t)];
    volatile uint32_t tail;
    char pad2[CACHE_LINE_SIZE - sizeof(uint32_t)];
    uint32_t capacity;
    uint32_t mask;
    PacketBuffer **descriptors;
    volatile uint64_t drop_count;
    bool backpressure_tripped;
} NetPulseRing;

NetPulseRing *net_pulse_ring_init(uint32_t capacity) {
    if (capacity == 0 || (capacity & (capacity - 1)) != 0) {
        errno = EINVAL;
        return NULL;
    }

    size_t ring_size = sizeof(NetPulseRing);
    NetPulseRing *ring = (NetPulseRing *)mmap(NULL, ring_size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (ring == MAP_FAILED) {
        return NULL;
    }

    size_t descriptors_size = sizeof(PacketBuffer *) * capacity;
    PacketBuffer **descriptors = (PacketBuffer **)mmap(NULL, descriptors_size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (descriptors == MAP_FAILED) {
        munmap(ring, ring_size);
        return NULL;
    }

    ring->head = 0;
    ring->tail = 0;
    ring->capacity = capacity;
    ring->mask = capacity - 1;
    ring->descriptors = descriptors;
    ring->drop_count = 0;
    ring->backpressure_tripped = false;

    return ring;
}

void net_pulse_ring_free(NetPulseRing *ring) {
    if (!ring) {
        return;
    }
    if (ring->descriptors) {
        munmap(ring->descriptors, sizeof(PacketBuffer *) * ring->capacity);
    }
    munmap(ring, sizeof(NetPulseRing));
}

bool net_pulse_ring_enqueue(NetPulseRing *ring, PacketBuffer *pkt) {
    if (!ring || !pkt) {
        return false;
    }

    uint32_t current_tail = __atomic_load_n(&ring->tail, __ATOMIC_RELAXED);
    uint32_t current_head = __atomic_load_n(&ring->head, __ATOMIC_ACQUIRE);

    if ((current_tail - current_head) >= ring->capacity) {
        __atomic_fetch_add(&ring->drop_count, 1, __ATOMIC_RELAXED);
        ring->backpressure_tripped = true;
        return false;
    }

    ring->descriptors[current_tail & ring->mask] = pkt;
    
    __atomic_store_n(&ring->tail, current_tail + 1, __ATOMIC_RELEASE);
    
    ring->backpressure_tripped = false;
    return true;
}

bool net_pulse_ring_dequeue(NetPulseRing *ring, PacketBuffer **pkt) {
    if (!ring || !pkt) {
        return false;
    }

    uint32_t current_head = __atomic_load_n(&ring->head, __ATOMIC_RELAXED);
    uint32_t current_tail = __atomic_load_n(&ring->tail, __ATOMIC_ACQUIRE);

    if (current_head == current_tail) {
        return false;
    }

    *pkt = ring->descriptors[current_head & ring->mask];
    
    __atomic_store_n(&ring->head, current_head + 1, __ATOMIC_RELEASE);

    return true;
}

uint32_t net_pulse_ring_count(const NetPulseRing *ring) {
    if (!ring) {
        return 0;
    }
    uint32_t tail = __atomic_load_n(&ring->tail, __ATOMIC_ACQUIRE);
    uint32_t head = __atomic_load_n(&ring->head, __ATOMIC_ACQUIRE);
    if (tail >= head) {
        return tail - head;
    }
    return (0xFFFFFFFF - head + 1) + tail;
}

bool net_pulse_ring_is_full(const NetPulseRing *ring) {
    if (!ring) {
        return false;
    }
    return net_pulse_ring_count(ring) >= ring->capacity;
}

bool net_pulse_ring_is_empty(const NetPulseRing *ring) {
    if (!ring) {
        return true;
    }
    uint32_t tail = __atomic_load_n(&ring->tail, __ATOMIC_ACQUIRE);
    uint32_t head = __atomic_load_n(&ring->head, __ATOMIC_ACQUIRE);
    return head == tail;
}