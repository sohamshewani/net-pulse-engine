#ifndef NET_PULSE_ENGINE_RING_H
#define NET_PULSE_ENGINE_RING_H

#define _GNU_SOURCE
#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>
#include <sys/types.h>

#define NET_PULSE_CACHE_LINE 64
#define NET_PULSE_PAGE_SIZE 4096

typedef enum {
    NET_PULSE_OK = 0,
    NET_PULSE_ERR_INVALID_ARG = -1,
    NET_PULSE_ERR_NO_MEM = -2,
    NET_PULSE_ERR_FULL = -3,
    NET_PULSE_ERR_EMPTY = -4,
    NET_PULSE_ERR_OVERFLOW = -5,
    NET_PULSE_ERR_SYSTEM = -6
} NetPulseStatus;

typedef struct {
    void *data;
    uint32_t length;
    uint32_t capacity;
    uint64_t timestamp_ns;
    uint32_t sequence;
    uint16_t flags;
    uint16_t protocol;
} PacketBuffer;

typedef struct {
    char *base;
    size_t capacity;
    size_t offset;
    size_t chunk_size;
    bool is_fallback;
    void *guard_page_start;
    void *guard_page_end;
    void *next_chunk;
} MemoryArena;

typedef struct {
    uint32_t head __attribute__((aligned(NET_PULSE_CACHE_LINE)));
    uint32_t tail __attribute__((aligned(NET_PULSE_CACHE_LINE)));
    uint32_t capacity;
    uint32_t mask;
    PacketBuffer **descriptors;
    uint64_t drop_count __attribute__((aligned(NET_PULSE_CACHE_LINE)));
    bool backpressure_tripped;
} NetPulseRing;

typedef struct {
    uint32_t send_next;
    uint32_t send_una;
    uint32_t recv_next;
    uint32_t window_size;
    uint32_t rtt_avg_ns;
    uint64_t token_bucket;
    uint64_t last_replenish_ns;
    uint32_t rate_limit_bps;
} SlidingWindow;

NetPulseStatus net_pulse_ring_init(NetPulseRing *ring, uint32_t capacity);
void net_pulse_ring_free(NetPulseRing *ring);
bool net_pulse_ring_enqueue(NetPulseRing *ring, PacketBuffer *pkt);
bool net_pulse_ring_dequeue(NetPulseRing *ring, PacketBuffer **pkt);
uint32_t net_pulse_ring_count(const NetPulseRing *ring);

NetPulseStatus memory_arena_init(MemoryArena *arena, size_t initial_chunk_size);
void memory_arena_free(MemoryArena *arena);
void *memory_arena_alloc(MemoryArena *arena, size_t size);
void memory_arena_reset(MemoryArena *arena);

void sliding_window_init(SlidingWindow *window, uint32_t initial_window_size, uint32_t rate_limit_bps);
bool sliding_window_can_send(SlidingWindow *window, uint32_t packet_size, uint64_t current_time_ns);
void sliding_window_acknowledge(SlidingWindow *window, uint32_t ack_sequence);

#endif /* NET_PULSE_ENGINE_RING_H */

#ifndef NET_PULSE_ENGINE_RING_IMPL
#define NET_PULSE_ENGINE_RING_IMPL

#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <unistd.h>
#include <errno.h>
#include <time.h>

static inline bool is_power_of_two(uint32_t n) {
    return n != 0 && (n & (n - 1)) == 0;
}

NetPulseStatus net_pulse_ring_init(NetPulseRing *ring, uint32_t capacity) {
    if (ring == NULL || !is_power_of_two(capacity)) {
        return NET_PULSE_ERR_INVALID_ARG;
    }

    ring->head = 0;
    ring->tail = 0;
    ring->capacity = capacity;
    ring->mask = capacity - 1;
    ring->drop_count = 0;
    ring->backpressure_tripped = false;

    size_t alloc_size = capacity * sizeof(PacketBuffer *);
    
    int flags = MAP_PRIVATE | MAP_ANONYMOUS;
#ifdef MAP_HUGETLB
    flags |= MAP_HUGETLB;
#endif

    ring->descriptors = (PacketBuffer **)mmap(NULL, alloc_size, PROT_READ | PROT_WRITE, flags, -1, 0);
    if (ring->descriptors == MAP_FAILED) {
        flags = MAP_PRIVATE | MAP_ANONYMOUS;
        ring->descriptors = (PacketBuffer **)mmap(NULL, alloc_size, PROT_READ | PROT_WRITE, flags, -1, 0);
        if (ring->descriptors == MAP_FAILED) {
            return NET_PULSE_ERR_NO_MEM;
        }
    }

    memset(ring->descriptors, 0, alloc_size);
    return NET_PULSE_OK;
}

void net_pulse_ring_free(NetPulseRing *ring) {
    if (ring == NULL) {
        return;
    }
    if (ring->descriptors != NULL && ring->descriptors != MAP_FAILED) {
        size_t alloc_size = ring->capacity * sizeof(PacketBuffer *);
        munmap(ring->descriptors, alloc_size);
        ring->descriptors = NULL;
    }
    ring->capacity = 0;
    ring->mask = 0;
    ring->head = 0;
    ring->tail = 0;
}

bool net_pulse_ring_enqueue(NetPulseRing *ring, PacketBuffer *pkt) {
    uint32_t head = __atomic_load_n(&ring->head, __ATOMIC_RELAXED);
    uint32_t tail = __atomic_load_n(&ring->tail, __ATOMIC_ACQUIRE);

    if ((head - tail) >= ring->capacity) {
        __atomic_fetch_add(&ring->drop_count, 1, __ATOMIC_RELAXED);
        ring->backpressure_tripped = true;
        return false;
    }

    ring->descriptors[head & ring->mask] = pkt;
    __atomic_store_n(&ring->head, head + 1, __ATOMIC_RELEASE);
    ring->backpressure_tripped = false;
    return true;
}

bool net_pulse_ring_dequeue(NetPulseRing *ring, PacketBuffer **pkt) {
    uint32_t tail = __atomic_load_n(&ring->tail, __ATOMIC_RELAXED);
    uint32_t head = __atomic_load_n(&ring->head, __ATOMIC_ACQUIRE);

    if (tail == head) {
        return false;
    }

    *pkt = ring->descriptors[tail & ring->mask];
    __atomic_store_n(&ring->tail, tail + 1, __ATOMIC_RELEASE);
    return true;
}

uint32_t net_pulse_ring_count(const NetPulseRing *ring) {
    uint32_t head = __atomic_load_n(&ring->head, __ATOMIC_ACQUIRE);
    uint32_t tail = __atomic_load_n(&ring->tail, __ATOMIC_ACQUIRE);
    if (head >= tail) {
        return head - tail;
    }
    return 0;
}

NetPulseStatus memory_arena_init(MemoryArena *arena, size_t initial_chunk_size) {
    if (arena == NULL || initial_chunk_size == 0) {
        return NET_PULSE_ERR_INVALID_ARG;
    }

    size_t page_size = (size_t)sysconf(_SC_PAGESIZE);
    size_t aligned_chunk_size = (initial_chunk_size + page_size - 1) & ~(page_size - 1);

    size_t total_map_size = aligned_chunk_size + (2 * page_size);
    char *map = (char *)mmap(NULL, total_map_size, PROT_NONE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (map == MAP_FAILED) {
        return NET_PULSE_ERR_NO_MEM;
    }

    arena->guard_page_start = map;
    arena->base = map + page_size;
    arena->guard_page_end = arena->base + aligned_chunk_size;

    if (mprotect(arena->base, aligned_chunk_size, PROT_READ | PROT_WRITE) != 0) {
        munmap(map, total_map_size);
        return NET_PULSE_ERR_SYSTEM;
    }

    arena->capacity = aligned_chunk_size;
    arena->offset = 0;
    arena->chunk_size = aligned_chunk_size;
    arena->is_fallback = false;
    arena->next_chunk = NULL;

    return NET_PULSE_OK;
}

void memory_arena_free(MemoryArena *arena) {
    if (arena == NULL || arena->guard_page_start == NULL) {
        return;
    }
    size_t total_map_size = arena->capacity + (2 * (size_t)sysconf(_SC_PAGESIZE));
    munmap(arena->guard_page_start, total_map_size);
    arena->guard_page_start = NULL;
    arena->base = NULL;
    arena->guard_page_end = NULL;
    arena->capacity = 0;
    arena->offset = 0;
}

void *memory_arena_alloc(MemoryArena *arena, size_t size) {
    if (arena == NULL || size == 0) {
        return NULL;
    }

    size_t alignment = sizeof(void *);
    size_t aligned_size = (size + (alignment - 1)) & ~(alignment - 1);

    if (arena->offset + aligned_size > arena->capacity) {
        arena->is_fallback = true;
        return NULL;
    }

    void *ptr = (void *)(arena->base + arena->offset);
    arena->offset += aligned_size;
    return ptr;
}

void memory_arena_reset(MemoryArena *arena) {
    if (arena == NULL) {
        return;
    }
    arena->offset = 0;
    arena->is_fallback = false;
}

void sliding_window_init(SlidingWindow *window, uint32_t initial_window_size, uint32_t rate_limit_bps) {
    if (window == NULL) {
        return;
    }
    window->send_next = 0;
    window->send_una = 0;
    window->recv_next = 0;
    window->window_size = initial_window_size;
    window->rtt_avg_ns = 1000000;
    window->token_bucket = rate_limit_bps / 8;
    window->last_replenish_ns = 0;
    window->rate_limit_bps = rate_limit_bps;
}

bool sliding_window_can_send(SlidingWindow *window, uint32_t packet_size, uint64_t current_time_ns) {
    if (window == NULL) {
        return false;
    }

    if ((window->send_next - window->send_una) >= window->window_size) {
        return false;
    }

    uint64_t elapsed_ns = current_time_ns - window->last_replenish_ns;
    if (elapsed_ns > 1000000) {
        uint64_t tokens_to_add = ((uint64_t)window->rate_limit_bps * elapsed_ns) / (8ULL * 1000000000ULL);
        window->token_bucket += tokens_to_add;
        uint64_t max_bucket = window->rate_limit_bps / 8;
        if (window->token_bucket > max_bucket) {
            window->token_bucket = max_bucket;
        }
        window->last_replenish_ns = current_time_ns;
    }

    if (window->token_bucket < packet_size) {
        return false;
    }

    window->token_bucket -= packet_size;
    window->send_next++;
    return true;
}

void sliding_window_acknowledge(SlidingWindow *window, uint32_t ack_sequence) {
    if (window == NULL) {
        return;
    }
    if (ack_sequence > window->send_una && ack_sequence <= window->send_next) {
        window->send_una = ack_sequence;
    }
}

#endif /* NET_PULSE_ENGINE_RING_IMPL */