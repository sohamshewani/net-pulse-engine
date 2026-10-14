#ifndef NET_PULSE_ENGINE_FILTER_H
#define NET_PULSE_ENGINE_FILTER_H

#define _GNU_SOURCE
#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>
#include <sys/types.h>

#define CACHE_LINE_SIZE 64

typedef struct {
    uint8_t *data;
    uint32_t length;
    uint32_t capacity;
    uint64_t timestamp;
    uint32_t flags;
} PacketBuffer;

typedef struct ArenaChunk {
    struct ArenaChunk *next;
    size_t capacity;
    size_t used;
    uint8_t data[];
} ArenaChunk;

typedef struct {
    ArenaChunk *current_chunk;
    ArenaChunk *reserve_chunk;
    size_t default_chunk_size;
    bool exhausted;
} MemoryArena;

typedef struct {
    uint32_t capacity;
    uint32_t mask;
    PacketBuffer **buffer;
    
    __attribute__((aligned(CACHE_LINE_SIZE))) volatile uint32_t head;
    __attribute__((aligned(CACHE_LINE_SIZE))) volatile uint32_t tail;
    
    uint64_t dropped_packets;
} NetPulseRing;

typedef struct {
    uint32_t left_edge;
    uint32_t right_edge;
    uint32_t window_size;
    uint64_t token_bucket;
    uint64_t max_tokens;
    uint64_t last_refill_time;
    bool backpressure_active;
} SlidingWindow;

typedef struct {
    NetPulseRing ingress_ring;
    NetPulseRing egress_ring;
    MemoryArena arena;
    SlidingWindow window;
    bool active;
} NetPulseEngine;

static inline bool is_power_of_two(uint32_t n) {
    return n != 0 && (n & (n - 1)) == 0;
}

bool net_pulse_ring_init(NetPulseRing *ring, uint32_t capacity) {
    if (!ring || !is_power_of_two(capacity)) {
        return false;
    }

    ring->capacity = capacity;
    ring->mask = capacity - 1;
    ring->head = 0;
    ring->tail = 0;
    ring->dropped_packets = 0;

    ring->buffer = (PacketBuffer **)__builtin_aligned_alloc(CACHE_LINE_SIZE, sizeof(PacketBuffer *) * capacity);
    if (!ring->buffer) {
        return false;
    }

    for (uint32_t i = 0; i < capacity; i++) {
        ring->buffer[i] = NULL;
    }

    return true;
}

void net_pulse_ring_free(NetPulseRing *ring) {
    if (ring && ring->buffer) {
        free(ring->buffer);
        ring->buffer = NULL;
        ring->capacity = 0;
        ring->mask = 0;
    }
}

bool net_pulse_ring_enqueue(NetPulseRing *ring, PacketBuffer *pkt) {
    uint32_t head = __atomic_load_n(&ring->head, __ATOMIC_RELAXED);
    uint32_t tail = __atomic_load_n(&ring->tail, __ATOMIC_ACQUIRE);

    if ((head - tail) >= ring->capacity) {
        __atomic_fetch_add(&ring->dropped_packets, 1, __ATOMIC_RELAXED);
        return false;
    }

    ring->buffer[head & ring->mask] = pkt;
    __atomic_store_n(&ring->head, head + 1, __ATOMIC_RELEASE);
    return true;
}

bool net_pulse_ring_dequeue(NetPulseRing *ring, PacketBuffer **pkt) {
    uint32_t tail = __atomic_load_n(&ring->tail, __ATOMIC_RELAXED);
    uint32_t head = __atomic_load_n(&ring->head, __ATOMIC_ACQUIRE);

    if (tail == head) {
        return false;
    }

    *pkt = ring->buffer[tail & ring->mask];
    ring->buffer[tail & ring->mask] = NULL;
    __atomic_store_n(&ring->tail, tail + 1, __ATOMIC_RELEASE);
    return true;
}

bool memory_arena_init(MemoryArena *arena, size_t chunk_size) {
    if (!arena || chunk_size == 0) {
        return false;
    }

    arena->default_chunk_size = chunk_size;
    arena->exhausted = false;

    size_t alloc_size = sizeof(ArenaChunk) + chunk_size;
    ArenaChunk *chunk = (ArenaChunk *)__builtin_aligned_alloc(CACHE_LINE_SIZE, alloc_size);
    if (!chunk) {
        return false;
    }

    chunk->next = NULL;
    chunk->capacity = chunk_size;
    chunk->used = 0;
    arena->current_chunk = chunk;

    ArenaChunk *reserve = (ArenaChunk *)__builtin_aligned_alloc(CACHE_LINE_SIZE, alloc_size);
    if (!reserve) {
        free(chunk);
        return false;
    }

    reserve->next = NULL;
    reserve->capacity = chunk_size;
    reserve->used = 0;
    arena->reserve_chunk = reserve;

    return true;
}

void memory_arena_free(MemoryArena *arena) {
    if (!arena) return;

    ArenaChunk *current = arena->current_chunk;
    while (current) {
        ArenaChunk *next = current->next;
        free(current);
        current = next;
    }

    if (arena->reserve_chunk) {
        free(arena->reserve_chunk);
        arena->reserve_chunk = NULL;
    }

    arena->current_chunk = NULL;
    arena->exhausted = false;
}

void *memory_arena_alloc(MemoryArena *arena, size_t size) {
    if (!arena || size == 0) return NULL;

    size_t aligned_size = (size + 7) & ~((size_t)7);

    ArenaChunk *chunk = arena->current_chunk;
    if (chunk && (chunk->used + aligned_size <= chunk->capacity)) {
        void *ptr = &chunk->data[chunk->used];
        chunk->used += aligned_size;
        return ptr;
    }

    if (arena->reserve_chunk && (arena->reserve_chunk->used + aligned_size <= arena->reserve_chunk->capacity)) {
        ArenaChunk *temp = arena->current_chunk;
        arena->current_chunk = arena->reserve_chunk;
        arena->reserve_chunk = temp;
        arena->exhausted = true;

        chunk = arena->current_chunk;
        void *ptr = &chunk->data[chunk->used];
        chunk->used += aligned_size;
        return ptr;
    }

    size_t new_capacity = arena->default_chunk_size;
    if (aligned_size > new_capacity) {
        new_capacity = aligned_size;
    }

    size_t alloc_size = sizeof(ArenaChunk) + new_capacity;
    ArenaChunk *new_chunk = (ArenaChunk *)__builtin_aligned_alloc(CACHE_LINE_SIZE, alloc_size);
    if (!new_chunk) {
        return NULL;
    }

    new_chunk->next = arena->current_chunk;
    new_chunk->capacity = new_capacity;
    new_chunk->used = aligned_size;
    arena->current_chunk = new_chunk;

    return &new_chunk->data[0];
}

void memory_arena_reset(MemoryArena *arena) {
    if (!arena) return;

    ArenaChunk *current = arena->current_chunk->next;
    while (current) {
        ArenaChunk *next = current->next;
        free(current);
        current = next;
    }

    arena->current_chunk->next = NULL;
    arena->current_chunk->used = 0;
    arena->exhausted = false;

    if (arena->reserve_chunk) {
        arena->reserve_chunk->used = 0;
    }
}

void sliding_window_init(SlidingWindow *window, uint32_t window_size, uint64_t max_tokens) {
    if (!window) return;
    window->left_edge = 0;
    window->right_edge = window_size;
    window->window_size = window_size;
    window->token_bucket = max_tokens;
    window->max_tokens = max_tokens;
    window->last_refill_time = 0;
    window->backpressure_active = false;
}

bool sliding_window_check_and_update(SlidingWindow *window, uint32_t seq_num, uint64_t current_time) {
    if (!window) return false;

    uint32_t diff = seq_num - window->left_edge;
    if (diff >= (1U << 31)) {
        return false;
    }

    if (diff >= window->window_size) {
        window->backpressure_active = true;
        return false;
    }

    if (window->token_bucket == 0) {
        window->backpressure_active = true;
        return false;
    }

    window->token_bucket--;
    window->backpressure_active = false;
    return true;
}

bool net_pulse_engine_init(NetPulseEngine *engine, uint32_t ring_capacity, size_t arena_chunk_size, uint32_t window_size) {
    if (!engine) return false;

    if (!net_pulse_ring_init(&engine->ingress_ring, ring_capacity)) {
        return false;
    }

    if (!net_pulse_ring_init(&engine->egress_ring, ring_capacity)) {
        net_pulse_ring_free(&engine->ingress_ring);
        return false;
    }

    if (!memory_arena_init(&engine->arena, arena_chunk_size)) {
        net_pulse_ring_free(&engine->ingress_ring);
        net_pulse_ring_free(&engine->egress_ring);
        return false;
    }

    sliding_window_init(&engine->window, window_size, ring_capacity);
    engine->active = true;

    return true;
}

void net_pulse_engine_free(NetPulseEngine *engine) {
    if (!engine) return;

    engine->active = false;
    net_pulse_ring_free(&engine->ingress_ring);
    net_pulse_ring_free(&engine->egress_ring);
    memory_arena_free(&engine->arena);
}

#endif /* NET_PULSE_ENGINE_FILTER_H */