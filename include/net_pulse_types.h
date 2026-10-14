#ifndef NET_PULSE_TYPES_H
#define NET_PULSE_TYPES_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>
#include <sys/types.h>

#define NET_PULSE_CACHE_LINE_SIZE 64
#define NET_PULSE_ALIGN(x) __attribute__((aligned(x)))

typedef enum {
    NET_PULSE_SUCCESS = 0,
    NET_PULSE_ERR_INVALID_ARG = -1,
    NET_PULSE_ERR_RING_FULL = -2,
    NET_PULSE_ERR_RING_EMPTY = -3,
    NET_PULSE_ERR_ARENA_EXHAUSTED = -4,
    NET_PULSE_ERR_OVERFLOW = -5,
    NET_PULSE_ERR_SYSTEM = -6
} NetPulseStatus;

typedef enum {
    PACKET_FLAG_NONE = 0,
    PACKET_FLAG_EXT_MEMORY = (1 << 0),
    PACKET_FLAG_DROPPED = (1 << 1),
    PACKET_FLAG_CHKSUM_VALID = (1 << 2)
} PacketFlags;

typedef struct NET_PULSE_ALIGN(NET_PULSE_CACHE_LINE_SIZE) {
    void* data;
    uint32_t length;
    uint32_t capacity;
    uint64_t timestamp_ns;
    uint32_t sequence_number;
    uint32_t flags;
} PacketBuffer;

typedef struct NET_PULSE_ALIGN(NET_PULSE_CACHE_LINE_SIZE) {
    uint32_t capacity;
    uint32_t mask;
    
    NET_PULSE_ALIGN(NET_PULSE_CACHE_LINE_SIZE) volatile uint32_t head;
    NET_PULSE_ALIGN(NET_PULSE_CACHE_LINE_SIZE) volatile uint32_t tail;
    
    PacketBuffer** descriptors;
} NetPulseRing;

typedef struct ArenaChunk {
    void* base;
    size_t size;
    size_t used;
    struct ArenaChunk* next;
} ArenaChunk;

typedef struct NET_PULSE_ALIGN(NET_PULSE_CACHE_LINE_SIZE) {
    ArenaChunk* current_chunk;
    ArenaChunk* reserve_chunk;
    size_t default_chunk_size;
    bool in_emergency_fallback;
    uint64_t total_allocated;
} MemoryArena;

typedef struct NET_PULSE_ALIGN(NET_PULSE_CACHE_LINE_SIZE) {
    uint32_t next_send_seq;
    uint32_t last_ack_seq;
    uint32_t window_size;
    uint32_t rtt_smoothed_ns;
    
    uint64_t token_bucket_tokens;
    uint64_t token_bucket_capacity;
    uint64_t token_refill_rate_per_sec;
    uint64_t last_refill_timestamp_ns;
    
    bool backpressure_tripped;
    uint32_t drop_counter;
} SlidingWindow;

static inline bool net_pulse_is_power_of_two(uint32_t x) {
    return x != 0 && (x & (x - 1)) == 0;
}

static inline NetPulseStatus net_pulse_ring_init(NetPulseRing* ring, uint32_t capacity) {
    if (!ring || !net_pulse_is_power_of_two(capacity)) {
        return NET_PULSE_ERR_INVALID_ARG;
    }
    
    ring->capacity = capacity;
    ring->mask = capacity - 1;
    __atomic_store_n(&ring->head, 0, __ATOMIC_RELEASE);
    __atomic_store_n(&ring->tail, 0, __ATOMIC_RELEASE);
    
    ring->descriptors = (PacketBuffer**)aligned_alloc(NET_PULSE_CACHE_LINE_SIZE, sizeof(PacketBuffer*) * capacity);
    if (!ring->descriptors) {
        return NET_PULSE_ERR_SYSTEM;
    }
    
    for (uint32_t i = 0; i < capacity; i++) {
        ring->descriptors[i] = NULL;
    }
    
    return NET_PULSE_SUCCESS;
}

static inline void net_pulse_ring_free(NetPulseRing* ring) {
    if (ring) {
        if (ring->descriptors) {
            free(ring->descriptors);
            ring->descriptors = NULL;
        }
        ring->capacity = 0;
        ring->mask = 0;
        __atomic_store_n(&ring->head, 0, __ATOMIC_RELAXED);
        __atomic_store_n(&ring->tail, 0, __ATOMIC_RELAXED);
    }
}

static inline NetPulseStatus net_pulse_ring_enqueue(NetPulseRing* ring, PacketBuffer* pkt) {
    if (!ring || !pkt) {
        return NET_PULSE_ERR_INVALID_ARG;
    }
    
    uint32_t current_head = __atomic_load_n(&ring->head, __ATOMIC_RELAXED);
    uint32_t current_tail = __atomic_load_n(&ring->tail, __ATOMIC_ACQUIRE);
    
    if ((current_head - current_tail) >= ring->capacity) {
        return NET_PULSE_ERR_RING_FULL;
    }
    
    ring->descriptors[current_head & ring->mask] = pkt;
    __atomic_store_n(&ring->head, current_head + 1, __ATOMIC_RELEASE);
    
    return NET_PULSE_SUCCESS;
}

static inline NetPulseStatus net_pulse_ring_dequeue(NetPulseRing* ring, PacketBuffer** pkt) {
    if (!ring || !pkt) {
        return NET_PULSE_ERR_INVALID_ARG;
    }
    
    uint32_t current_tail = __atomic_load_n(&ring->tail, __ATOMIC_RELAXED);
    uint32_t current_head = __atomic_load_n(&ring->head, __ATOMIC_ACQUIRE);
    
    if (current_tail == current_head) {
        return NET_PULSE_ERR_RING_EMPTY;
    }
    
    *pkt = ring->descriptors[current_tail & ring->mask];
    ring->descriptors[current_tail & ring->mask] = NULL;
    __atomic_store_n(&ring->tail, current_tail + 1, __ATOMIC_RELEASE);
    
    return NET_PULSE_SUCCESS;
}

static inline NetPulseStatus net_pulse_arena_init(MemoryArena* arena, size_t default_chunk_size) {
    if (!arena || default_chunk_size == 0) {
        return NET_PULSE_ERR_INVALID_ARG;
    }
    
    void* base = aligned_alloc(NET_PULSE_CACHE_LINE_SIZE, default_chunk_size);
    if (!base) {
        return NET_PULSE_ERR_SYSTEM;
    }
    
    ArenaChunk* chunk = (ArenaChunk*)malloc(sizeof(ArenaChunk));
    if (!chunk) {
        free(base);
        return NET_PULSE_ERR_SYSTEM;
    }
    
    chunk->base = base;
    chunk->size = default_chunk_size;
    chunk->used = 0;
    chunk->next = NULL;
    
    void* reserve_base = aligned_alloc(NET_PULSE_CACHE_LINE_SIZE, default_chunk_size);
    if (!reserve_base) {
        free(chunk);
        free(base);
        return NET_PULSE_ERR_SYSTEM;
    }
    
    ArenaChunk* reserve = (ArenaChunk*)malloc(sizeof(ArenaChunk));
    if (!reserve) {
        free(reserve_base);
        free(chunk);
        free(base);
        return NET_PULSE_ERR_SYSTEM;
    }
    
    reserve->base = reserve_base;
    reserve->size = default_chunk_size;
    reserve->used = 0;
    reserve->next = NULL;
    
    arena->current_chunk = chunk;
    arena->reserve_chunk = reserve;
    arena->default_chunk_size = default_chunk_size;
    arena->in_emergency_fallback = false;
    arena->total_allocated = default_chunk_size;
    
    return NET_PULSE_SUCCESS;
}

static inline void net_pulse_arena_free(MemoryArena* arena) {
    if (!arena) {
        return;
    }
    
    ArenaChunk* current = arena->current_chunk;
    while (current) {
        ArenaChunk* next = current->next;
        free(current->base);
        free(current);
        current = next;
    }
    
    if (arena->reserve_chunk) {
        free(arena->reserve_chunk->base);
        free(arena->reserve_chunk);
    }
    
    arena->current_chunk = NULL;
    arena->reserve_chunk = NULL;
    arena->total_allocated = 0;
}

static inline void* net_pulse_arena_alloc(MemoryArena* arena, size_t size) {
    if (!arena || size == 0) {
        return NULL;
    }
    
    size_t aligned_size = (size + (NET_PULSE_CACHE_LINE_SIZE - 1)) & ~(NET_PULSE_CACHE_LINE_SIZE - 1);
    
    ArenaChunk* chunk = arena->current_chunk;
    if (chunk->used + aligned_size <= chunk->size) {
        void* ptr = (char*)chunk->base + chunk->used;
        chunk->used += aligned_size;
        return ptr;
    }
    
    if (chunk->next && (chunk->next->used + aligned_size <= chunk->next->size)) {
        arena->current_chunk = chunk->next;
        void* ptr = (char*)arena->current_chunk->base + arena->current_chunk->used;
        arena->current_chunk->used += aligned_size;
        return ptr;
    }
    
    void* new_base = aligned_alloc(NET_PULSE_CACHE_LINE_SIZE, arena->default_chunk_size);
    if (!new_base) {
        if (!arena->in_emergency_fallback && arena->reserve_chunk) {
            arena->in_emergency_fallback = true;
            ArenaChunk* reserve = arena->reserve_chunk;
            arena->reserve_chunk = NULL;
            reserve->next = arena->current_chunk;
            arena->current_chunk = reserve;
            
            if (arena->current_chunk->used + aligned_size <= arena->current_chunk->size) {
                void* ptr = (char*)arena->current_chunk->base + arena->current_chunk->used;
                arena->current_chunk->used += aligned_size;
                return ptr;
            }
        }
        return NULL;
    }
    
    ArenaChunk* new_chunk = (ArenaChunk*)malloc(sizeof(ArenaChunk));
    if (!new_chunk) {
        free(new_base);
        return NULL;
    }
    
    new_chunk->base = new_base;
    new_chunk->size = arena->default_chunk_size;
    new_chunk->used = aligned_size;
    new_chunk->next = arena->current_chunk;
    
    arena->current_chunk = new_chunk;
    arena->total_allocated += arena->default_chunk_size;
    
    return new_base;
}

static inline void net_pulse_arena_epoch_reset(MemoryArena* arena) {
    if (!arena) {
        return;
    }
    
    ArenaChunk* chunk = arena->current_chunk;
    while (chunk) {
        chunk->used = 0;
        chunk = chunk->next;
    }
    
    if (arena->reserve_chunk) {
        arena->reserve_chunk->used = 0;
    }
    
    arena->in_emergency_fallback = false;
}

static inline void net_pulse_sliding_window_init(SlidingWindow* window, uint32_t window_size, uint64_t token_capacity, uint64_t refill_rate) {
    if (!window) {
        return;
    }
    
    window->next_send_seq = 0;
    window->last_ack_seq = 0;
    window->window_size = window_size;
    window->rtt_smoothed_ns = 1000000;
    window->token_bucket_tokens = token_capacity;
    window->token_bucket_capacity = token_capacity;
    window->token_refill_rate_per_sec = refill_rate;
    window->last_refill_timestamp_ns = 0;
    window->backpressure_tripped = false;
    window->drop_counter = 0;
}

static inline bool net_pulse_sliding_window_can_send(SlidingWindow* window, uint32_t current_seq) {
    if (!window) {
        return false;
    }
    
    if (window->backpressure_tripped) {
        return false;
    }
    
    int32_t diff = (int32_t)(current_seq - window->last_ack_seq);
    if (diff >= (int32_t)window->window_size) {
        return false;
    }
    
    if (window->token_bucket_tokens == 0) {
        return false;
    }
    
    return true;
}

static inline void net_pulse_sliding_window_update(SlidingWindow* window, uint32_t ack_seq, uint64_t current_time_ns) {
    if (!window) {
        return;
    }
    
    if ((int32_t)(ack_seq - window->last_ack_seq) > 0) {
        window->last_ack_seq = ack_seq;
        window->backpressure_tripped = false;
    }
    
    uint64_t elapsed_ns = current_time_ns - window->last_refill_timestamp_ns;
    if (elapsed_ns > 1000000) {
        uint64_t tokens_to_add = (elapsed_ns * window->token_refill_rate_per_sec) / 1000000000ULL;
        window->token_bucket_tokens += tokens_to_add;
        if (window->token_bucket_tokens > window->token_bucket_capacity) {
            window->token_bucket_tokens = window->token_bucket_capacity;
        }
        window->last_refill_timestamp_ns = current_time_ns;
    }
}

static inline void net_pulse_sliding_window_consume_token(SlidingWindow* window) {
    if (window && window->token_bucket_tokens > 0) {
        window->token_bucket_tokens--;
    }
}

#endif /* NET_PULSE_TYPES_H */