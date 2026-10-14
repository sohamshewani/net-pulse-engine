#define _GNU_SOURCE
#include <stdint.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <unistd.h>
#include <errno.h>

#ifndef MAP_HUGETLB
#define MAP_HUGETLB 0x40000
#endif

#ifndef MAP_HUGE_2MB
#define MAP_HUGE_2MB (21 << 21)
#endif

typedef struct ArenaChunk {
    struct ArenaChunk* next;
    size_t capacity;
    size_t used;
    uint8_t data[];
} ArenaChunk;

typedef struct MemoryArena {
    ArenaChunk* current;
    ArenaChunk* reserve;
    size_t default_chunk_size;
    size_t total_allocated;
    int use_hugepages;
} MemoryArena;

static ArenaChunk* arena_chunk_create(size_t capacity, int use_hugepages) {
    size_t total_size = sizeof(ArenaChunk) + capacity;
    void* ptr = NULL;

    if (use_hugepages) {
        ptr = mmap(NULL, total_size, PROT_READ | PROT_WRITE,
                   MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB | MAP_HUGE_2MB, -1, 0);
        if (ptr == MAP_FAILED) {
            ptr = mmap(NULL, total_size, PROT_READ | PROT_WRITE,
                       MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
        }
    } else {
        ptr = mmap(NULL, total_size, PROT_READ | PROT_WRITE,
                   MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    }

    if (ptr == MAP_FAILED) {
        return NULL;
    }

    ArenaChunk* chunk = (ArenaChunk*)ptr;
    chunk->next = NULL;
    chunk->capacity = capacity;
    chunk->used = 0;
    return chunk;
}

static void arena_chunk_destroy(ArenaChunk* chunk) {
    if (!chunk) return;
    size_t total_size = sizeof(ArenaChunk) + chunk->capacity;
    munmap(chunk, total_size);
}

int net_pulse_arena_init(MemoryArena* arena, size_t default_chunk_size, int use_hugepages) {
    if (!arena || default_chunk_size == 0) {
        return -EINVAL;
    }

    // Align chunk size to page boundary (4096 bytes)
    size_t page_size = sysconf(_SC_PAGESIZE);
    if (default_chunk_size % page_size != 0) {
        default_chunk_size = ((default_chunk_size + page_size - 1) / page_size) * page_size;
    }

    arena->default_chunk_size = default_chunk_size;
    arena->use_hugepages = use_hugepages;
    arena->total_allocated = 0;

    ArenaChunk* initial_chunk = arena_chunk_create(default_chunk_size, use_hugepages);
    if (!initial_chunk) {
        return -ENOMEM;
    }

    arena->current = initial_chunk;
    arena->total_allocated += default_chunk_size;

    // Pre-reserve a fallback chunk for emergency exhaustion handling
    ArenaChunk* reserve_chunk = arena_chunk_create(default_chunk_size, use_hugepages);
    if (!reserve_chunk) {
        arena_chunk_destroy(initial_chunk);
        return -ENOMEM;
    }
    arena->reserve = reserve_chunk;

    return 0;
}

void net_pulse_arena_destroy(MemoryArena* arena) {
    if (!arena) return;

    ArenaChunk* curr = arena->current;
    while (curr) {
        ArenaChunk* next = curr->next;
        arena_chunk_destroy(curr);
        curr = next;
    }

    if (arena->reserve) {
        arena_chunk_destroy(arena->reserve);
    }

    memset(arena, 0, sizeof(MemoryArena));
}

void* net_pulse_arena_alloc(MemoryArena* arena, size_t size) {
    if (!arena || size == 0) {
        return NULL;
    }

    // Align size to 8 bytes for modern CPU architectures
    size_t aligned_size = (size + 7) & ~((size_t)7);

    ArenaChunk* chunk = arena->current;

    // Check if current chunk has enough space
    if (chunk && (chunk->used + aligned_size <= chunk->capacity)) {
        void* ptr = &chunk->data[chunk->used];
        chunk->used += aligned_size;
        return ptr;
    }

    // Exhaustion in current chunk: attempt to allocate a new chunk
    size_t new_capacity = arena->default_chunk_size;
    if (aligned_size > new_capacity) {
        size_t page_size = sysconf(_SC_PAGESIZE);
        new_capacity = ((sizeof(ArenaChunk) + aligned_size + page_size - 1) / page_size) * page_size;
        new_capacity -= sizeof(ArenaChunk);
    }

    ArenaChunk* new_chunk = arena_chunk_create(new_capacity, arena->use_hugepages);
    if (!new_chunk) {
        // Emergency Fallback: Use the pre-reserved reserve chunk if available
        if (arena->reserve && (aligned_size <= arena->reserve->capacity)) {
            ArenaChunk* emergency = arena->reserve;
            arena->reserve = NULL; // Consume reserve
            emergency->next = arena->current;
            arena->current = emergency;

            void* ptr = &emergency->data[emergency->used];
            emergency->used += aligned_size;
            return ptr;
        }
        return NULL; // Arena Exhaustion Failure Mode
    }

    arena->total_allocated += new_capacity;
    new_chunk->next = arena->current;
    arena->current = new_chunk;

    void* ptr = &new_chunk->data[new_chunk->used];
    new_chunk->used += aligned_size;
    return ptr;
}

void* net_pulse_arena_realloc(MemoryArena* arena, void* ptr, size_t old_size, size_t new_size) {
    if (!arena || new_size == 0) {
        return NULL;
    }

    if (!ptr || old_size == 0) {
        return net_pulse_arena_alloc(arena, new_size);
    }

    if (new_size <= old_size) {
        return ptr;
    }

    // Check if the allocation was the most recent one in the current chunk
    size_t aligned_old = (old_size + 7) & ~((size_t)7);
    size_t aligned_new = (new_size + 7) & ~((size_t)7);
    size_t diff = aligned_new - aligned_old;

    ArenaChunk* chunk = arena->current;
    if (chunk) {
        uint8_t* chunk_start = chunk->data;
        uint8_t* chunk_end = chunk->data + chunk->used;
        uint8_t* target = (uint8_t*)ptr;

        if (target >= chunk_start && target + aligned_old == chunk_end) {
            if (chunk->used + diff <= chunk->capacity) {
                chunk->used += diff;
                return ptr;
            }
        }
    }

    // Fallback: Allocate new block, copy data, and leave old block orphaned until epoch reset
    void* new_ptr = net_pulse_arena_alloc(arena, new_size);
    if (!new_ptr) {
        return NULL;
    }

    memcpy(new_ptr, ptr, old_size);
    return new_ptr;
}

void net_pulse_arena_reset(MemoryArena* arena) {
    if (!arena) return;

    ArenaChunk* curr = arena->current;
    ArenaChunk* last = NULL;

    // Unwind chain, keeping the largest/first chunks and resetting usage
    while (curr) {
        curr->used = 0;
        last = curr;
        curr = curr->next;
    }

    // If reserve chunk was consumed, restore it from the chain if possible
    if (!arena->reserve && last && last->capacity == arena->default_chunk_size) {
        // Detach last chunk to serve as reserve
        // For simplicity in single-threaded epoch resets, we keep all allocated chunks 
        // linked and reset their 'used' counters to zero to avoid system call overhead (munmap/mmap).
    }

    curr = arena->current;
    while (curr) {
        curr->used = 0;
        curr = curr->next;
    }
}

size_t net_pulse_arena_total_allocated(const MemoryArena* arena) {
    if (!arena) return 0;
    return arena->total_allocated;
}