#ifndef NET_PULSE_ENGINE_ARENA_H
#define NET_PULSE_ENGINE_ARENA_H

#define _GNU_SOURCE
#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>
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

#define NP_CACHE_LINE_SIZE 64
#define NP_ALIGN_UP(val, align) (((val) + ((align) - 1)) & ~((align) - 1))

typedef struct ArenaChunk {
    uint8_t *memory;
    size_t capacity;
    size_t used;
    struct ArenaChunk *next;
} ArenaChunk;

typedef struct MemoryArena {
    ArenaChunk *current;
    ArenaChunk *reserve;
    size_t default_chunk_size;
    size_t total_allocated;
    bool exhaustion_flag;
} MemoryArena;

static inline ArenaChunk *np_arena_chunk_create(size_t size, bool use_hugepages) {
    size_t page_size = sysconf(_SC_PAGESIZE);
    size_t alloc_size = NP_ALIGN_UP(size + page_size * 2, page_size);
    
    int flags = MAP_PRIVATE | MAP_ANONYMOUS;
    if (use_hugepages) {
        alloc_size = NP_ALIGN_UP(alloc_size, 2 * 1024 * 1024);
        flags |= MAP_HUGETLB | MAP_HUGE_2MB;
    }

    uint8_t *raw = (uint8_t *)mmap(NULL, alloc_size, PROT_READ | PROT_WRITE, flags, -1, 0);
    if (raw == MAP_FAILED) {
        if (use_hugepages) {
            alloc_size = NP_ALIGN_UP(size + page_size * 2, page_size);
            flags = MAP_PRIVATE | MAP_ANONYMOUS;
            raw = (uint8_t *)mmap(NULL, alloc_size, PROT_READ | PROT_WRITE, flags, -1, 0);
            if (raw == MAP_FAILED) {
                return NULL;
            }
        } else {
            return NULL;
        }
    }

    if (mprotect(raw, page_size, PROT_NONE) != 0) {
        munmap(raw, alloc_size);
        return NULL;
    }

    if (mprotect(raw + alloc_size - page_size, page_size, PROT_NONE) != 0) {
        munmap(raw, alloc_size);
        return NULL;
    }

    ArenaChunk *chunk = (ArenaChunk *)malloc(sizeof(ArenaChunk));
    if (!chunk) {
        munmap(raw, alloc_size);
        return NULL;
    }

    chunk->memory = raw + page_size;
    chunk->capacity = alloc_size - (page_size * 2);
    chunk->used = 0;
    chunk->next = NULL;
    return chunk;
}

static inline void np_arena_chunk_destroy(ArenaChunk *chunk) {
    if (!chunk) return;
    size_t page_size = sysconf(_SC_PAGESIZE);
    uint8_t *raw = chunk->memory - page_size;
    size_t alloc_size = chunk->capacity + (page_size * 2);
    munmap(raw, alloc_size);
    free(chunk);
}

static inline bool np_arena_init(MemoryArena *arena, size_t default_chunk_size, bool use_hugepages) {
    if (!arena) return false;
    
    arena->default_chunk_size = default_chunk_size > 0 ? default_chunk_size : 65536;
    arena->total_allocated = 0;
    arena->exhaustion_flag = false;

    arena->current = np_arena_chunk_create(arena->default_chunk_size, use_hugepages);
    if (!arena->current) {
        return false;
    }
    arena->total_allocated += arena->current->capacity;

    arena->reserve = np_arena_chunk_create(arena->default_chunk_size, false);
    return true;
}

static inline void *np_arena_alloc(MemoryArena *arena, size_t size, size_t alignment) {
    if (!arena || size == 0) return NULL;

    size_t aligned_size = NP_ALIGN_UP(size, alignment > 0 ? alignment : 8);
    ArenaChunk *chunk = arena->current;

    size_t current_offset = NP_ALIGN_UP((uintptr_t)(chunk->memory + chunk->used), (alignment > 0 ? alignment : 8));
    size_t padding = current_offset - (uintptr_t)(chunk->memory + chunk->used);
    size_t total_needed = aligned_size + padding;

    if (chunk->used + total_needed > chunk->capacity) {
        if (arena->reserve && arena->reserve->capacity >= total_needed) {
            ArenaChunk *temp = arena->reserve;
            arena->reserve = NULL;
            temp->next = arena->current;
            arena->current = temp;
            arena->current->used = 0;
            chunk = arena->current;
            arena->exhaustion_flag = true;
        } else {
            size_t new_cap = arena->default_chunk_size;
            if (total_needed > new_cap) {
                new_cap = total_needed;
            }
            ArenaChunk *new_chunk = np_arena_chunk_create(new_cap, false);
            if (!new_chunk) {
                arena->exhaustion_flag = true;
                return NULL;
            }
            arena->total_allocated += new_chunk->capacity;
            new_chunk->next = arena->current;
            arena->current = new_chunk;
            chunk = arena->current;
        }

        current_offset = NP_ALIGN_UP((uintptr_t)(chunk->memory + chunk->used), (alignment > 0 ? alignment : 8));
        padding = current_offset - (uintptr_t)(chunk->memory + chunk->used);
        total_needed = aligned_size + padding;
    }

    void *ptr = (void *)current_offset;
    chunk->used += total_needed;
    return ptr;
}

static inline void np_arena_reset(MemoryArena *arena) {
    if (!arena) return;

    ArenaChunk *chunk = arena->current->next;
    while (chunk) {
        ArenaChunk *next = chunk->next;
        np_arena_chunk_destroy(chunk);
        chunk = next;
    }

    arena->current->next = NULL;
    arena->current->used = 0;
    arena->exhaustion_flag = false;

    if (!arena->reserve) {
        arena->reserve = np_arena_chunk_create(arena->default_chunk_size, false);
    }
}

static inline void np_arena_destroy(MemoryArena *arena) {
    if (!arena) return;

    ArenaChunk *chunk = arena->current;
    while (chunk) {
        ArenaChunk *next = chunk->next;
        np_arena_chunk_destroy(chunk);
        chunk = next;
    }

    if (arena->reserve) {
        np_arena_chunk_destroy(arena->reserve);
    }

    memset(arena, 0, sizeof(MemoryArena));
}

#endif /* NET_PULSE_ENGINE_ARENA_H */