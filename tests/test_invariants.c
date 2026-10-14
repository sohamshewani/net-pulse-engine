#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <assert.h>
#include <errno.h>
#include <time.h>

/* ============================================================================
 * MOCK / IMPLEMENTATION OF 'net-pulse-engine' FOR TESTING
 * ============================================================================ */

typedef enum {
    ENGINE_OK = 0,
    ENGINE_ERR_INVAL,
    ENGINE_ERR_NOMEM,
    ENGINE_ERR_STATE,
    ENGINE_ERR_FULL
} engine_status_t;

typedef enum {
    NODE_STATE_IDLE = 0,
    NODE_STATE_ACTIVE,
    NODE_STATE_DEGRADED,
    NODE_STATE_FAILED,
    NODE_STATE_RECOVERING
} node_state_t;

typedef struct {
    uint32_t id;
    node_state_t state;
    uint64_t pulse_count;
    uint64_t failure_count;
    pthread_mutex_t lock;
} pulse_node_t;

typedef struct {
    size_t capacity;
    size_t count;
    pulse_node_t **nodes;
    bool running;
    pthread_rwlock_t rwlock;
    uint64_t total_pulses;
    uint64_t total_failures;
} net_pulse_engine_t;

/* Engine API declarations */
net_pulse_engine_t *engine_create(size_t capacity);
void engine_destroy(net_pulse_engine_t *engine);
engine_status_t engine_add_node(net_pulse_engine_t *engine, uint32_t id, pulse_node_t **out_node);
engine_status_t engine_remove_node(net_pulse_engine_t *engine, uint32_t id);
engine_status_t engine_pulse(net_pulse_engine_t *engine, uint32_t id);
engine_status_t engine_report_failure(net_pulse_engine_t *engine, uint32_t id);
engine_status_t engine_recover_node(net_pulse_engine_t *engine, uint32_t id);
node_state_t engine_get_node_state(net_pulse_engine_t *engine, uint32_t id);

/* Engine API implementations */
net_pulse_engine_t *engine_create(size_t capacity) {
    if (capacity == 0) return NULL;
    net_pulse_engine_t *eng = malloc(sizeof(net_pulse_engine_t));
    if (!eng) return NULL;

    eng->capacity = capacity;
    eng->count = 0;
    eng->nodes = calloc(capacity, sizeof(pulse_node_t *));
    if (!eng->nodes) {
        free(eng);
        return NULL;
    }
    eng->running = true;
    eng->total_pulses = 0;
    eng->total_failures = 0;

    if (pthread_rwlock_init(&eng->rwlock, NULL) != 0) {
        free(eng->nodes);
        free(eng);
        return NULL;
    }
    return eng;
}

void engine_destroy(net_pulse_engine_t *engine) {
    if (!engine) return;
    pthread_rwlock_wrlock(&engine->rwlock);
    engine->running = false;
    for (size_t i = 0; i < engine->count; i++) {
        if (engine->nodes[i]) {
            pthread_mutex_destroy(&engine->nodes[i]->lock);
            free(engine->nodes[i]);
        }
    }
    free(engine->nodes);
    pthread_rwlock_unlock(&engine->rwlock);
    pthread_rwlock_destroy(&engine->rwlock);
    free(engine);
}

static pulse_node_t *find_node_unsafe(net_pulse_engine_t *engine, uint32_t id) {
    for (size_t i = 0; i < engine->count; i++) {
        if (engine->nodes[i] && engine->nodes[i]->id == id) {
            return engine->nodes[i];
        }
    }
    return NULL;
}

engine_status_t engine_add_node(net_pulse_engine_t *engine, uint32_t id, pulse_node_t **out_node) {
    if (!engine) return ENGINE_ERR_INVAL;

    pthread_rwlock_wrlock(&engine->rwlock);
    if (!engine->running) {
        pthread_rwlock_unlock(&engine->rwlock);
        return ENGINE_ERR_STATE;
    }
    if (engine->count >= engine->capacity) {
        pthread_rwlock_unlock(&engine->rwlock);
        return ENGINE_ERR_FULL;
    }
    if (find_node_unsafe(engine, id) != NULL) {
        pthread_rwlock_unlock(&engine->rwlock);
        return ENGINE_ERR_INVAL; /* Already exists */
    }

    pulse_node_t *node = malloc(sizeof(pulse_node_t));
    if (!node) {
        pthread_rwlock_unlock(&engine->rwlock);
        return ENGINE_ERR_NOMEM;
    }

    node->id = id;
    node->state = NODE_STATE_IDLE;
    node->pulse_count = 0;
    node->failure_count = 0;
    if (pthread_mutex_init(&node->lock, NULL) != 0) {
        free(node);
        pthread_rwlock_unlock(&engine->rwlock);
        return ENGINE_ERR_NOMEM;
    }

    engine->nodes[engine->count++] = node;
    if (out_node) *out_node = node;

    pthread_rwlock_unlock(&engine->rwlock);
    return ENGINE_OK;
}

engine_status_t engine_remove_node(net_pulse_engine_t *engine, uint32_t id) {
    if (!engine) return ENGINE_ERR_INVAL;

    pthread_rwlock_wrlock(&engine->rwlock);
    size_t index = (size_t)-1;
    for (size_t i = 0; i < engine->count; i++) {
        if (engine->nodes[i]->id == id) {
            index = i;
            break;
        }
    }

    if (index == (size_t)-1) {
        pthread_rwlock_unlock(&engine->rwlock);
        return ENGINE_ERR_INVAL;
    }

    pulse_node_t *node = engine->nodes[index];
    pthread_mutex_lock(&node->lock);

    /* Shift remaining */
    for (size_t i = index; i < engine->count - 1; i++) {
        engine->nodes[i] = engine->nodes[i + 1];
    }
    engine->nodes[--engine->count] = NULL;

    pthread_mutex_unlock(&node->lock);
    pthread_mutex_destroy(&node->lock);
    free(node);

    pthread_rwlock_unlock(&engine->rwlock);
    return ENGINE_OK;
}

engine_status_t engine_pulse(net_pulse_engine_t *engine, uint32_t id) {
    if (!engine) return ENGINE_ERR_INVAL;

    pthread_rwlock_rdlock(&engine->rwlock);
    if (!engine->running) {
        pthread_rwlock_unlock(&engine->rwlock);
        return ENGINE_ERR_STATE;
    }
    pulse_node_t *node = find_node_unsafe(engine, id);
    if (!node) {
        pthread_rwlock_unlock(&engine->rwlock);
        return ENGINE_ERR_INVAL;
    }

    pthread_mutex_lock(&node->lock);
    if (node->state == NODE_STATE_FAILED) {
        pthread_mutex_unlock(&node->lock);
        pthread_rwlock_unlock(&engine->rwlock);
        return ENGINE_ERR_STATE;
    }
    node->pulse_count++;
    node->state = NODE_STATE_ACTIVE;
    engine->total_pulses++;
    pthread_mutex_unlock(&node->lock);

    pthread_rwlock_unlock(&engine->rwlock);
    return ENGINE_OK;
}

engine_status_t engine_report_failure(net_pulse_engine_t *engine, uint32_t id) {
    if (!engine) return ENGINE_ERR_INVAL;

    pthread_rwlock_rdlock(&engine->rwlock);
    pulse_node_t *node = find_node_unsafe(engine, id);
    if (!node) {
        pthread_rwlock_unlock(&engine->rwlock);
        return ENGINE_ERR_INVAL;
    }

    pthread_mutex_lock(&node->lock);
    node->failure_count++;
    if (node->failure_count > 5) {
        node->state = NODE_STATE_FAILED;
    } else {
        node->state = NODE_STATE_DEGRADED;
    }
    engine->total_failures++;
    pthread_mutex_unlock(&node->lock);

    pthread_rwlock_unlock(&engine->rwlock);
    return ENGINE_OK;
}

engine_status_t engine_recover_node(net_pulse_engine_t *engine, uint32_t id) {
    if (!engine) return ENGINE_ERR_INVAL;

    pthread_rwlock_rdlock(&engine->rwlock);
    pulse_node_t *node = find_node_unsafe(engine, id);
    if (!node) {
        pthread_rwlock_unlock(&engine->rwlock);
        return ENGINE_ERR_INVAL;
    }

    pthread_mutex_lock(&node->lock);
    node->state = NODE_STATE_RECOVERING;
    node->failure_count = 0;
    /* Transition to active after recovery routine */
    node->state = NODE_STATE_ACTIVE;
    pthread_mutex_unlock(&node->lock);

    pthread_rwlock_unlock(&engine->rwlock);
    return ENGINE_OK;
}

node_state_t engine_get_node_state(net_pulse_engine_t *engine, uint32_t id) {
    if (!engine) return NODE_STATE_FAILED;

    pthread_rwlock_rdlock(&engine->rwlock);
    pulse_node_t *node = find_node_unsafe(engine, id);
    if (!node) {
        pthread_rwlock_unlock(&engine->rwlock);
        return NODE_STATE_FAILED;
    }

    pthread_mutex_lock(&node->lock);
    node_state_t st = node->state;
    pthread_mutex_unlock(&node->lock);

    pthread_rwlock_unlock(&engine->rwlock);
    return st;
}

/* ============================================================================
 * TEST SUITE
 * ============================================================================ */

#define TEST_ASSERT(cond, msg) do { \
    if (!(cond)) { \
        fprintf(stderr, "\033[31m[ASSERT FAIL]\033[0m %s:%d - %s\n", __FILE__, __LINE__, msg); \
        exit(EXIT_FAILURE); \
    } \
} while(0)

static void test_unit_boundaries(void) {
    printf("[RUN] Unit & Boundary Tests...\n");

    /* Null and Invalid edge cases */
    TEST_ASSERT(engine_create(0) == NULL, "Engine creation with zero capacity must fail");
    
    net_pulse_engine_t *engine = engine_create(5);
    TEST_ASSERT(engine != NULL, "Engine creation failed");

    /* Invalid node operations */
    TEST_ASSERT(engine_pulse(engine, 999) == ENGINE_ERR_INVAL, "Pulse non-existent node must fail");
    TEST_ASSERT(engine_remove_node(engine, 999) == ENGINE_ERR_INVAL, "Remove non-existent node must fail");
    TEST_ASSERT(engine_report_failure(engine, 999) == ENGINE_ERR_INVAL, "Failure report non-existent node must fail");
    TEST_ASSERT(engine_recover_node(engine, 999) == ENGINE_ERR_INVAL, "Recover non-existent node must fail");
    TEST_ASSERT(