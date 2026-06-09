/*
 * ============================================================================
 * EECG242  –  Spring 2026
 * Network Communication Simulation (Send-and-Wait Protocol)
 * Fully Compliant with Doctor's Specification Manual (RTOS_Prjct - 2026.pdf)
 * Optimized version: Pristine Isolation & Clean Dashboard Metrics Output
 * * FIXES APPLIED:
 * - Added missing xStatsMutex initialization to prevent startup hard-fault.
 * - Replaced dangerous vTaskDelete() with safe pipeline-draining reset.
 * - Replaced redundant xToutTimer with native xSemaphoreTake timeout.
 * ============================================================================
 */

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>

/* FreeRTOS includes */
#include "FreeRTOS.h"
#include "task.h"
#include "queue.h"
#include "timers.h"
#include "semphr.h"

/* ═══════════════════════════════════════════════════════════════════════════
 * SECTION 1 – GLOBAL CONFIGURATIONS & PARAMETERS (Part 3 of PDF)
 * ═══════════════════════════════════════════════════════════════════════════ */
#define L1                500u      /* Min packet length (bytes) */
#define L2                1500u     /* Max packet length (bytes) */
#define T1_MS             100u      /* Min inter-arrival time (ms) */
#define T2_MS             200u      /* Max inter-arrival time (ms) */
#define K_BYTES           40u       /* ACK packet size (bytes) */
#define C_BPS             100000UL  /* Link capacity (bits/sec) */
#define D_MS              5u        /* Propagation delay (ms) */
#define P_ACK_PERCENT     1u        /* ACK drop probability (1% fixed) */
#define MAX_RETRIES       4u        /* Maximum retransmissions allowed */
#define TARGET_PACKETS    2000u     /* Total unique packets to achieve */

#define STACK_SIZE_WORDS  1024u     /* Safe increased Task Stack size */

/* Parametric Execution Arrays (4 Drop Percentages: 1%, 2%, 4%, 8%) */
static const uint32_t P_drop_percent_array[4] = {1u, 2u, 4u, 8u};
static const char* P_drop_strings[4]          = {"0.01", "0.02", "0.04", "0.08"};

/* Active simulation parameters modified dynamically by the Outer Loop */
static volatile uint32_t current_P_drop_percent = 1u;
static volatile uint32_t current_Tout           = 150u; /* Locked at 150ms */
static volatile BaseType_t g_sim_running        = pdFALSE; /* Controls generation */

/* Unified State Tracking across Test Configurations */
static volatile uint32_t g_packet_seq           = 0UL;
static volatile uint32_t g_last_received_seq    = UINT32_MAX;

/* Results storage arrays for final presentation in the Report */
static uint32_t throughput_matrix[4];
static uint32_t avg_tx_int_matrix[4];
static uint32_t avg_tx_frac_matrix[4];
static uint32_t dropped_pkts_matrix[4];

/* ═══════════════════════════════════════════════════════════════════════════
 * SECTION 2 – DATA STRUCTURES (Part 2 of PDF)
 * ═══════════════════════════════════════════════════════════════════════════ */
typedef struct __attribute__((packed)) {
    uint8_t  sender_id;
    uint8_t  dest_id;
    uint16_t length;
    uint32_t seq_num;
    uint8_t  payload[L2 - 8u]; /* 8 bytes header, rest is payload */
} Packet_t;

typedef struct __attribute__((packed)) {
    uint8_t  src_node;
    uint8_t  dest_node;
    uint32_t seq_num;
    uint8_t  padding[K_BYTES - 6u]; /* K = 40 bytes total */
} ACK_t;

/* ═══════════════════════════════════════════════════════════════════════════
 * SECTION 3 – GLOBAL RTOS HANDLES & COUNTERS
 * ═══════════════════════════════════════════════════════════════════════════ */
static QueueHandle_t xGeneratedQueue = NULL;
static QueueHandle_t xTxLinkQueue    = NULL;
static QueueHandle_t xRxDataQueue    = NULL;
static QueueHandle_t xAckLinkQueue   = NULL;
static QueueHandle_t xAckRxQueue     = NULL;

static TaskHandle_t hPkgGen = NULL, hSender = NULL, hDataLink = NULL, hAckLink = NULL, hReceiver = NULL, hAckHandler = NULL;

static SemaphoreHandle_t xAckSem     = NULL;
static SemaphoreHandle_t xRunDoneSem = NULL;
static SemaphoreHandle_t xStatsMutex = NULL;

/* Volatile statistics counters */
static volatile uint32_t gReceivedBytes   = 0UL;
static volatile uint32_t gReceivedCount   = 0UL;
static volatile uint32_t gDroppedAfterMax = 0UL;
static volatile uint32_t gTotalAttempts   = 0UL;
static volatile TickType_t gStartTick     = 0u;
static volatile TickType_t gEndTick       = 0u;

/* Shared variables for Sender and AckHandler coordination */
static volatile uint32_t current_expected_ack_seq = UINT32_MAX;

/* ═══════════════════════════════════════════════════════════════════════════
 * SECTION 4 – HELPERS (Pure Integer Methods)
 * ═══════════════════════════════════════════════════════════════════════════ */
void vTimerCallback(TimerHandle_t xTimer) {
    (void)xTimer;
    /* No action needed, we will use xSemaphoreTake with timeout instead of a timer */
}

uint32_t rand_range(uint32_t min, uint32_t max) {
    return min + (uint32_t)(rand() % (max - min + 1u));
}

int rand_bool_percent(uint32_t percentage) {
    return (uint32_t)(rand() % 100u) < percentage;
}

/* ═══════════════════════════════════════════════════════════════════════════
 * SECTION 5 – CORE TASK IMPLEMENTATIONS
 * ═══════════════════════════════════════════════════════════════════════════ */

/* 1. Packet Generator Task */
void pkgGenTask(void *pvParams) {
    (void)pvParams;
    for (;;) {
        /* Only generate packets if the simulation state is active */
        if (g_sim_running == pdFALSE) {
            vTaskDelay(pdMS_TO_TICKS(50));
            continue;
        }

        uint32_t delay = rand_range(T1_MS, T2_MS);
        vTaskDelay(pdMS_TO_TICKS(delay));

        Packet_t *pkt = (Packet_t *)pvPortMalloc(sizeof(Packet_t));
        if (pkt != NULL) {
            pkt->sender_id = 1u;
            pkt->dest_id   = 2u;
            pkt->seq_num   = g_packet_seq++;
            pkt->length    = (uint16_t)rand_range(L1, L2);
            memset(pkt->payload, 0xAB, sizeof(pkt->payload));

            if (xQueueSend(xGeneratedQueue, &pkt, portMAX_DELAY) != pdTRUE) {
                vPortFree(pkt);
            }
        }
    }
}

/* 2. Sender Task (Send-and-Wait Loop) */
void senderTask(void *pvParams) {
    (void)pvParams;
    for (;;) {
        Packet_t *pkt = NULL;
        if (xQueueReceive(xGeneratedQueue, &pkt, portMAX_DELAY) == pdTRUE) {
            uint8_t attempt_count = 0u;
            BaseType_t packet_acked = pdFALSE;

            while (attempt_count < MAX_RETRIES) {
                Packet_t *pkt_copy = (Packet_t *)pvPortMalloc(sizeof(Packet_t));
                if (pkt_copy != NULL) {
                    memcpy(pkt_copy, pkt, sizeof(Packet_t));

                    xSemaphoreTake(xStatsMutex, portMAX_DELAY);
                    gTotalAttempts++;
                    attempt_count++;
                    current_expected_ack_seq = pkt->seq_num;
                    xSemaphoreGive(xStatsMutex);

                    /* Send packet onto the link */
                    xQueueSend(xTxLinkQueue, &pkt_copy, portMAX_DELAY);

                    /* Block on xAckSem. If we receive it within Tout, it's a success.
                       If we timeout (pdFALSE), we loop and retry. */
                    if (xSemaphoreTake(xAckSem, pdMS_TO_TICKS(current_Tout)) == pdTRUE) {
                        packet_acked = pdTRUE;
                        break; /* Success, break retry loop */
                    }
                }
            }

            vPortFree(pkt); /* Free original packet */

            if (packet_acked == pdFALSE) {
                xSemaphoreTake(xStatsMutex, portMAX_DELAY);
                gDroppedAfterMax++;
                xSemaphoreGive(xStatsMutex);
            }
        }
    }
}

/* 3. ACK Handler Task */
void ackHandlerTask(void *pvParams) {
    (void)pvParams;
    for (;;) {
        ACK_t *ack = NULL;
        if (xQueueReceive(xAckRxQueue, &ack, portMAX_DELAY) == pdTRUE) {
            xSemaphoreTake(xStatsMutex, portMAX_DELAY);
            uint32_t expected = current_expected_ack_seq;
            xSemaphoreGive(xStatsMutex);

            if (ack->seq_num == expected) {
                xSemaphoreGive(xAckSem); /* Wake up the sender task */
            }
            vPortFree(ack);
        }
    }
}

/* 4. Communication Link Task (Data Direction) */
void dataLinkTask(void *pvParams) {
    (void)pvParams;
    for (;;) {
        Packet_t *pkt = NULL;
        if (xQueueReceive(xTxLinkQueue, &pkt, portMAX_DELAY) == pdTRUE) {
            uint32_t delay_ms = D_MS + ((uint32_t)pkt->length * 8UL * 1000UL) / C_BPS;
            vTaskDelay(pdMS_TO_TICKS(delay_ms));

            if (rand_bool_percent(current_P_drop_percent)) {
                vPortFree(pkt);
            } else {
                if (xQueueSend(xRxDataQueue, &pkt, portMAX_DELAY) != pdTRUE) {
                    vPortFree(pkt);
                }
            }
        }
    }
}

/* 5. Communication Link Task (ACK Direction) */
void ackLinkTask(void *pvParams) {
    (void)pvParams;
    for (;;) {
        ACK_t *ack = NULL;
        if (xQueueReceive(xAckLinkQueue, &ack, portMAX_DELAY) == pdTRUE) {
            uint32_t delay_ms = D_MS + (K_BYTES * 8UL * 1000UL) / C_BPS;
            vTaskDelay(pdMS_TO_TICKS(delay_ms));

            if (rand_bool_percent(P_ACK_PERCENT)) {
                vPortFree(ack);
            } else {
                if (xQueueSend(xAckRxQueue, &ack, portMAX_DELAY) != pdTRUE) {
                    vPortFree(ack);
                }
            }
        }
    }
}

/* 6. Receiver Task */
void receiverTask(void *pvParams) {
    (void)pvParams;
    for (;;) {
        Packet_t *pkt = NULL;
        if (xQueueReceive(xRxDataQueue, &pkt, portMAX_DELAY) == pdTRUE) {
            uint32_t seq = pkt->seq_num;
            uint16_t len = pkt->length;

            ACK_t *ack = (ACK_t *)pvPortMalloc(sizeof(ACK_t));
            if (ack != NULL) {
                ack->src_node  = 2u;
                ack->dest_node = 1u;
                ack->seq_num   = seq;
                if (xQueueSend(xAckLinkQueue, &ack, portMAX_DELAY) != pdTRUE) {
                    vPortFree(ack);
                }
            }

            xSemaphoreTake(xStatsMutex, portMAX_DELAY);
            if (seq != g_last_received_seq) {
                if (gReceivedCount == 0UL) {
                    gStartTick = xTaskGetTickCount();
                }
                g_last_received_seq = seq;
                gReceivedBytes     += len;
                gReceivedCount++;

                if (gReceivedCount >= TARGET_PACKETS) {
                    gEndTick = xTaskGetTickCount();
                    xSemaphoreGive(xRunDoneSem);
                }
            }
            xSemaphoreGive(xStatsMutex);

            vPortFree(pkt);
        }
    }
}

/* ═══════════════════════════════════════════════════════════════════════════
 * SECTION 6 – AUTOMATION ENGINE (Safe Pipeline Flush)
 * ═══════════════════════════════════════════════════════════════════════════ */
void resetStats(void) {
    /* 1. Stop generation */
    g_sim_running = pdFALSE;

    /* 2. Wait for all in-flight packets to finish traversing the links (Safe drain) */
    vTaskDelay(pdMS_TO_TICKS(500));

    /* 3. Flush queues */
    Packet_t *pkt; ACK_t *ack;
    while (xQueueReceive(xGeneratedQueue, &pkt, 0) == pdTRUE) { vPortFree(pkt); }
    while (xQueueReceive(xTxLinkQueue,    &pkt, 0) == pdTRUE) { vPortFree(pkt); }
    while (xQueueReceive(xRxDataQueue,    &pkt, 0) == pdTRUE) { vPortFree(pkt); }
    while (xQueueReceive(xAckLinkQueue,   &ack, 0) == pdTRUE) { vPortFree(ack); }
    while (xQueueReceive(xAckRxQueue,     &ack, 0) == pdTRUE) { vPortFree(ack); }

    xSemaphoreTake(xAckSem, 0);
    xSemaphoreTake(xRunDoneSem, 0);

    /* 4. Reset global runtime variables safely */
    xSemaphoreTake(xStatsMutex, portMAX_DELAY);
    g_packet_seq             = 0UL;
    g_last_received_seq      = UINT32_MAX;
    gReceivedBytes           = 0UL;
    gReceivedCount           = 0UL;
    gDroppedAfterMax         = 0UL;
    gTotalAttempts           = 0UL;
    gStartTick               = 0u;
    gEndTick                 = 0u;
    current_expected_ack_seq = UINT32_MAX;
    xSemaphoreGive(xStatsMutex);

    /* 5. Resume generation */
    g_sim_running = pdTRUE;
}

void mainExperimentTask(void *pvParams) {
    (void)pvParams;
    vTaskDelay(pdMS_TO_TICKS(1000u));

    /* Create simulation tasks exactly ONCE here to avoid Heap Corruption */
    xTaskCreate(pkgGenTask,     "PktGenTask", STACK_SIZE_WORDS, NULL, (tskIDLE_PRIORITY + 2u), &hPkgGen);
    xTaskCreate(senderTask,     "SenderTask", STACK_SIZE_WORDS, NULL, (tskIDLE_PRIORITY + 3u), &hSender);
    xTaskCreate(ackHandlerTask, "AckHandler", STACK_SIZE_WORDS, NULL, (tskIDLE_PRIORITY + 3u), &hAckHandler);
    xTaskCreate(dataLinkTask,   "DataLink",   STACK_SIZE_WORDS, NULL, (tskIDLE_PRIORITY + 2u), &hDataLink);
    xTaskCreate(ackLinkTask,    "AckLink",    STACK_SIZE_WORDS, NULL, (tskIDLE_PRIORITY + 2u), &hAckLink);
    xTaskCreate(receiverTask,   "Receiver",   STACK_SIZE_WORDS, NULL, (tskIDLE_PRIORITY + 2u), &hReceiver);

    for (int i = 0; i < 4; i++) {
        current_P_drop_percent = P_drop_percent_array[i];
        current_Tout           = 150u;

        printf("[SIMULATION STARTING] Testing Configuration: P_drop = %s | Tout = 150ms\r\n", P_drop_strings[i]);
        resetStats();

        /* Hold execution here until target metrics are hit safely */
        xSemaphoreTake(xRunDoneSem, portMAX_DELAY);

        xSemaphoreTake(xStatsMutex, portMAX_DELAY);
        uint32_t elapsed_ms = ((uint32_t)(gEndTick - gStartTick) * 1000u) / configTICK_RATE_HZ;
        uint32_t throughput_Bps = (elapsed_ms > 0u) ? (gReceivedBytes * 1000u / elapsed_ms) : 0u;
        uint32_t avg_tx_int   = gTotalAttempts / TARGET_PACKETS;
        uint32_t avg_tx_frac  = ((gTotalAttempts * 100u) / TARGET_PACKETS) % 100u;
        uint32_t final_drops  = gDroppedAfterMax;
        xSemaphoreGive(xStatsMutex);

        throughput_matrix[i]   = throughput_Bps;
        avg_tx_int_matrix[i]   = avg_tx_int;
        avg_tx_frac_matrix[i]  = avg_tx_frac;
        dropped_pkts_matrix[i] = final_drops;

        printf("[METRICS CAPTURED] P_drop = %s | Throughput = %lu Bytes/sec | Avg Tx = %lu.%02lu | Permanent Drops = %lu\r\n\r\n",
               P_drop_strings[i], (unsigned long)throughput_Bps, (unsigned long)avg_tx_int, (unsigned long)avg_tx_frac, (unsigned long)final_drops);
    }

    /* ════════════════════════════════════════════════════════════════════════
     * FINAL REPORT CONSOLIDATED COMPILATION PANELS
     * ════════════════════════════════════════════════════════════════════════ */
    printf("\r\n=========================================================================\r\n");
    printf("                  FINAL RESULTS COMPILATION SUMMARY\r\n");
    printf("=========================================================================\r\n");

    printf("\r\n1. THROUGHPUT MATRIX (Bytes/sec)\r\n");
    printf("---------------------------------------\r\n");
    for (int i = 0; i < 4; i++) {
        printf("P_drop = %s |  %lu\r\n", P_drop_strings[i], (unsigned long)throughput_matrix[i]);
    }

    printf("\r\n2. AVERAGE TRANSMISSIONS PER PACKET (Discussion Question 1)\r\n");
    printf("---------------------------------------\r\n");
    for (int i = 0; i < 4; i++) {
        printf("P_drop = %s |  %lu.%02lu\r\n", P_drop_strings[i], (unsigned long)avg_tx_int_matrix[i], (unsigned long)avg_tx_frac_matrix[i]);
    }

    printf("\r\n3. TOTAL PERMANENT DROPS AFTER 4 EXPIRED TRIES (Discussion Question 2)\r\n");
    printf("---------------------------------------\r\n");
    for (int i = 0; i < 4; i++) {
        printf("P_drop = %s |  %lu\r\n", P_drop_strings[i], (unsigned long)dropped_pkts_matrix[i]);
    }
    printf("=========================================================================\r\n");
    printf("Simulation Execution Completed Successfully.\r\n");

    vTaskSuspend(NULL);
}

/* ═══════════════════════════════════════════════════════════════════════════
 * SECTION 7 – MAIN INITIALIZATION
 * ═══════════════════════════════════════════════════════════════════════════ */
int main(void) {
    srand(12345u);

    xAckSem     = xSemaphoreCreateBinary();
    xRunDoneSem = xSemaphoreCreateBinary();
    xStatsMutex = xSemaphoreCreateMutex(); /* <--- FATAL FLAW FIXED HERE */

    configASSERT(xAckSem     != NULL);
    configASSERT(xRunDoneSem != NULL);
    configASSERT(xStatsMutex != NULL);

    xGeneratedQueue = xQueueCreate(20u, sizeof(Packet_t *));
    xTxLinkQueue    = xQueueCreate(20u, sizeof(Packet_t *));
    xRxDataQueue    = xQueueCreate(20u, sizeof(Packet_t *));
    xAckLinkQueue   = xQueueCreate(20u, sizeof(ACK_t *));
    xAckRxQueue     = xQueueCreate(20u, sizeof(ACK_t *));

    configASSERT(xGeneratedQueue != NULL);
    configASSERT(xTxLinkQueue    != NULL);
    configASSERT(xRxDataQueue    != NULL);
    configASSERT(xAckLinkQueue   != NULL);
    configASSERT(xAckRxQueue     != NULL);

    /* Orchestration Task running above everyone else. */
    xTaskCreate(mainExperimentTask, "MasterExec", STACK_SIZE_WORDS, NULL, (tskIDLE_PRIORITY + 4u), NULL);

    vTaskStartScheduler();
    for (;;);
    return 0;
}

/* ═══════════════════════════════════════════════════════════════════════════
 * SECTION 8 – HOOK FUNCTIONS
 * ═══════════════════════════════════════════════════════════════════════════ */
void vApplicationMallocFailedHook(void) {
    taskDISABLE_INTERRUPTS();
    printf("[CRITICAL ERROR] pvPortMalloc Failed Exception!\r\n");
    for (;;);
}

void vApplicationStackOverflowHook(TaskHandle_t xTask, char *pcTaskName) {
    (void)xTask;
    taskDISABLE_INTERRUPTS();
    printf("[CRITICAL ERROR] Stack Overflow Detected inside task: %s!\r\n", pcTaskName);
    for (;;);
}

void vApplicationIdleHook(void) {}
void vApplicationTickHook(void) {}
