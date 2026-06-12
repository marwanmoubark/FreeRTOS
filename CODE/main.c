#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>

#include "FreeRTOS.h"
#include "task.h"
#include "queue.h"
#include "timers.h"
#include "semphr.h"

/* ── Simulation parameters ─────────────────────────────────────────────── */
#define L1                500u       /* Minimum packet payload length (bytes) */
#define L2                1500u      /* Maximum packet payload length (bytes) */
#define T1_MS             100u       /* Minimum inter-arrival time (ms) */
#define T2_MS             200u       /* Maximum inter-arrival time (ms) */
#define K_BYTES           40u        /* Fixed ACK frame size (bytes) */
#define C_BPS             100000UL   /* Link bit-rate (bits/sec) */
#define D_MS              5u         /* One-way propagation delay (ms) */
#define P_ACK_PERCENT     1u         /* ACK drop probability (%) */
#define MAX_RETRIES       4u         /* Max transmit attempts before packet is abandoned */
#define TARGET_PACKETS    2000u      /* Unique packets to receive before ending a run */
#define STACK_SIZE_WORDS  1024u      /* Stack depth per task (words) */

/* Sweep values for the 4×4 experiment grid */
static const uint32_t P_drop_percent_array[4] = {1u, 2u, 4u, 8u};
static const char*    P_drop_strings[4]        = {"0.01", "0.02", "0.04", "0.08"};
static const uint32_t Tout_array[4]            = {150u, 175u, 200u, 225u};

/* Active run parameters; updated by the experiment loop before each run */
static volatile uint32_t current_P_drop_percent = 1u;
static volatile uint32_t current_Tout           = 150u;
static volatile BaseType_t g_sim_running        = pdFALSE;

/* Sequence tracking shared between generator and receiver */
static volatile uint32_t g_packet_seq      = 0UL;
static volatile uint32_t g_last_received_seq = UINT32_MAX;

/* Result matrices indexed [timeout_index][pdrop_index] */
static uint32_t throughput_matrix[4][4];
static uint32_t avg_tx_int_matrix[4][4];
static uint32_t avg_tx_frac_matrix[4][4];
static uint32_t dropped_pkts_matrix[4][4];

/* ── Data structures ────────────────────────────────────────────────────── */

/* Data packet: 8-byte header followed by a variable-length payload field */
typedef struct __attribute__((packed)) {
    uint8_t  sender_id;
    uint8_t  dest_id;
    uint16_t length;
    uint32_t seq_num;
    uint8_t  payload[L2 - 8u];
} Packet_t;

/* ACK frame: padded to exactly K_BYTES so the link delay calculation is accurate */
typedef struct __attribute__((packed)) {
    uint8_t  src_node;
    uint8_t  dest_node;
    uint32_t seq_num;
    uint8_t  padding[K_BYTES - 6u];
} ACK_t;

/* Events posted to the sender task by the ACK handler and the timeout timer */
typedef enum {
    SENDER_EV_ACK_RECEIVED,
    SENDER_EV_TIMEOUT
} SenderEvent_t;

/* ── RTOS handles ───────────────────────────────────────────────────────── */
static QueueHandle_t xGeneratedQueue   = NULL; /* Generator  → Sender */
static QueueHandle_t xTxLinkQueue      = NULL; /* Sender     → Data link */
static QueueHandle_t xRxDataQueue      = NULL; /* Data link  → Receiver */
static QueueHandle_t xAckLinkQueue     = NULL; /* Receiver   → ACK link */
static QueueHandle_t xAckRxQueue       = NULL; /* ACK link   → ACK handler */
static QueueHandle_t xSenderEventQueue = NULL; /* ACK handler / timer → Sender */

static TaskHandle_t hPkgGen = NULL, hSender = NULL, hDataLink = NULL,
                    hAckLink = NULL, hReceiver = NULL, hAckHandler = NULL;

static TimerHandle_t    xToutTimer  = NULL; /* One-shot retransmit timeout timer */
static SemaphoreHandle_t xRunDoneSem = NULL; /* Signals the experiment loop that TARGET_PACKETS were received */
static SemaphoreHandle_t xStatsMutex = NULL; /* Guards all shared statistic variables */

/* Run statistics – always accessed under xStatsMutex */
static volatile uint32_t gReceivedBytes   = 0UL;
static volatile uint32_t gReceivedCount   = 0UL;
static volatile uint32_t gDroppedAfterMax = 0UL;
static volatile uint32_t gTotalAttempts   = 0UL;
static volatile TickType_t gStartTick     = 0u;
static volatile TickType_t gEndTick       = 0u;

/* Sequence number the sender is currently waiting to be ACK-ed */
static volatile uint32_t current_expected_ack_seq = UINT32_MAX;

/* ── Helpers ────────────────────────────────────────────────────────────── */

/* Called by the one-shot timer when Tout elapses without a valid ACK */
void vTimerCallback(TimerHandle_t xTimer) {
    (void)xTimer;
    SenderEvent_t ev = SENDER_EV_TIMEOUT;
    xQueueSend(xSenderEventQueue, &ev, 0);
}

uint32_t rand_range(uint32_t min, uint32_t max) {
    return min + (uint32_t)(rand() % (max - min + 1u));
}

/* Returns non-zero with the given probability (0–100 %) */
int rand_bool_percent(uint32_t percentage) {
    return (uint32_t)(rand() % 100u) < percentage;
}

/* ── Task implementations ───────────────────────────────────────────────── */

/* Generates packets at random intervals in [T1_MS, T2_MS] while the sim is running */
void pkgGenTask(void *pvParams) {
    (void)pvParams;
    for (;;) {
        if (g_sim_running == pdFALSE) {
            vTaskDelay(pdMS_TO_TICKS(50));
            continue;
        }

        vTaskDelay(pdMS_TO_TICKS(rand_range(T1_MS, T2_MS)));

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

/* Send-and-Wait sender: transmits a packet, waits for ACK or timeout, retries up to MAX_RETRIES */
void senderTask(void *pvParams) {
    (void)pvParams;
    for (;;) {
        Packet_t *pkt = NULL;
        if (xQueueReceive(xGeneratedQueue, &pkt, portMAX_DELAY) == pdTRUE) {
            uint8_t    attempt_count = 0u;
            BaseType_t packet_acked  = pdFALSE;

            while (attempt_count < MAX_RETRIES) {
                if (g_sim_running == pdFALSE) break; /* Abort cleanly during a reset */

                Packet_t *pkt_copy = (Packet_t *)pvPortMalloc(sizeof(Packet_t));
                if (pkt_copy != NULL) {
                    memcpy(pkt_copy, pkt, sizeof(Packet_t));

                    xSemaphoreTake(xStatsMutex, portMAX_DELAY);
                    gTotalAttempts++;
                    attempt_count++;
                    current_expected_ack_seq = pkt->seq_num;
                    xSemaphoreGive(xStatsMutex);

                    /* Discard stale events before arming the timer */
                    SenderEvent_t dump_ev;
                    while (xQueueReceive(xSenderEventQueue, &dump_ev, 0) == pdTRUE);

                    xQueueSend(xTxLinkQueue, &pkt_copy, portMAX_DELAY);
                    xTimerChangePeriod(xToutTimer, pdMS_TO_TICKS(current_Tout), 0);

                    /* Block for at most Tout + 50 ms so a mid-run reset cannot deadlock this task */
                    SenderEvent_t incoming_event;
                    TickType_t xTicksToWait = pdMS_TO_TICKS(current_Tout + 50u);

                    if (xQueueReceive(xSenderEventQueue, &incoming_event, xTicksToWait) == pdTRUE) {
                        if (incoming_event == SENDER_EV_ACK_RECEIVED) {
                            xTimerStop(xToutTimer, 0);
                            packet_acked = pdTRUE;
                            break;
                        }
                        /* SENDER_EV_TIMEOUT: fall through and retransmit */
                    } else {
                        if (g_sim_running == pdFALSE) break;
                    }
                }
            }

            vPortFree(pkt);

            if (packet_acked == pdFALSE && g_sim_running == pdTRUE) {
                xSemaphoreTake(xStatsMutex, portMAX_DELAY);
                gDroppedAfterMax++;
                xSemaphoreGive(xStatsMutex);
            }
        }
    }
}

/* Receives ACKs from the link and notifies the sender if the sequence number matches */
void ackHandlerTask(void *pvParams) {
    (void)pvParams;
    for (;;) {
        ACK_t *ack = NULL;
        if (xQueueReceive(xAckRxQueue, &ack, portMAX_DELAY) == pdTRUE) {
            xSemaphoreTake(xStatsMutex, portMAX_DELAY);
            uint32_t expected = current_expected_ack_seq;
            xSemaphoreGive(xStatsMutex);

            if (ack->seq_num == expected) {
                SenderEvent_t ev = SENDER_EV_ACK_RECEIVED;
                xQueueSend(xSenderEventQueue, &ev, 0);
            }
            vPortFree(ack);
        }
    }
}

/* Models the forward data channel: adds serialisation + propagation delay, drops packets with probability P_drop */
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

/* Models the reverse ACK channel: fixed serialisation + propagation delay, drops ACKs with probability P_ACK_PERCENT */
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

/* Accepts delivered packets, sends an ACK, and accumulates byte/count statistics */
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
                g_last_received_seq = seq;

                /* Count only application payload bytes, excluding the 8-byte header */
                if (len > 8u) {
                    gReceivedBytes += (len - 8u);
                }

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

/* ── Experiment engine ──────────────────────────────────────────────────── */

/* Stops generation, drains all queues, resets statistics, then re-enables generation */
void resetStats(void) {
    g_sim_running = pdFALSE;

    /* Allow in-flight packets to clear the link tasks before flushing */
    vTaskDelay(pdMS_TO_TICKS(500));

    Packet_t *pkt; ACK_t *ack; SenderEvent_t ev;
    while (xQueueReceive(xGeneratedQueue,   &pkt, 0) == pdTRUE) { vPortFree(pkt); }
    while (xQueueReceive(xTxLinkQueue,      &pkt, 0) == pdTRUE) { vPortFree(pkt); }
    while (xQueueReceive(xRxDataQueue,      &pkt, 0) == pdTRUE) { vPortFree(pkt); }
    while (xQueueReceive(xAckLinkQueue,     &ack, 0) == pdTRUE) { vPortFree(ack); }
    while (xQueueReceive(xAckRxQueue,       &ack, 0) == pdTRUE) { vPortFree(ack); }
    while (xQueueReceive(xSenderEventQueue, &ev,  0) == pdTRUE);

    xTimerStop(xToutTimer, 0);
    xSemaphoreTake(xRunDoneSem, 0); /* Consume any pending 'done' signal */

    xSemaphoreTake(xStatsMutex, portMAX_DELAY);
    g_packet_seq             = 0UL;
    g_last_received_seq      = UINT32_MAX;
    gReceivedBytes           = 0UL;
    gReceivedCount           = 0UL;
    gDroppedAfterMax         = 0UL;
    gTotalAttempts           = 0UL;
    gEndTick                 = 0u;
    current_expected_ack_seq = UINT32_MAX;
    gStartTick               = xTaskGetTickCount(); /* Timing starts here */
    xSemaphoreGive(xStatsMutex);

    g_sim_running = pdTRUE;
}

/* Orchestrates the 4×4 parameter sweep and prints per-run and summary results */
void mainExperimentTask(void *pvParams) {
    (void)pvParams;
    vTaskDelay(pdMS_TO_TICKS(1000u));

    xTaskCreate(pkgGenTask,     "PktGenTask", STACK_SIZE_WORDS, NULL, (tskIDLE_PRIORITY + 2u), &hPkgGen);
    xTaskCreate(senderTask,     "SenderTask", STACK_SIZE_WORDS, NULL, (tskIDLE_PRIORITY + 3u), &hSender);
    xTaskCreate(ackHandlerTask, "AckHandler", STACK_SIZE_WORDS, NULL, (tskIDLE_PRIORITY + 3u), &hAckHandler);
    xTaskCreate(dataLinkTask,   "DataLink",   STACK_SIZE_WORDS, NULL, (tskIDLE_PRIORITY + 2u), &hDataLink);
    xTaskCreate(ackLinkTask,    "AckLink",    STACK_SIZE_WORDS, NULL, (tskIDLE_PRIORITY + 2u), &hAckLink);
    xTaskCreate(receiverTask,   "Receiver",   STACK_SIZE_WORDS, NULL, (tskIDLE_PRIORITY + 2u), &hReceiver);

    for (int t = 0; t < 4; t++) {
        current_Tout = Tout_array[t];

        for (int p = 0; p < 4; p++) {
            current_P_drop_percent = P_drop_percent_array[p];

            resetStats();
            xSemaphoreTake(xRunDoneSem, portMAX_DELAY);

            xSemaphoreTake(xStatsMutex, portMAX_DELAY);
            uint32_t elapsed_ms     = ((uint32_t)(gEndTick - gStartTick) * 1000u) / configTICK_RATE_HZ;
            uint32_t throughput_Bps = (elapsed_ms > 0u) ? (gReceivedBytes * 1000u / elapsed_ms) : 0u;

            uint32_t total_unique  = gReceivedCount + gDroppedAfterMax;
            uint32_t avg_tx_int    = (total_unique > 0UL) ? (gTotalAttempts / total_unique) : 0UL;
            uint32_t avg_tx_frac   = (total_unique > 0UL) ? (((gTotalAttempts * 100u) / total_unique) % 100u) : 0UL;

            uint32_t final_drops = gDroppedAfterMax;
            uint32_t final_seq   = g_packet_seq;
            uint32_t final_bytes = gReceivedBytes;
            uint32_t final_trans = gTotalAttempts;
            xSemaphoreGive(xStatsMutex);

            throughput_matrix[t][p]   = throughput_Bps;
            avg_tx_int_matrix[t][p]   = avg_tx_int;
            avg_tx_frac_matrix[t][p]  = avg_tx_frac;
            dropped_pkts_matrix[t][p] = final_drops;

            printf("\r\n--------------------------------------------------\r\n");
            printf("Pdrop                   : %s\r\n", P_drop_strings[p]);
            printf("Tout                    : %lu\r\n", (unsigned long)current_Tout);
            printf("Total Simulation Time   : %lu ms\r\n", (unsigned long)elapsed_ms);
            printf("Total Unique Bytes Rcvd : %lu\r\n", (unsigned long)final_bytes);
            printf("Throughput              : %lu\r\n", (unsigned long)throughput_Bps);
            printf("Total Transmissions     : %lu\r\n", (unsigned long)final_trans);
            printf("Generated Packets       : %lu\r\n", (unsigned long)final_seq);
            printf("Avg TX per packet       : %lu.%02lu\r\n", (unsigned long)avg_tx_int, (unsigned long)avg_tx_frac);
            printf("Dropped (>4 attempts)   : %lu\r\n", (unsigned long)final_drops);
            printf("--------------------------------------------------\r\n");
        }
    }

    printf("\r\n=========================================================================\r\n");
    printf("                    FINAL RESULTS COMPILATION SUMMARY MATRICES\r\n");
    printf("=========================================================================\r\n");

    printf("\r\n1. THROUGHPUT MATRIX (Bytes/sec - Pure User Payload Only)\r\n");
    printf("Timeout \\ P_drop |   0.01   |   0.02   |   0.04   |   0.08   |\r\n");
    printf("-----------------------------------------------------------------\r\n");
    for (int t = 0; t < 4; t++) {
        printf("%lu ms          |  %-7lu |  %-7lu |  %-7lu |  %-7lu |\r\n",
               (unsigned long)Tout_array[t],
               (unsigned long)throughput_matrix[t][0], (unsigned long)throughput_matrix[t][1],
               (unsigned long)throughput_matrix[t][2], (unsigned long)throughput_matrix[t][3]);
    }

    printf("\r\n2. AVERAGE TRANSMISSIONS PER PACKET (Discussion Question 1)\r\n");
    printf("Timeout \\ P_drop |   0.01   |   0.02   |   0.04   |   0.08   |\r\n");
    printf("-----------------------------------------------------------------\r\n");
    for (int t = 0; t < 4; t++) {
        printf("%lu ms          |  %lu.%02lu   |  %lu.%02lu   |  %lu.%02lu   |  %lu.%02lu   |\r\n",
               (unsigned long)Tout_array[t],
               (unsigned long)avg_tx_int_matrix[t][0], (unsigned long)avg_tx_frac_matrix[t][0],
               (unsigned long)avg_tx_int_matrix[t][1], (unsigned long)avg_tx_frac_matrix[t][1],
               (unsigned long)avg_tx_int_matrix[t][2], (unsigned long)avg_tx_frac_matrix[t][2],
               (unsigned long)avg_tx_int_matrix[t][3], (unsigned long)avg_tx_frac_matrix[t][3]);
    }

    printf("\r\n3. TOTAL PERMANENT DROPS AFTER 4 EXPIRED TRIES (Discussion Question 2)\r\n");
    printf("Timeout \\ P_drop |   0.01   |   0.02   |   0.04   |   0.08   |\r\n");
    printf("-----------------------------------------------------------------\r\n");
    for (int t = 0; t < 4; t++) {
        printf("%lu ms          |  %-7lu |  %-7lu |  %-7lu |  %-7lu |\r\n",
               (unsigned long)Tout_array[t],
               (unsigned long)dropped_pkts_matrix[t][0], (unsigned long)dropped_pkts_matrix[t][1],
               (unsigned long)dropped_pkts_matrix[t][2], (unsigned long)dropped_pkts_matrix[t][3]);
    }
    printf("=========================================================================\r\n");
    printf("Simulation Execution Completed Successfully.\r\n");

    vTaskSuspend(NULL);
}

/* ── Initialisation ─────────────────────────────────────────────────────── */
int main(void) {
    srand(54321u);

    xRunDoneSem = xSemaphoreCreateBinary();
    xStatsMutex = xSemaphoreCreateMutex();
    configASSERT(xRunDoneSem != NULL);
    configASSERT(xStatsMutex != NULL);

    xGeneratedQueue   = xQueueCreate(20u, sizeof(Packet_t *));
    xTxLinkQueue      = xQueueCreate(20u, sizeof(Packet_t *));
    xRxDataQueue      = xQueueCreate(20u, sizeof(Packet_t *));
    xAckLinkQueue     = xQueueCreate(20u, sizeof(ACK_t *));
    xAckRxQueue       = xQueueCreate(20u, sizeof(ACK_t *));
    xSenderEventQueue = xQueueCreate(5u,  sizeof(SenderEvent_t));
    configASSERT(xGeneratedQueue   != NULL);
    configASSERT(xTxLinkQueue      != NULL);
    configASSERT(xRxDataQueue      != NULL);
    configASSERT(xAckLinkQueue     != NULL);
    configASSERT(xAckRxQueue       != NULL);
    configASSERT(xSenderEventQueue != NULL);

    /* One-shot software timer; period is overwritten before each use */
    xToutTimer = xTimerCreate("ToutTimer", pdMS_TO_TICKS(150), pdFALSE, (void *)0, vTimerCallback);
    configASSERT(xToutTimer != NULL);

    xTaskCreate(mainExperimentTask, "MasterExec", STACK_SIZE_WORDS, NULL, (tskIDLE_PRIORITY + 4u), NULL);

    vTaskStartScheduler();
    for (;;);
    return 0;
}

/* ── FreeRTOS hook functions ────────────────────────────────────────────── */
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
