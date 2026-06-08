/*
 * =========================================================================
 * EECG242  -  Spring 2026
 * Network Communication Simulation  (Send-and-Wait Protocol)
 * Full Integrated Code — All Phases
 * =========================================================================
 */

/* ── Standard headers ───────────────────────────────────────────────────── */
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <string.h>                   /* FIX 1: missing — needed for memset/memcpy */

/* ── FreeRTOS headers ───────────────────────────────────────────────────── */
#include "FreeRTOS.h"
#include "task.h"
#include "queue.h"
#include "timers.h"
#include "semphr.h"

/* =========================================================================
 * PHASE 1: ENVIRONMENT SETUP & DATA STRUCTURES
 * ========================================================================= */

#define L1           500u
#define L2           1500u
#define T1_MS        100u
#define T2_MS        200u
#define K_BYTES      40u
#define C_BITSEC     100000UL
#define D_MS         5u
#define P_ACK        0.01f
#define MAX_ATTEMPTS 4u
#define TARGET_PKTS  2000u

/* FIX 2: TARGET_PACKETS was referenced in original but never defined —
 * added alias so both names work                                           */
#define TARGET_PACKETS TARGET_PKTS

const uint32_t Tout_ms_array[4] = {150u, 175u, 200u, 225u};

/* FIX 3: P_drop_array used float but rand_bool/gCurrentPdrop caused
 * FPU faults under QEMU. Converted to integer thresholds:
 * probability * RAND_MAX:  0.01→328, 0.02→655, 0.04→1311, 0.08→2621     */
const int P_drop_int_array[4] = {328, 655, 1311, 2621};

/* Packet structure */
typedef struct __attribute__((packed)) {
    uint8_t  sender_id;
    uint8_t  dest_id;
    uint16_t length;
    uint32_t seq_num;
    uint8_t  payload[L2 - 8u];
} Packet_t;

/* ACK structure */
typedef struct __attribute__((packed)) {
    uint8_t  src_node;
    uint8_t  dest_node;
    uint32_t seq_num;
    uint8_t  padding[K_BYTES - 6u];
} ACK_t;

#define HEADER_SIZE 8u

/* ── Queue Handles ──────────────────────────────────────────────────────── */
static QueueHandle_t xGeneratedQueue = NULL;
static QueueHandle_t xTxLinkQueue    = NULL;
static QueueHandle_t xRxDataQueue    = NULL;
static QueueHandle_t xAckLinkQueue   = NULL;
static QueueHandle_t xAckRxQueue     = NULL;

/* ── Semaphore & Timer Handles ──────────────────────────────────────────── */
static SemaphoreHandle_t xRunDoneSem  = NULL;
static SemaphoreHandle_t xTxMutex    = NULL;
static SemaphoreHandle_t xStatsMutex = NULL;
static SemaphoreHandle_t xSenderReady = NULL;
static TimerHandle_t     xToutTimer  = NULL;

/* FIX 4: xTimerSem was declared but never created or used — removed       */

/* ── Shared TX packet pointer ───────────────────────────────────────────── */
static Packet_t * volatile pxTxPacket = NULL;

/* ── Task Handles ───────────────────────────────────────────────────────── */
static TaskHandle_t xPkgGenTaskHandle   = NULL;
static TaskHandle_t xDataLinkTaskHandle = NULL;
static TaskHandle_t xAckLinkTaskHandle  = NULL;
static TaskHandle_t xSenderTaskHandle   = NULL;
static TaskHandle_t xReceiverTaskHandle = NULL;

/* ── Experiment Run Parameters ──────────────────────────────────────────── */
volatile int      gCurrentPdropInt = 328;   /* integer threshold, no float */
volatile uint32_t gCurrentTout     = 150u;

/* ── Statistics Globals ─────────────────────────────────────────────────── */
volatile uint32_t  gTxTotal         = 0u;
volatile uint32_t  gDroppedAfterMax = 0u;  /* FIX 5: was gDroppedMaxRetry  */
volatile uint32_t  gTotalAttempts   = 0u;
volatile uint32_t  gReceivedCount   = 0u;
volatile uint32_t  gReceivedBytes   = 0u;  /* FIX 6: uint64_t → uint32_t,
                                              QEMU printf can't do %llu    */
volatile uint8_t   ucRetryCnt       = 0u;
volatile TickType_t gStartTick      = 0u;
volatile TickType_t gEndTick        = 0u;
volatile BaseType_t xSimDone        = pdFALSE;

/* ── Helper: integer random range ───────────────────────────────────────── */
/* FIX 7: ulRandInRange was called but never defined                        */
static uint32_t ulRandInRange(uint32_t lo, uint32_t hi)
{
    return lo + (uint32_t)(rand() % (int)(hi - lo + 1u));
}

/* FIX 3 continued: integer drop check — no float, no FPU                  */
static int iShouldDrop(int threshold)
{
    return (rand() < threshold);
}

/* ── Delay calculator ───────────────────────────────────────────────────── */
static uint32_t ulCalcDelayMs(uint32_t length_bytes)
{
    uint32_t tx_ms = (length_bytes * 8UL * 1000UL) / C_BITSEC;
    return D_MS + tx_ms;
}

/* =========================================================================
 * PHASE 2: PACKET GENERATOR TASK
 * ========================================================================= */
void pkgGenTask(void *pvParams)
{
    (void)pvParams;
    uint32_t seq = 0u;

    for (;;)
    {
        /* Pause while experiment controller is resetting */
        while (xSimDone == pdTRUE)
        {
            vTaskDelay(pdMS_TO_TICKS(50u));
        }

        vTaskDelay(pdMS_TO_TICKS(ulRandInRange(T1_MS, T2_MS)));

        if (xSimDone == pdTRUE) { continue; }

        Packet_t *pkt = (Packet_t *) pvPortMalloc(sizeof(Packet_t));

        if (pkt != NULL)
        {
            pkt->sender_id = 1u;
            pkt->dest_id   = 2u;
            pkt->seq_num   = seq;
            /* FIX 3: rand_uniform removed — use integer range instead      */
            pkt->length    = (uint16_t)ulRandInRange(L1, L2);

            uint16_t payload_len = (uint16_t)(pkt->length - HEADER_SIZE);
            memset(pkt->payload, 0xABu, payload_len);

            /* FIX 8: xPacketQueue → xGeneratedQueue (wrong queue name)    */
            if (xQueueSend(xGeneratedQueue, &pkt,
                           pdMS_TO_TICKS(500u)) == pdTRUE)
            {
                seq++;
                
                xSemaphoreTake(xStatsMutex, portMAX_DELAY);
                gTxTotal++;
                xSemaphoreGive(xStatsMutex);
            }
            else
            {
                vPortFree(pkt);
            }
        }
    }
}

/* =========================================================================
 * PHASE 3: COMMUNICATION LINK SIMULATION
 * ========================================================================= */
void dataLinkTask(void *pvParams)
{
    (void)pvParams;

    for (;;)
    {
        Packet_t *pkt = NULL;
        if (xQueueReceive(xTxLinkQueue, &pkt, portMAX_DELAY) == pdTRUE)
        {
            uint32_t delay_ms = ulCalcDelayMs(pkt->length);
            vTaskDelay(pdMS_TO_TICKS(delay_ms));

            if (xSimDone == pdTRUE)
            {
                vPortFree(pkt);
                continue;
            }

            /* FIX 3: rand_bool → iShouldDrop with integer threshold       */
            if (iShouldDrop(gCurrentPdropInt))
            {
                vPortFree(pkt);
            }
            else
            {
                if (xQueueSend(xRxDataQueue, &pkt,
                               pdMS_TO_TICKS(100u)) != pdTRUE)
                {
                    vPortFree(pkt);
                }
            }
        }
    }
}

static void ackLinkTask(void *pvParams)
{
    (void)pvParams;

    /* Integer threshold for fixed P_ACK = 0.01 → 328                     */
    const int ackDropThreshold = (int)(P_ACK * (float)RAND_MAX);

    for (;;)
    {
        ACK_t *ack = NULL;
        if (xQueueReceive(xAckLinkQueue, &ack, portMAX_DELAY) == pdTRUE)
        {
            uint32_t delay_ms = ulCalcDelayMs(K_BYTES);
            vTaskDelay(pdMS_TO_TICKS(delay_ms));

            if (xSimDone == pdTRUE)
            {
                vPortFree(ack);
                continue;
            }

            if (iShouldDrop(ackDropThreshold))
            {
                vPortFree(ack);
            }
            else
            {
                if (xQueueSend(xAckRxQueue, &ack,
                               pdMS_TO_TICKS(100u)) != pdTRUE)
                {
                    vPortFree(ack);
                }
            }
        }
    }
}

/* =========================================================================
 * PHASE 4: SENDER TASK & RETRANSMISSION TIMER
 * ========================================================================= */

/* FIX 9: DEADLOCK — original timer callback took xTxMutex then called
 * xSemaphoreGive(xSenderReady). The senderTask held xSenderReady and
 * waited for xTxMutex. Classic AB-BA deadlock.
 * Fix: timer callback uses taskENTER_CRITICAL (no mutex needed inside
 * a timer callback since timer daemon is single-threaded) and signals
 * the sender task via direct task notification instead of semaphore.      */
void vTimerCallback(TimerHandle_t xTimer)
{
    (void)xTimer;

    /* Hard exit — no allocations during or after reset */
    if (xSimDone == pdTRUE) { return; }
    if (pxTxPacket == NULL) { return; }

    ucRetryCnt++;

    if (ucRetryCnt < MAX_ATTEMPTS)
    {
        Packet_t *net_copy = (Packet_t *) pvPortMalloc(sizeof(Packet_t));
        if (net_copy == NULL)
        {
            /* Heap too low — treat as max retries reached */
            goto discard;
        }

        memcpy(net_copy, pxTxPacket, sizeof(Packet_t));

        xSemaphoreTake(xStatsMutex, 0);
        gTxTotal++;
        xSemaphoreGive(xStatsMutex);

        if (xQueueSend(xTxLinkQueue, &net_copy, 0) != pdTRUE)
        {
            vPortFree(net_copy);
            goto discard;
        }

        xTimerReset(xToutTimer, 0);
        return;   /* timer restarted — wait for next expiry */
    }

discard:
    vPortFree(pxTxPacket);
    pxTxPacket = NULL;
    ucRetryCnt = 0u;

    xSemaphoreTake(xStatsMutex, 0);
    gDroppedAfterMax++;
    xSemaphoreGive(xStatsMutex);

    xSemaphoreGive(xSenderReady);
}

void senderTask(void *pvParams)
{
    (void)pvParams;

    for (;;)
    {
        /* Wait until previous packet is resolved (ACK or max retries)     */
        xSemaphoreTake(xSenderReady, portMAX_DELAY);

        Packet_t *pkt = NULL;
        if (xQueueReceive(xGeneratedQueue, &pkt, portMAX_DELAY) != pdTRUE)
        {
            xSemaphoreGive(xSenderReady);
            continue;
        }

        if (xSimDone == pdTRUE)
        {
            vPortFree(pkt);
            xSemaphoreGive(xSenderReady);
            continue;
        }

        /* Store in TX buffer */
        pxTxPacket = pkt;
        ucRetryCnt = 0u;

        xSemaphoreTake(xStatsMutex, portMAX_DELAY);
        gTotalAttempts++;
        xSemaphoreGive(xStatsMutex);

        /* Send a copy to the link (original stays in TX buffer) */
        Packet_t *net_copy = (Packet_t *) pvPortMalloc(sizeof(Packet_t));
        if (net_copy != NULL)
        {
            memcpy(net_copy, pkt, sizeof(Packet_t));
            xQueueSend(xTxLinkQueue, &net_copy, portMAX_DELAY);
        }

        /* Start the retransmission timeout timer */
        xTimerChangePeriod(xToutTimer,
                           pdMS_TO_TICKS(gCurrentTout), 0u);
        xTimerStart(xToutTimer, 0u);

        /* ── Wait for ACK ──────────────────────────────────────────────
         * FIX 9 continued: sender now blocks on xAckRxQueue directly.
         * When ACK arrives, it stops the timer and signals xSenderReady.
         * This eliminates the mutex/semaphore deadlock entirely.          */
        ACK_t *ack = NULL;
        /* Wait up to (MAX_ATTEMPTS * Tout) before giving up              */
        TickType_t xWait = pdMS_TO_TICKS(
                               (uint32_t)MAX_ATTEMPTS * gCurrentTout + 500u);

        if (xQueueReceive(xAckRxQueue, &ack, xWait) == pdTRUE)
        {
            if (ack != NULL && ack->seq_num == pxTxPacket->seq_num)
            {
                xTimerStop(xToutTimer, 0u);

                vPortFree(ack);
                vPortFree(pxTxPacket);
                pxTxPacket = NULL;
                ucRetryCnt = 0u;
            }
            else if (ack != NULL)
            {
                /* Stale ACK — discard, timer callback will handle retry   */
                vPortFree(ack);
                /* Don't give xSenderReady — timer is still running        */
                continue;
            }
        }
        /* If no ACK within xWait, timer callback already handled retries */

        /* Signal ready for next packet only if TX buffer is clear        */
        if (pxTxPacket == NULL)
        {
            xSemaphoreGive(xSenderReady);
        }
    }
}

/* =========================================================================
 * PHASE 5: RECEIVER TASK & ACK GENERATION
 * ========================================================================= */
void receiverTask(void *pvParams)
{
    (void)pvParams;

    /* FIX 10: last_received_seq initialised to UINT32_MAX not -1
     * because seq_num is uint32_t — assigning -1 to uint32_t gives
     * 0xFFFFFFFF which is the same value but the intent is clearer       */
    uint32_t last_received_seq = UINT32_MAX;

    for (;;)
    {
        Packet_t *pkt = NULL;
        if (xQueueReceive(xRxDataQueue, &pkt, portMAX_DELAY) != pdTRUE)
        {
            continue;
        }

        if (xSimDone == pdTRUE)
        {
            vPortFree(pkt);
            continue;
        }

        uint32_t seq    = pkt->seq_num;
        uint16_t length = pkt->length;

        /* FIX 11: gReceivedCount increment and TARGET check were inside
         * the stats mutex but the ACK send was outside it — moved all
         * stats updates together and fixed the termination logic which
         * checked >= TARGET_PKTS AFTER incrementing, meaning it triggered
         * one packet too late                                             */
        if (seq != last_received_seq)
        {
            last_received_seq = seq;

            xSemaphoreTake(xStatsMutex, portMAX_DELAY);

            if (gReceivedCount == 0u)
            {
                gStartTick = xTaskGetTickCount();
            }

            gReceivedCount++;
            gReceivedBytes += length;

            uint32_t localCount = gReceivedCount;
            xSemaphoreGive(xStatsMutex);

            /* Send ACK for unique packet */
            ACK_t *ack = (ACK_t *) pvPortMalloc(sizeof(ACK_t));
            if (ack != NULL)
            {
                memset(ack, 0, sizeof(ACK_t));
                ack->src_node  = 2u;
                ack->dest_node = 1u;
                ack->seq_num   = seq;

                if (xQueueSend(xAckLinkQueue, &ack,
                               pdMS_TO_TICKS(100u)) != pdTRUE)
                {
                    vPortFree(ack);
                }
            }

            /* Check termination AFTER sending ACK                        */
            if (localCount >= TARGET_PKTS)
            {
                gEndTick = xTaskGetTickCount();
                xSimDone = pdTRUE;
                xSemaphoreGive(xRunDoneSem);
                vTaskSuspend(NULL);   /* suspend self */
            }
        }
        else
        {
            /* Duplicate — still ACK it so sender can clear TX buffer     */
            ACK_t *ack = (ACK_t *) pvPortMalloc(sizeof(ACK_t));
            if (ack != NULL)
            {
                memset(ack, 0, sizeof(ACK_t));
                ack->src_node  = 2u;
                ack->dest_node = 1u;
                ack->seq_num   = seq;

                if (xQueueSend(xAckLinkQueue, &ack,
                               pdMS_TO_TICKS(100u)) != pdTRUE)
                {
                    vPortFree(ack);
                }
            }
        }

        vPortFree(pkt);
        pkt = NULL;

    }   /* FIX 12: original code was missing closing brace for for(;;)   */
}

/* =========================================================================
 * PHASE 6: EXPERIMENT CONTROLLER
 * ========================================================================= */
void resetStats(void)
{
    /* CRITICAL: stop timer BEFORE clearing pxTxPacket.
     * Without this, the timer fires during reset, allocates
     * another copy, and corrupts the just-freed memory.     */
    xTimerStop(xToutTimer, portMAX_DELAY);

    /* Small delay to let any in-flight timer callback finish */
    vTaskDelay(pdMS_TO_TICKS(10u));

    gTxTotal         = 0u;
    gDroppedAfterMax = 0u;
    gTotalAttempts   = 0u;
    gReceivedBytes   = 0u;
    gReceivedCount   = 0u;
    gStartTick       = 0u;
    gEndTick         = 0u;
    xSimDone         = pdFALSE;

    /* Free the TX buffer if a packet was mid-flight */
    if (pxTxPacket != NULL)
    {
        vPortFree(pxTxPacket);
        pxTxPacket = NULL;
    }
    ucRetryCnt = 0u;

    /* Drain all queues — free every pointer inside them */
    Packet_t *tmpPkt = NULL;
    ACK_t    *tmpAck = NULL;

    while (xQueueReceive(xGeneratedQueue, &tmpPkt, 0) == pdTRUE)
        { vPortFree(tmpPkt); tmpPkt = NULL; }
    while (xQueueReceive(xTxLinkQueue,    &tmpPkt, 0) == pdTRUE)
        { vPortFree(tmpPkt); tmpPkt = NULL; }
    while (xQueueReceive(xRxDataQueue,    &tmpPkt, 0) == pdTRUE)
        { vPortFree(tmpPkt); tmpPkt = NULL; }
    while (xQueueReceive(xAckLinkQueue,   &tmpAck, 0) == pdTRUE)
        { vPortFree(tmpAck); tmpAck = NULL; }
    while (xQueueReceive(xAckRxQueue,     &tmpAck, 0) == pdTRUE)
        { vPortFree(tmpAck); tmpAck = NULL; }

    /* Consume any stale semaphore tokens */
    xSemaphoreTake(xRunDoneSem, 0);

    /* Reset sender semaphore to exactly 1 free slot */
    xSemaphoreTake(xSenderReady, 0);
    xSemaphoreGive(xSenderReady);
}

void experimentTask(void *pvParams)
{
    (void)pvParams;

    uint32_t throughput_results[4][4];
    uint32_t dropped_results[4][4];
    uint32_t avg_attempts_int[4][4];
    uint32_t avg_attempts_frac[4][4];
    uint32_t time_sec[4][4];
    uint32_t time_ms[4][4];

    printf("\r\n");
    printf("====================================================\r\n");
    printf("   NETWORK SIMULATION — SEND & WAIT PROTOCOL\r\n");
    printf("   16 Runs | Target: %u packets per run\r\n",
           (unsigned int)TARGET_PKTS);
    printf("====================================================\r\n");
    printf("  P_drop values : 1%%  2%%  4%%  8%%\r\n");
    printf("  Tout   values : 150ms  175ms  200ms  225ms\r\n");
    printf("====================================================\r\n\n");

    int run = 1;

    for (int p_idx = 0; p_idx < 4; p_idx++)
    {
        for (int t_idx = 0; t_idx < 4; t_idx++)
        {
            gCurrentPdropInt = P_drop_int_array[p_idx];
            gCurrentTout     = Tout_ms_array[t_idx];

            /* ── Case header ── */
            printf("----------------------------------------------------\r\n");
            printf("  Case %2d / 16 | Pdrop=%d%%  Tout=%u ms\r\n",
                   run,
                   (p_idx == 0) ? 1 :
                   (p_idx == 1) ? 2 :
                   (p_idx == 2) ? 4 : 8,
                   (unsigned int)gCurrentTout);
            printf("  Status: RUNNING...\r\n");

            resetStats();

            vTaskResume(xPkgGenTaskHandle);
            vTaskResume(xDataLinkTaskHandle);
            vTaskResume(xAckLinkTaskHandle);
            vTaskResume(xSenderTaskHandle);
            vTaskResume(xReceiverTaskHandle);

            /* Block until receiver signals 2000 packets done */
            xSemaphoreTake(xRunDoneSem, portMAX_DELAY);

            vTaskSuspend(xPkgGenTaskHandle);
            vTaskSuspend(xDataLinkTaskHandle);
            vTaskSuspend(xAckLinkTaskHandle);
            vTaskSuspend(xSenderTaskHandle);
            /* receiver suspends itself */

            /* ── Calculate results ── */
            uint32_t elapsedTicks = (uint32_t)(gEndTick - gStartTick);
            uint32_t eSec  = elapsedTicks / (uint32_t)configTICK_RATE_HZ;
            uint32_t eMs   = (elapsedTicks % (uint32_t)configTICK_RATE_HZ)
                             * 1000u / (uint32_t)configTICK_RATE_HZ;
            uint32_t tp    = (eSec > 0u) ? gReceivedBytes / eSec : 0u;
            uint32_t avgI  = gTotalAttempts / TARGET_PKTS;
            uint32_t avgF  = (gTotalAttempts * 100u / TARGET_PKTS) % 100u;

            /* Store for final table */
            throughput_results[p_idx][t_idx] = tp;
            dropped_results[p_idx][t_idx]    = gDroppedAfterMax;
            avg_attempts_int[p_idx][t_idx]   = avgI;
            avg_attempts_frac[p_idx][t_idx]  = avgF;
            time_sec[p_idx][t_idx]           = eSec;
            time_ms[p_idx][t_idx]            = eMs;

            /* ── Case result ── */
            printf("  Status: DONE\r\n");
            printf("  Time        : %u.%03u s\r\n",
                   (unsigned int)eSec, (unsigned int)eMs);
            printf("  Throughput  : %u bytes/sec\r\n",
                   (unsigned int)tp);
            printf("  Avg attempts: %u.%02u per packet\r\n",
                   (unsigned int)avgI, (unsigned int)avgF);
            printf("  Pkt dropped : %u (max retries exceeded)\r\n",
                   (unsigned int)gDroppedAfterMax);

            run++;
        }
    }

    /* ══════════════════════════════════════════════════════
     * FINAL SUMMARY TABLES
     * ══════════════════════════════════════════════════════ */
    printf("\r\n");
    printf("====================================================\r\n");
    printf("          FINAL RESULTS SUMMARY\r\n");
    printf("====================================================\r\n");

    /* ── Table 1: Throughput ── */
    printf("\r\n  [TABLE 1] Throughput (bytes/sec)\r\n");
    printf("  %-8s | %-10s | %-10s | %-10s | %-10s\r\n",
           "Pdrop", "Tout=150", "Tout=175", "Tout=200", "Tout=225");
    printf("  ---------------------------------------------------------\r\n");
    const char *plabels[4] = {"1%","2%","4%","8%"};
    for (int p = 0; p < 4; p++)
    {
        printf("  %-8s | %-10u | %-10u | %-10u | %-10u\r\n",
               plabels[p],
               (unsigned int)throughput_results[p][0],
               (unsigned int)throughput_results[p][1],
               (unsigned int)throughput_results[p][2],
               (unsigned int)throughput_results[p][3]);
    }

    /* ── Table 2: Average transmissions per packet ── */
    printf("\r\n  [TABLE 2] Avg transmissions per packet\r\n");
    printf("  %-8s | %-10s | %-10s | %-10s | %-10s\r\n",
           "Pdrop", "Tout=150", "Tout=175", "Tout=200", "Tout=225");
    printf("  ---------------------------------------------------------\r\n");
    for (int p = 0; p < 4; p++)
    {
        printf("  %-8s | %u.%02u       | %u.%02u       | %u.%02u       | %u.%02u\r\n",
               plabels[p],
               (unsigned int)avg_attempts_int[p][0],
               (unsigned int)avg_attempts_frac[p][0],
               (unsigned int)avg_attempts_int[p][1],
               (unsigned int)avg_attempts_frac[p][1],
               (unsigned int)avg_attempts_int[p][2],
               (unsigned int)avg_attempts_frac[p][2],
               (unsigned int)avg_attempts_int[p][3],
               (unsigned int)avg_attempts_frac[p][3]);
    }

    /* ── Table 3: Packets dropped after max retries ── */
    printf("\r\n  [TABLE 3] Packets dropped (max retries exceeded)\r\n");
    printf("  %-8s | %-10s | %-10s | %-10s | %-10s\r\n",
           "Pdrop", "Tout=150", "Tout=175", "Tout=200", "Tout=225");
    printf("  ---------------------------------------------------------\r\n");
    for (int p = 0; p < 4; p++)
    {
        printf("  %-8s | %-10u | %-10u | %-10u | %-10u\r\n",
               plabels[p],
               (unsigned int)dropped_results[p][0],
               (unsigned int)dropped_results[p][1],
               (unsigned int)dropped_results[p][2],
               (unsigned int)dropped_results[p][3]);
    }

    printf("\r\n====================================================\r\n");
    printf("  All 16 runs complete. Simulation ended.\r\n");
    printf("====================================================\r\n");

    vTaskSuspend(NULL);
}

/* =========================================================================
 * MAIN
 * ========================================================================= */
int main(void)
{
    srand(12345u);

    xTxMutex     = xSemaphoreCreateMutex();
    xStatsMutex  = xSemaphoreCreateMutex();
    xSenderReady = xSemaphoreCreateCounting(1u, 1u);
    xRunDoneSem  = xSemaphoreCreateBinary();

    configASSERT(xTxMutex     != NULL);
    configASSERT(xStatsMutex  != NULL);
    configASSERT(xSenderReady != NULL);
    configASSERT(xRunDoneSem  != NULL);

    xGeneratedQueue = xQueueCreate(30u, sizeof(Packet_t *));
    xTxLinkQueue    = xQueueCreate(30u, sizeof(Packet_t *));
    xRxDataQueue    = xQueueCreate(30u, sizeof(Packet_t *));
    xAckLinkQueue   = xQueueCreate(30u, sizeof(ACK_t *));
    xAckRxQueue     = xQueueCreate(30u, sizeof(ACK_t *));

    configASSERT(xGeneratedQueue != NULL);
    configASSERT(xTxLinkQueue    != NULL);
    configASSERT(xRxDataQueue    != NULL);
    configASSERT(xAckLinkQueue   != NULL);
    configASSERT(xAckRxQueue     != NULL);

    xToutTimer = xTimerCreate("Tout",
                              pdMS_TO_TICKS(150u),
                              pdFALSE,
                              NULL,
                              vTimerCallback);
    configASSERT(xToutTimer != NULL);

    /* Stack sizes increased to 1024 to prevent stack overflow with printf */
    xTaskCreate(pkgGenTask,     "PkgGen",   1024u, NULL, 2u,
                &xPkgGenTaskHandle);
    xTaskCreate(dataLinkTask,   "DataLink", 1024u, NULL, 2u,
                &xDataLinkTaskHandle);
    xTaskCreate(ackLinkTask,    "AckLink",  1024u, NULL, 2u,
                &xAckLinkTaskHandle);
    xTaskCreate(senderTask,     "Sender",   1024u, NULL, 3u,
                &xSenderTaskHandle);
    xTaskCreate(receiverTask,   "Receiver", 1024u, NULL, 3u,
                &xReceiverTaskHandle);
    xTaskCreate(experimentTask, "ExpCtrl",  1024u, NULL, 4u,
                NULL);

    /* All tasks start suspended — experimentTask resumes them per run     */
    vTaskSuspend(xPkgGenTaskHandle);
    vTaskSuspend(xDataLinkTaskHandle);
    vTaskSuspend(xAckLinkTaskHandle);
    vTaskSuspend(xSenderTaskHandle);
    vTaskSuspend(xReceiverTaskHandle);

    printf("====================================================\r\n");
    printf("Starting AUTOMATED simulation — 16 runs\r\n");
    printf("Target = %u pkts per run\r\n", (unsigned int)TARGET_PKTS);
    printf("====================================================\r\n");

    vTaskStartScheduler();

    for (;;) {}
    return 0;
}

/* =========================================================================
 * FREERTOS HOOKS
 * ========================================================================= */
void vApplicationMallocFailedHook(void)
{
    printf("FATAL: Malloc Failed!\r\n");
    for (;;);
}

void vApplicationStackOverflowHook(TaskHandle_t xTask, char *pcTaskName)
{
    (void)xTask;
    printf("FATAL: Stack Overflow in task: %s\r\n", pcTaskName);
    for (;;);
}

void vApplicationIdleHook(void) {}
void vApplicationTickHook(void) {}
