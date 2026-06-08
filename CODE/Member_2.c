/* =========================================================================
 * PHASE 4: SENDER TASK & RETRANSMISSION TIMER
 * ========================================================================= */

/* Timer Callback — called by FreeRTOS timer daemon when Tout expires.
   Gives xTimerSem to unblock senderTask for retransmission check.          */
void vTimerCallback(TimerHandle_t xTimer) {
    (void)xTimer;
    BaseType_t xHigherPriorityTaskWoken = pdFALSE;
    xSemaphoreGiveFromISR(xTimerSem, &xHigherPriorityTaskWoken);
    portYIELD_FROM_ISR(xHigherPriorityTaskWoken);
}

void senderTask(void *pvParams) {
    (void)pvParams;
    Packet_t *pkt = NULL;
    ACK_t    *ack = NULL;

    for (;;) {
        /* ── Fetch next packet — blocks until pkgGenTask provides one ────── */
        if (xQueueReceive(xGeneratedQueue, &pkt, portMAX_DELAY) == pdTRUE) {

            uint8_t attempt_count = 0u;

            /* ── TRANSMIT LOOP: retry same packet until ACK or max attempts ─ */
            while (1) {

                /* 1. Send packet pointer to the link layer */
                xQueueSend(xTxLinkQueue, &pkt, portMAX_DELAY);

                /* 2. (Re)start one-shot Tout timer */
                xTimerChangePeriod(xToutTimer, pdMS_TO_TICKS(gCurrentTout), 0);
                xTimerStart(xToutTimer, 0);

                /* 3. Count this transmission */
                attempt_count++;
                gTxTotal++;
                gTotalAttempts++;

                /* 4. Wait for ACK (Tout + 50ms safety margin) */
                if (xQueueReceive(xAckRxQueue, &ack,
                    pdMS_TO_TICKS(gCurrentTout + 50u)) == pdTRUE)
                {
                    /* ── ACK arrived: check sequence number ─────────────── */
                    if (ack->seq_num == pkt->seq_num) {
                        /* Valid ACK — stop timer, free memory, next packet */
                        xTimerStop(xToutTimer, 0);
                        xSemaphoreTake(xTimerSem, 0); /* drain any stale token */
                        vPortFree(ack);
                        vPortFree(pkt);
                        printf("SENDER: ACK OK seq=%lu\r\n",
                               (unsigned long)pkt->seq_num);
                        break; /* exit transmit loop */
                    } else {
                        /* Stale/wrong ACK — free and fall through to timeout check */
                        vPortFree(ack);
                    }
                }

                /* 5. Check if Tout fired (semaphore given by callback) */
                if (xSemaphoreTake(xTimerSem, 0) == pdTRUE) {
                    if (attempt_count < MAX_ATTEMPTS) {
                        /* Retransmit — loop back to top of while(1) */
                        printf("SENDER: TIMEOUT seq=%lu attempt=%u — retransmit\r\n",
                               (unsigned long)pkt->seq_num, attempt_count);
                        continue;
                    } else {
                        /* Max attempts reached — abandon packet */
                        printf("SENDER: DROPPED seq=%lu after %u attempts\r\n",
                               (unsigned long)pkt->seq_num, attempt_count);
                        vPortFree(pkt);
                        gDroppedAfterMax++;
                        break; /* exit transmit loop */
                    }
                }
            } /* end while(1) transmit loop */
        }
    } /* end for(;;) */
}


/* =========================================================================
 * PHASE 5: RECEIVER TASK & ACK GENERATION
 * ========================================================================= */
void receiverTask(void *pvParams) {
    (void)pvParams;

    /* UINT32_MAX used as sentinel meaning "no packet received yet" */
    uint32_t last_received_seq = UINT32_MAX;
    Packet_t *pkt = NULL;

    for (;;) {
        /* ── Block until dataLinkTask delivers a packet ───────────────────── */
        if (xQueueReceive(xRxDataQueue, &pkt, portMAX_DELAY) == pdTRUE) {

            /* ── Record start time on very first unique packet of the run ─── */
            if (gReceivedCount == 0u) {
                gStartTick = xTaskGetTickCount();
            }

            /* ── Duplicate detection: S&W is in-order so only last seq repeats */
            if (pkt->seq_num != last_received_seq) {
                /* New unique packet */
                gReceivedBytes   += (uint64_t)pkt->length;
                gReceivedCount++;
                last_received_seq = pkt->seq_num;
                printf("RECEIVER: NEW seq=%lu count=%lu\r\n",
                       (unsigned long)pkt->seq_num,
                       (unsigned long)gReceivedCount);
            } else {
                /* Duplicate — still ACK it (our previous ACK was lost) */
                printf("RECEIVER: DUPLICATE seq=%lu — re-ACKing\r\n",
                       (unsigned long)pkt->seq_num);
            }

            /* ── Build and send ACK (always, even for duplicates) ─────────── */
            ACK_t *ack = (ACK_t *) pvPortMalloc(sizeof(ACK_t));
            if (ack != NULL) {
                ack->src_node  = 2u;
                ack->dest_node = 1u;
                ack->seq_num   = pkt->seq_num;

                /* Zero-fill padding */
                for (uint8_t i = 0u; i < (uint8_t)(K_BYTES - 6u); i++) {
                    ack->padding[i] = 0x00u;
                }

                if (xQueueSend(xAckLinkQueue, &ack, portMAX_DELAY) != pdTRUE) {
                    printf("RECEIVER: ACK queue full\r\n");
                    vPortFree(ack);
                }
            }

            /* ── Free the data packet (done with it after ACK is built) ───── */
            vPortFree(pkt);

            /* ── Check run completion ────────────────────────────────────── */
            if (gReceivedCount >= TARGET_PKTS) {
                gEndTick = xTaskGetTickCount();
                printf("RECEIVER: Run complete — %lu packets received\r\n",
                       (unsigned long)gReceivedCount);

                /* Signal experimentTask that this run is finished */
                xSemaphoreGive(xRunDoneSem);

                /* Reset local state for next run, then suspend self */
                last_received_seq = UINT32_MAX;
                vTaskSuspend(NULL);
            }
        }
    } /* end for(;;) */
}


/* =========================================================================
 * PHASE 6: EXPERIMENT CONTROLLER TASK & STATISTICS
 * ========================================================================= */

/* ── resetStats: called between runs to zero counters and flush queues ───── */
void resetStats(void) {
    /* 1. Zero all global counters */
    gTxTotal         = 0u;
    gDroppedAfterMax = 0u;
    gTotalAttempts   = 0u;
    gReceivedBytes   = 0u;
    gReceivedCount   = 0u;
    gStartTick       = 0u;
    gEndTick         = 0u;

    /* 2. Drain queues manually to free all malloc'd pointers inside them.
          xQueueReset() alone would leak memory — we free each pointer first. */
    Packet_t *tmpPkt = NULL;
    ACK_t    *tmpAck = NULL;

    while (xQueueReceive(xGeneratedQueue, &tmpPkt, 0) == pdTRUE) { vPortFree(tmpPkt); }
    while (xQueueReceive(xTxLinkQueue,    &tmpPkt, 0) == pdTRUE) { vPortFree(tmpPkt); }
    while (xQueueReceive(xRxDataQueue,    &tmpPkt, 0) == pdTRUE) { vPortFree(tmpPkt); }
    while (xQueueReceive(xAckLinkQueue,   &tmpAck, 0) == pdTRUE) { vPortFree(tmpAck); }
    while (xQueueReceive(xAckRxQueue,     &tmpAck, 0) == pdTRUE) { vPortFree(tmpAck); }

    /* 3. Drain semaphores in case a stale token was left */
    xSemaphoreTake(xRunDoneSem, 0);
    xSemaphoreTake(xTimerSem,   0);
}

/* ── experimentTask: orchestrates all 16 parametric runs ────────────────── */
void experimentTask(void *pvParams) {
    (void)pvParams;

    float throughput_results[4][4]; /* [p_idx][t_idx] — 16 values */

    printf("\r\n--- EXPERIMENT STARTED ---\r\n\n");

    /* Outer loop: 4 drop probabilities */
    for (int p_idx = 0; p_idx < 4; p_idx++) {

        /* Inner loop: 4 timeout values */
        for (int t_idx = 0; t_idx < 4; t_idx++) {

            /* 1. Set parameters for this run */
            gCurrentPdrop = P_drop_array[p_idx];
            gCurrentTout  = Tout_ms_array[t_idx];

            printf("\r\n--- RUN [%d][%d]: Pdrop=%.2f  Tout=%lu ms ---\r\n",
                   p_idx, t_idx,
                   (double)gCurrentPdrop,
                   (unsigned long)gCurrentTout);

            /* 2. Reset all counters and flush all queues */
            resetStats();

            /* 3. Resume all operational tasks */
            vTaskResume(xPkgGenTaskHandle);
            vTaskResume(xDataLinkTaskHandle);
            vTaskResume(xAckLinkTaskHandle);
            vTaskResume(xSenderTaskHandle);
            vTaskResume(xReceiverTaskHandle);

            /* 4. Block here until receiverTask signals 2000 unique packets done */
            xSemaphoreTake(xRunDoneSem, portMAX_DELAY);

            /* 5. Suspend all operational tasks so they stop while we compute */
            vTaskSuspend(xPkgGenTaskHandle);
            vTaskSuspend(xDataLinkTaskHandle);
            vTaskSuspend(xAckLinkTaskHandle);
            vTaskSuspend(xSenderTaskHandle);
            /* receiverTask already suspended itself, safe to call again */
            vTaskSuspend(xReceiverTaskHandle);

            /* 6. Calculate statistics */
            float elapsed_sec  = (float)(gEndTick - gStartTick) / (float)configTICK_RATE_HZ;
            float throughput   = (float)gReceivedBytes / elapsed_sec;   /* bytes/sec */
            float avg_attempts = (float)gTotalAttempts / (float)TARGET_PKTS;

            /* 7. Store result */
            throughput_results[p_idx][t_idx] = throughput;

            /* 8. Print run summary */
            printf("RESULT: Pdrop=%.2f Tout=%lu ms | "
                   "Throughput=%.1f B/s | "
                   "AvgAttempts=%.2f | "
                   "Dropped=%lu\r\n",
                   (double)gCurrentPdrop,
                   (unsigned long)gCurrentTout,
                   (double)throughput,
                   (double)avg_attempts,
                   (unsigned long)gDroppedAfterMax);

        } /* end Tout loop */
    } /* end Pdrop loop */

    /* ── Print final formatted table ────────────────────────────────────── */
    printf("\r\n\n================ FINAL THROUGHPUT TABLE (Bytes/sec) =================\r\n");
    printf("%-12s | %-12s | %-12s | %-12s | %-12s\r\n",
           "Pdrop\\Tout", "150 ms", "175 ms", "200 ms", "225 ms");
    printf("---------------------------------------------------------------------\r\n");

    for (int p_idx = 0; p_idx < 4; p_idx++) {
        printf("%-12.2f | %-12.1f | %-12.1f | %-12.1f | %-12.1f\r\n",
               (double)P_drop_array[p_idx],
               (double)throughput_results[p_idx][0],
               (double)throughput_results[p_idx][1],
               (double)throughput_results[p_idx][2],
               (double)throughput_results[p_idx][3]);
    }

    printf("=====================================================================\r\n");
    printf("Experiment complete.\r\n");

    /* Suspend forever — experiment is done */
    vTaskSuspend(NULL);
}


/* =========================================================================
 * MAIN FUNCTION  (single entry point — Member 1's original main removed)
 * ========================================================================= */
int main(void) {
    /* 1. Initialize random seed */
    srand(12345);

    /* 2. Verify struct sizes (Phase 1 requirement) */
    printf("Verification: sizeof(Packet_t) = %u (Expected 1500)\r\n",
           (unsigned int)sizeof(Packet_t));
    printf("Verification: sizeof(ACK_t)    = %u (Expected 40)\r\n",
           (unsigned int)sizeof(ACK_t));

    /* 3. Create all queues */
    xGeneratedQueue = xQueueCreate(20u, sizeof(Packet_t *));
    xTxLinkQueue    = xQueueCreate(20u, sizeof(Packet_t *));
    xRxDataQueue    = xQueueCreate(20u, sizeof(Packet_t *));
    xAckLinkQueue   = xQueueCreate(20u, sizeof(ACK_t *));
    xAckRxQueue     = xQueueCreate(20u, sizeof(ACK_t *));

    /* 4. Create semaphores */
    xTimerSem   = xSemaphoreCreateBinary();
    xRunDoneSem = xSemaphoreCreateBinary();

    /* 5. Create retransmission timer (one-shot, period set per run) */
    xToutTimer = xTimerCreate(
        "Tout",
        pdMS_TO_TICKS(150u),   /* initial period — overwritten per run */
        pdFALSE,               /* one-shot (not auto-reload)            */
        NULL,
        vTimerCallback
    );

    /* 6. Verify all OS objects were created successfully */
    if (xGeneratedQueue == NULL || xTxLinkQueue    == NULL ||
        xRxDataQueue    == NULL || xAckLinkQueue   == NULL ||
        xAckRxQueue     == NULL || xTimerSem       == NULL ||
        xRunDoneSem     == NULL || xToutTimer      == NULL)
    {
        printf("ERROR: Failed to create OS objects!\r\n");
        while (1) {}
    }

    /* 7. Create all operational tasks — suspended immediately.
          experimentTask will resume them one run at a time.              */
    xTaskCreate(pkgGenTask,    "PktGen",   512u, NULL, 2u, &xPkgGenTaskHandle);
    xTaskCreate(dataLinkTask,  "LinkData", 512u, NULL, 3u, &xDataLinkTaskHandle);
    xTaskCreate(ackLinkTask,   "LinkAck",  512u, NULL, 3u, &xAckLinkTaskHandle);
    xTaskCreate(senderTask,    "Sender",   512u, NULL, 2u, &xSenderTaskHandle);
    xTaskCreate(receiverTask,  "Receiver", 512u, NULL, 2u, &xReceiverTaskHandle);

    vTaskSuspend(xPkgGenTaskHandle);
    vTaskSuspend(xDataLinkTaskHandle);
    vTaskSuspend(xAckLinkTaskHandle);
    vTaskSuspend(xSenderTaskHandle);
    vTaskSuspend(xReceiverTaskHandle);

    /* 8. Create experiment controller at higher priority so it orchestrates */
    xTaskCreate(experimentTask, "ExpCtrl", 512u, NULL, 4u, NULL);

    /* 9. Start FreeRTOS scheduler — never returns */
    printf("Starting FreeRTOS Scheduler...\r\n");
    vTaskStartScheduler();

    /* Should never reach here */
    while (1) {}
    return 0;
}


/* =========================================================================
 * FREERTOS HOOK FUNCTIONS  (single set — duplicates removed)
 * ========================================================================= */
void vApplicationMallocFailedHook(void) {
    printf("FATAL: Malloc Failed!\r\n");
    while (1);
}

void vApplicationStackOverflowHook(TaskHandle_t xTask, char *pcTaskName) {
    (void)xTask;
    printf("FATAL: Stack Overflow in task: %s\r\n", pcTaskName);
    while (1);
}

void vApplicationIdleHook(void) {
}

void vApplicationTickHook(void) {
}
