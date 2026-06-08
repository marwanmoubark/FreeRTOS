/*
 * =========================================================================
 * EECG242  –  Spring 2026
 * Network Communication Simulation  (Send-and-Wait Protocol)
 * Phase 1, 2, & 3 (Member 1 Completed)
 * =========================================================================
 */

/* ── Standard headers ───────────────────────────────────────────────────── */
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>

/* ── FreeRTOS headers ───────────────────────────────────────────────────── */
#include "FreeRTOS.h"
#include "task.h"
#include "queue.h"
#include "timers.h"
#include "semphr.h"

/* =========================================================================
 * PHASE 1: ENVIRONMENT SETUP & DATA STRUCTURES 
 * ========================================================================= */

/* ── Global Parameter Defines ───────────────────────────────────────────── */
#define L1          500u       /* Min packet length (bytes) */
#define L2          1500u      /* Max packet length (bytes) */
#define T1_MS       100u       /* Min inter-arrival time (ms) */
#define T2_MS       200u       /* Max inter-arrival time (ms) */
#define K_BYTES     40u        /* ACK packet size (bytes) */
#define C_BITSEC    100000UL   /* Link capacity (bits/sec) */
#define D_MS        5u         /* Propagation delay (ms) */
#define P_ACK       0.01f      /* ACK drop probability */

/* ── Parametric Arrays for Runs ─────────────────────────────────────────── */
const float P_drop_array[4] = {0.01f, 0.02f, 0.04f, 0.08f};
const uint32_t Tout_ticks_array[4] = {
    pdMS_TO_TICKS(150u), 
    pdMS_TO_TICKS(175u), 
    pdMS_TO_TICKS(200u), 
    pdMS_TO_TICKS(225u)
};

/* ── Data Structures ────────────────────────────────────────────────────── */

/* 1. DATA Packet Structure */
typedef struct __attribute__((packed)) {
    uint8_t  sender_id;  /* 1 byte */
    uint8_t  dest_id;    /* 1 byte */
    uint16_t length;     /* 2 bytes */
    uint32_t seq_num;    /* 4 bytes */
    /* Header total = 8 bytes */
    uint8_t  payload[L2 - 8u]; /* Max payload = 1492 bytes */
} Packet_t;

/* 2. ACK Packet Structure */
typedef struct __attribute__((packed)) {
    uint8_t  src_node;   /* 1 byte */
    uint8_t  dest_node;  /* 1 byte */
    uint32_t seq_num;    /* 4 bytes */
    /* Padding to reach K=40 bytes (40 - 6 = 34 bytes) */
    uint8_t  padding[K_BYTES - 6u]; 
} ACK_t;

/* ── Global Queues ──────────────────────────────────────────────────────── */
QueueHandle_t xGeneratedQueue = NULL;  /* PktGen to Sender */

/* Link Data Queues (Tx/Rx) */
QueueHandle_t xTxLinkQueue = NULL;     /* Sender to Link */
QueueHandle_t xRxDataQueue = NULL;     /* Link to Receiver */

/* Link ACK Queues (Tx/Rx) */
QueueHandle_t xAckLinkQueue = NULL;    /* Receiver to Link (ACK) */
QueueHandle_t xAckRxQueue = NULL;      /* Link to Sender (ACK) */

/* Global Drop Probability for current run */
volatile float gCurrentPdrop = 0.01f; 

/* ── Helper Functions ───────────────────────────────────────────────────── */

/* Returns a float uniform random number in [min, max] */
float rand_uniform(float min, float max) {
    float scale = rand() / (float) RAND_MAX; 
    return min + scale * (max - min);
}

/* Returns 1 (true) with probability p */
uint8_t rand_bool(float p) {
    float r = rand() / (float) RAND_MAX;
    return (r < p) ? 1 : 0;
}


/* =========================================================================
 * PHASE 2: PACKET GENERATOR TASK 
 * ========================================================================= */
void pkgGenTask(void *pvParams) {
    for(;;) {
        /* Sequence number defined as static inside the loop as requested */
        static uint32_t seq = 0;

        Packet_t *pkt = (Packet_t *) pvPortMalloc(sizeof(Packet_t));
        
        if(pkt != NULL) {
            pkt->sender_id = 1;
            pkt->dest_id = 2;
            pkt->seq_num = seq++; 
            pkt->length = (uint16_t)rand_uniform(L1, L2);
            
            uint16_t payload_len = pkt->length - 8u; 
            for(uint16_t i = 0; i < payload_len; i++) {
                pkt->payload[i] = 0xAB;
            }

            /* Exact string requested in the PDF */
            printf("PKT GEN: seq=%u len=%u\r\n", (unsigned int)pkt->seq_num, pkt->length);

            /* Send to queue and block indefinitely if full (portMAX_DELAY) */
            xQueueSend(xGeneratedQueue, &pkt, portMAX_DELAY);
        }

        uint32_t delay_ms = (uint32_t)rand_uniform(T1_MS, T2_MS);
        vTaskDelay(pdMS_TO_TICKS(delay_ms));
    }
}


/* =========================================================================
 * PHASE 3: COMMUNICATION LINK SIMULATION 
 * ========================================================================= */

/* ── Task 1: Data Link (Sender -> Receiver) ─────────────────────────────── */
void dataLinkTask(void *pvParams) {
    for(;;) {
        Packet_t *pkt = NULL;
        
        if(xQueueReceive(xTxLinkQueue, &pkt, portMAX_DELAY) == pdTRUE) {
            
            /* Delay = Propagation + Transmission */
            uint32_t delay_ms = D_MS + ((pkt->length * 8UL) / (C_BITSEC / 1000UL));
            vTaskDelay(pdMS_TO_TICKS(delay_ms));
            
            /* Drop Probability */
            if(rand_bool(gCurrentPdrop) == 1) {
                printf("LINK DATA: seq=%lu DROPPED in Link!\r\n", (unsigned long)pkt->seq_num);
                vPortFree(pkt);
            } else {
                if(xQueueSend(xRxDataQueue, &pkt, pdMS_TO_TICKS(100)) != pdTRUE) {
                    vPortFree(pkt);
                } else {
                    printf("LINK DATA: seq=%lu forwarded successfully.\r\n", (unsigned long)pkt->seq_num);
                }
            }
        }
    }
}

/* ── Task 2: ACK Link (Receiver -> Sender) ──────────────────────────────── */
void ackLinkTask(void *pvParams) {
    for(;;) {
        ACK_t *ack = NULL;
        
        if(xQueueReceive(xAckLinkQueue, &ack, portMAX_DELAY) == pdTRUE) {
            
            /* Delay for fixed size K_BYTES */
            uint32_t delay_ms = D_MS + ((K_BYTES * 8UL) / (C_BITSEC / 1000UL));
            vTaskDelay(pdMS_TO_TICKS(delay_ms));
            
            /* ACK Drop Probability (Constant P_ACK) */
            if(rand_bool(P_ACK) == 1) {
                printf("LINK ACK: seq=%lu DROPPED in Link!\r\n", (unsigned long)ack->seq_num);
                vPortFree(ack);
            } else {
                if(xQueueSend(xAckRxQueue, &ack, pdMS_TO_TICKS(100)) != pdTRUE) {
                    vPortFree(ack);
                } else {
                    printf("LINK ACK: seq=%lu forwarded successfully.\r\n", (unsigned long)ack->seq_num);
                }
            }
        }
    }
}


/* =========================================================================
 * MAIN FUNCTION
 * ========================================================================= */
int main(void) {
    /* Initialize random seed */
    srand(12345);

    /* --- Struct Size Verification (Phase 1 Requirement) --- */
    printf("Verification: sizeof(Packet_t) = %u bytes (Expected 1500)\r\n", sizeof(Packet_t));
    printf("Verification: sizeof(ACK_t) = %u bytes (Expected 40)\r\n", sizeof(ACK_t));
    /* ------------------------------------------------------ */

    /* 1. Create All Queues */
    xGeneratedQueue = xQueueCreate(20, sizeof(Packet_t *));
    xTxLinkQueue    = xQueueCreate(20, sizeof(Packet_t *));
    xRxDataQueue    = xQueueCreate(20, sizeof(Packet_t *));
    xAckLinkQueue   = xQueueCreate(20, sizeof(ACK_t *));
    xAckRxQueue     = xQueueCreate(20, sizeof(ACK_t *));
    
    /* Ensure all queues were created successfully */
    if(xGeneratedQueue != NULL && xTxLinkQueue != NULL && xRxDataQueue != NULL && 
       xAckLinkQueue != NULL && xAckRxQueue != NULL) {
        
        /* 2. Create Tasks */
        xTaskCreate(pkgGenTask, "PktGen", 512, NULL, 2, NULL);
        xTaskCreate(dataLinkTask, "LinkData", 512, NULL, 3, NULL);
        xTaskCreate(ackLinkTask, "LinkAck", 512, NULL, 3, NULL);
        
        /* 3. Start Scheduler */
        printf("Starting FreeRTOS Simulation (Phases 1-3)...\r\n");
        vTaskStartScheduler();
    } else {
        printf("Error: Failed to create queues!\r\n");
    }

    /* Should never reach here */
    while(1) {
    }
    return 0;
}


/* =========================================================================
 * FREERTOS HOOK FUNCTIONS 
 * ========================================================================= */
void vApplicationMallocFailedHook(void) {
    printf("Malloc Failed!\r\n");
    while(1);
}

void vApplicationStackOverflowHook(TaskHandle_t xTask, char *pcTaskName) {
    (void)xTask;
    printf("Stack Overflow in task: %s\r\n", pcTaskName);
    while(1);
}

void vApplicationIdleHook(void) {
}

void vApplicationTickHook(void) {
}
