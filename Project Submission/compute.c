//
//  compute.c
//
//
//  Created by Caleb Davis on 11/11/19.
//

#include <stdio.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ipc.h>
#include <sys/msg.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/errno.h>
#include <assert.h>

int jobsSent = 0;           /* These variables are allocated globally because  */
int jobsReceived = 0;       /* values cannot be passed to an interrupt handler */

/* Type for messages going on the queue */
typedef struct QueueMessage{
    long type;
    int jobid;
    int rowvec;
    int colvec;
    int innerDim;
    int data[100];
} Msg;

/* SIGINT handler to display jobs sent and recieved */
void sigintHandler(int sig_num) {
    signal(SIGINT, sigintHandler);
    printf("Jobs Sent %d Jobs Recieved %d\n", jobsSent, jobsReceived);
    fflush(stdout);
}

/* Computes the dot product for the messages passed to it */
int computeDotProduct (Msg message) {
    int i;
    int sum = 0;
    for (i = 0; i < message.innerDim; i++) {
        sum += message.data[i] * message.data[i+message.innerDim];
    }
    return sum;
}

/* Thread function to read and print output values only */
void* output (void* arg) {
    pthread_mutex_t *lock = (pthread_mutex_t*) arg;
    key_t key = ftok("mcdavis17", 1);
    const int msgid = msgget(key, 0666 | IPC_CREAT);
    if (msgid == -1) {
        printf("could not connect, msgid = -1 %d\n", msgid);
        exit(1);
    }

    /* Infinite loop for each thread to pull from the queue */
    while(1) {

        pthread_mutex_lock(lock);   /* Lock critical section */
        Msg message;
        msgrcv(msgid, &message, sizeof(message.data), 1, 0 /*& IPC_NOWAIT*/);    /* Recieve the messages of type 1 */
        printf("Recieving job id %d type %ld size %zu\n", message.jobid, message.type, sizeof(message.data));
        jobsReceived++;
        pthread_mutex_unlock(lock);
        message.data[0] = computeDotProduct(message);
        printf("Sum for cell %d,%d is is %d\n", message.rowvec, message.colvec, message.data[0]);

    }

}

/* Thread function to read, compute, and send sums */
void* compute(void* arg) {
    pthread_mutex_t *lock = (pthread_mutex_t*) arg;
    key_t key = ftok("mcdavis17", 1);
    const int msgid = msgget(key, 0666 | IPC_CREAT);
    if (msgid == -1) {
        printf("could not connect, msgid = -1 %d\n", msgid);
        exit(1);
    }

    /* Infinite loop for each thread to pull from the queue */
    while(1) {

        pthread_mutex_lock(lock);   /* Lock critical section */
        Msg message;
        msgrcv(msgid, &message, sizeof(Msg), 1, 0 /*& IPC_NOWAIT*/);    /* Recieve the messages of type 1 */

        printf("Recieving job id %d type %ld size %zu\n", message.jobid, message.type, (message.innerDim*2+4) * sizeof(int));
        jobsReceived++;

        /* Update message contents to put back on queue */
        message.type = 2;
        message.data[0] = computeDotProduct(message);
        int rc = msgsnd(msgid, &message, 5 * sizeof(int), 1);
        assert(rc==0);
        printf("Sending job id %d type %ld size %zu (rc=%d)\n", message.jobid, message.type, 5 * sizeof(int), rc);
        jobsSent++;
        pthread_mutex_unlock(lock);

    }
    return NULL;
}

int main(int argc, char* argv[]) {
    int i;
    signal(SIGINT, sigintHandler);
    pthread_t *threads;

    pthread_mutex_t *mutex = malloc(sizeof(pthread_mutex_t));
    int rc = pthread_mutex_init(mutex, NULL);
    assert(rc == 0);

    int numThreads = atoi(argv[1]);
    threads = malloc(numThreads * sizeof(pthread_t));

    /* creates a thread pool based on type of run (-n results in a 'read and output only' run) */
    for (i = 0; i < numThreads; i++) {
        if (argc == 2)
            pthread_create(&(threads[i]), NULL, compute, (void*) mutex);
        else if (argc == 3) {
            printf("argc is %d", argc);
            if ( strcmp (argv[1],  "-n") )
                pthread_create(&(threads[i]), NULL, output, (void*) mutex);
        }
    }

    //SHOULD NOT JOIN THREADS AS EACH THREAD RUNS AN INFINITE WHILE LOOP TO PULL FROM THE QUEUE
    for (i = 0; i < numThreads; i++) {
        pthread_join(threads[i], NULL);
    }
        
    /*should be unreachable*/
    return 0;
}
