//
//  package.c
//
//
//  Created by Caleb Davis on 11/11/19.
//

#include <stdio.h>
#include <pthread.h>
#include <stdlib.h>
#include <ctype.h>
#include <string.h>
#include <sys/ipc.h>
#include <signal.h>
#include <sys/errno.h>
#include <sys/types.h>
#include <sys/msg.h>
#include <assert.h>
#include <semaphore.h>
#include <unistd.h>

int jobsSent = 0;           /* These variables are allocated globally because  */
int jobsReceived = 0;       /* values cannot be passed to an interrupt handler */

typedef struct Matrix {
    int rows;
    int columns;
    int** mx;
} M;

/* Type for messages going on the queue */
typedef struct QueueMessage{
    long type;
    int jobid;
    int rowvec;
    int colvec;
    int innerDim;
    int data[100];
} Msg;

/* Struct for argument to pass to threads */
struct myArg {
    Msg *mssg;
    pthread_mutex_t lock;
    size_t size;
};

/* Function signatures */
int** getMatrix(char*, int* , int*);
void printMatrix(M);
void freeMatrix(M);
void printToFile(M);

/* SIGINT handler to display jobs sent and recieved */
void sigintHandler(int sig_num) {
    signal(SIGINT, sigintHandler);
    printf("Jobs Sent %d Jobs Recieved %d\n", jobsSent, jobsReceived);
    fflush(stdout);
}

/* Thread funciton which takes a message and sends it to the ipc message queue. */
/* It then waits until a return value has been sent and recieves it as well */
void* package(void *arg) {
    key_t key = ftok("mcdavis17", 1);           /* connecting to the message queue */
    int msgid = msgget(key, 0666 | IPC_CREAT);
    if (msgid == -1) {
        printf("could not create msgid\n");
    }

    struct myArg *argument = (struct myArg *) arg;
    pthread_mutex_lock(&argument->lock);         /* locking the lock */
    Msg *msg = argument->mssg;
    Msg messageSend = *msg;

    /* Sending the message to the queue and checking that it succeeds */
    int rc = msgsnd(msgid, &messageSend, (messageSend.innerDim*2 + 5) * sizeof(int), 1); /* Send data of double the size of array plus 5 values in struct */
    assert(rc==0);
    printf("Sending job id %d type %ld size %zu (rc=%d)\n", messageSend.jobid, messageSend.type, (messageSend.innerDim*2+4) * sizeof(int), rc);
    jobsSent++;

    /* Returning a computed value from the message queue */
    Msg messageReturn;
    msgrcv(msgid, &messageReturn, 5 * sizeof(int), 2, 0);
    msg = &messageReturn;
    printf("Recieving job id %d type %ld size %zu\n", messageReturn.jobid, messageReturn.type, (messageReturn.innerDim+1) * sizeof(int));
    jobsReceived++;
    pthread_mutex_unlock(&argument->lock);      /* unlocking the lock */
    
    /* Returning the value to the output matrix */
    void *p = msg;
    return p;
}

int main(int argc, char* argv[]) {
    pthread_mutex_t mutex;
    int i, j, k;
    int seconds = atoi(argv[3]);
    signal(SIGINT, sigintHandler);
    pthread_t *threads;
    int rc = pthread_mutex_init(&mutex, NULL);
    assert(rc == 0);
    
    /* Getting the matrices from the file */
    M matrix1;
    M matrix2;
    matrix1.rows = matrix1.columns = matrix2.rows = matrix2.columns = 0;
    matrix1.mx = getMatrix(argv[1], &matrix1.rows, &matrix1.columns);
    matrix2.mx = getMatrix(argv[2], &matrix2.rows, &matrix2.columns);
    printMatrix(matrix1);
    printMatrix(matrix2);

    /* Allocating space for each thread */
    threads = malloc( (matrix1.rows*matrix2.columns) * sizeof(pthread_t));
    if (matrix1.columns != matrix2.rows) { printf("Error: incompatible matrices\n"); return -1; }
    if (threads == NULL) {
        printf("Error, not allocated\n");
        return -1;
    }

    /* Iterating through each row and column of the matrix, initializing the message with data, and creating
        a thread which will send and return the dot product of each  */
    int count = 0;
    for (i = 0; i < matrix1.rows; i++) {
        for (j = 0; j < matrix2.columns; j++) {
            Msg *message = malloc(sizeof(Msg));
            message->type = 1;
            message->jobid = count;
            message->rowvec = i;
            message->colvec = j;
            message->innerDim = matrix1.columns;

            struct myArg *arg = malloc(sizeof(struct myArg));
            arg->mssg = message;
            arg->lock = mutex;

            for (k = 0; k < matrix1.columns; k++) {
               message->data[k] = matrix1.mx[i][k];
               message->data[message->innerDim+k] = matrix2.mx[k][j];
           }
            int err = pthread_create(&(threads[count++]), NULL, package, (void*) arg);
            if (err != 0) {
                printf("Error, thread number \"%d\" did not create, returned this %d\n", count, err);
                return -1;
            }
            sleep(seconds); /* sleeping for the desired number of seconds */
        }
    }

    /* Returning results from threads to the main function */
    M result;
    result.rows = matrix1.rows;
    result.columns = matrix2.columns;
    void *p = malloc(sizeof(Msg));
    result.mx = malloc( (result.rows) * sizeof(int*));
    for (i = 0; i < result.rows; i++) {
        result.mx[i] = malloc (result.columns * sizeof(int));
    }
    for (i = 0; i < result.rows*result.columns; i++) {
        if (pthread_join(threads[i], &p) != 0) {
            printf("Thread %d was not joined", i);
        }
        Msg *msg = malloc(sizeof(Msg));
        msg = (Msg*)p;
        Msg message = *msg;

        result.mx[message.rowvec][message.colvec] = message.data[0];
    }

    printMatrix(result);
    printToFile(result);
    return 0;
}

//    FUNCTION TO GET A MATRIX FROM A FILE
int** getMatrix(char* file, int *rows, int *columns) {
    int i, j, k;
    FILE* matrixFile = fopen(file, "r");
    char *c = malloc(sizeof(char*));
    int element;
    
    //GETTING THE NUMBER OF ROWS AND COLUMNS IN THE MATRIX
    fscanf(matrixFile, "%s", c);
    *rows = atoi(c);
    fscanf(matrixFile, "%s", c);
    *columns = atoi(c);
    
    //DYNAMICALLY ALLOCATING DATA FOR THE MATRIX SO IT IS ON THE HEAP
    int **matrix = malloc(*rows * sizeof(int *));
    for(i = 0; i < *rows; i++)
        matrix[i] = malloc(*columns * sizeof(int));
    
    //FILLING THE MATRIX WITH VALUES FROM THE FILE
    for (j = 0; j < *rows; j++) {
        for (k = 0; k < *columns; k++) {
            fscanf(matrixFile, "%s", c);
            element = atoi(c);
            matrix[j][k] = element;
        }
    }
    free(c);
    fclose(matrixFile);
    return matrix;
}

void printToFile(M matrix) {
    int j, k;
    FILE* file = fopen("m_output.dat", "w");

    //fprintf(file, "%d %d\n", matrix.rows, matrix.columns);
    for (j = 0; j < matrix.rows; j++) {
        for (k = 0; k < matrix.columns; k++) {
            fprintf(file, "%d ", matrix.mx[j][k]);
        }
        printf("\n");
    }
}

/* Prints a matrix to the console */
void printMatrix(M matrix) {
    int j, k;
    //PRINTING THE MATRIX
    printf("%d %d\n", matrix.rows, matrix.columns);
    for (j = 0; j < matrix.rows; j++) {
        for (k = 0; k < matrix.columns; k++) {
            printf("%d ", matrix.mx[j][k]);
        }
    }
}
