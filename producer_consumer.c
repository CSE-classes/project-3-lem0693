/*
*   Use condition variables to implement the producer-consumer algorithm. 
*/

#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

void *producer(void *arg);
void *consumer(void *arg);

pthread_mutex_t total_lock;
pthread_cond_t empty;
pthread_cond_t full;
FILE *file;
int in = 0;
int out = 0;
int size = 0;
char buffer[5];

int main(int argc, char *argv[])
{
    int rc;
    //creating the threads
    pthread_t thread1, thread2;
    pthread_mutex_init(&total_lock, NULL);
    pthread_cond_init(&empty, NULL);
    pthread_cond_init(&full, NULL);



    //thread 1 - producer
    rc=pthread_create(&thread1,NULL,producer,NULL);
	if (rc){
		printf("ERROR: return error from pthread_create() is %d\n", rc);
		exit(-1);
	}
    //thread 2 - consumer
    rc=pthread_create(&thread2,NULL,consumer,NULL);
	if (rc){
		printf("ERROR: return error from pthread_create() is %d\n", rc);
		exit(-1);
	}

    //join the threads
    pthread_join(thread1, NULL);
    pthread_join(thread2, NULL);

}

void *producer(void *arg){
    char ch_in;

    //opens the file message.txt ->
    if((file=fopen("message.txt", "r"))==NULL){
		printf("ERROR: can't open string.txt!\n");
		return 0;
	}

    //Will start a loop that scans every character in message.txt. Locks mutex and waits if the size is too big.
    while((fscanf(file, "%c", &ch_in)) != EOF){
        pthread_mutex_lock(&total_lock);

        //waits for consumer to complete its tasks until it is signalled. prevents thread from waking
        while(5 == size) pthread_cond_wait(&empty, &total_lock);

        //char is put in the buffer based on the global variable in.
        buffer[in] = ch_in; ++in; in = in % 5; size++;

        //signals that the buffer is full.
        pthread_cond_signal(&full);
        pthread_mutex_unlock(&total_lock);
    }

    fclose(file);
    pthread_mutex_lock(&total_lock);

    while(5 == size) pthread_cond_wait(&empty, &total_lock);

    //makes it so that consumer will know when to stop.
    buffer[in] = '\0'; size++;

    pthread_cond_signal(&full);
    pthread_mutex_unlock(&total_lock);
    return NULL;
}

void *consumer(void *arg){
    char ch_out;
    
    //forever loop to keep consumer running
    for(;;){

        //lock mutex, looks to see if size has increased
        pthread_mutex_lock(&total_lock);

        //waits until the buffer is being used.
        while((size == 0)) pthread_cond_wait(&full, &total_lock);

        ch_out = buffer[out]; ++out; out = out % 5; size--;

        //signals buffer is empty
        pthread_cond_signal(&empty);
        pthread_mutex_unlock(&total_lock);

        //stops at the end as intended by producer.
        if(ch_out == '\0') break;
        
        //prints characters
        printf("%c", ch_out);
    }
    return NULL;
}

