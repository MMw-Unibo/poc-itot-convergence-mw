#include <stdio.h>
#include <signal.h>
#include <string.h>
#include <librdkafka/rdkafka.h>
#include <json-c/json.h>
#include <open62541/plugin/pubsub.h>
#include <open62541/plugin/log_stdout.h>
#include <time.h>

static volatile sig_atomic_t run = 1;
#define TIME_SENT "time_sent"
#define TIME_RECV "time_recv"
#define TIME_SENT_GATEWAY "time_sent_gateway"
#define LATENCY "latency"
#define FILENAME_LENGTH 128
#define GROUP_ID_STR_SIZE 16

#define TOTAL_MESSAGES "TOTAL_MESSAGES"
#define OUTPUT_FILE "OUTPUT_FILE"

struct message_info {
    int64_t num;
    int64_t ts_send;
    int64_t ts_recv;
    int64_t time_send_gateway;
    int64_t gateway_time;
    int64_t latency;
    int64_t jitter;
    int64_t ts_recv_kafka;
    int64_t latency_kafka;
};

size_t total_messages;
char *output_filename;
static struct message_info *messages = NULL;
static int64_t current_msg_num = 0;

/**
 * @brief Signal termination of program
 */
static void stop(int sig) {
    (void) sig;
    run = 0;
}

void save_timestamps_in_file(char *out_filename, char *topic);

void rand_string(char *str, int size);

int main(int argc, char **argv) {
    rd_kafka_t *rk;          /* Consumer instance handle */
    rd_kafka_conf_t *conf;   /* Temporary configuration object */
    rd_kafka_resp_err_t err; /* librdkafka API error code */
    char errstr[512];        /* librdkafka API error reporting buffer */
    const char *brokers;     /* Argument: broker list */
    char **topics;           /* Argument: list of topics to subscribe to */
    int topic_cnt;           /* Number of topics to subscribe to */
    rd_kafka_topic_partition_list_t *subscription; /* Subscribed topics */
    int i;

    /*
     * Argument validation
     */
    if (argc < 3) {
        fprintf(stderr,
                "%% Usage: "
                "%s <broker> <topic1> <topic2>..\n",
                argv[0]);
        return 1;
    }

    brokers = argv[1];
    topics = &argv[2];
    topic_cnt = argc - 2;

    char *endptr;
    total_messages = strtol(getenv(TOTAL_MESSAGES), &endptr, 10);
    output_filename = getenv(OUTPUT_FILE);

    messages = malloc(sizeof(struct message_info) * total_messages);

    /*
     * Create Kafka client configuration place-holder
     */
    conf = rd_kafka_conf_new();

    /* Set bootstrap broker(s) as a comma-separated list of
     * host or host:port (default port 9092).
     * librdkafka will use the bootstrap brokers to acquire the full
     * set of brokers from the cluster. */
    if (rd_kafka_conf_set(conf, "bootstrap.servers", brokers,
                          errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        fprintf(stderr, "%s\n", errstr);
        rd_kafka_conf_destroy(conf);
        return 1;
    }

    char groupid[GROUP_ID_STR_SIZE] = "";
    rand_string(groupid, GROUP_ID_STR_SIZE);
    if (rd_kafka_conf_set(conf, "group.id", groupid,
                          errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        fprintf(stderr, "%s\n", errstr);
        rd_kafka_conf_destroy(conf);
        return 1;
    }

    /* If there is no previously committed offset for a partition
     * the auto.offset.reset strategy will be used to decide where
     * in the partition to start fetching messages.
     * By setting this to earliest the consumer will read all messages
     * in the partition if there was no previously committed offset. */
    if (rd_kafka_conf_set(conf, "auto.offset.reset", "latest",
                          errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        fprintf(stderr, "%s\n", errstr);
        rd_kafka_conf_destroy(conf);
        return 1;
    }

    /*
     * Create consumer instance.
     *
     * NOTE: rd_kafka_new() takes ownership of the conf object
     *       and the application must not reference it again after
     *       this call.
     */
    rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
    if (!rk) {
        fprintf(stderr,
                "%% Failed to create new consumer: %s\n", errstr);
        return 1;
    }

    conf = NULL; /* Configuration object is now owned, and freed,
                      * by the rd_kafka_t instance. */

    /* Redirect all messages from per-partition queues to
     * the main queue so that messages can be consumed with one
     * call from all assigned partitions.
     *
     * The alternative is to poll the main queue (for events)
     * and each partition queue separately, which requires setting
     * up a rebalance callback and keeping track of the assignment:
     * but that is more complex and typically not recommended. */
    rd_kafka_poll_set_consumer(rk);

    /* Convert the list of topics to a format suitable for librdkafka */
    subscription = rd_kafka_topic_partition_list_new(topic_cnt);
    for (i = 0; i < topic_cnt; i++)
        rd_kafka_topic_partition_list_add(subscription,
                                          topics[i],
                /* the partition is ignored
                 * by subscribe() */
                                          RD_KAFKA_PARTITION_UA);

    /* Subscribe to the list of topics */
    err = rd_kafka_subscribe(rk, subscription);
    if (err) {
        fprintf(stderr,
                "%% Failed to subscribe to %d topics: %s\n",
                subscription->cnt, rd_kafka_err2str(err));
        rd_kafka_topic_partition_list_destroy(subscription);
        rd_kafka_destroy(rk);
        return 1;
    }

    fprintf(stderr,
            "%% Subscribed to %d topic(s), "
            "waiting for rebalance and messages...\n",
            subscription->cnt);

    rd_kafka_topic_partition_list_destroy(subscription);

    /* Signal handler for clean shutdown */
    signal(SIGINT, stop);

    /* Subscribing to topics will trigger a group rebalance
     * which may take some time to finish, but there is no need
     * for the application to handle this idle period in a special way
     * since a rebalance may happen at any time.
     * Start polling for messages. */

    UA_DateTime now_time;

    while (run) {
        rd_kafka_message_t *rkm;

        rkm = rd_kafka_consumer_poll(rk, 10);
        if (!rkm) {
            continue; /* Timeout: no message within 10ms,
                                   *  try again. This short timeout allows
                                   *  checking for `run` at frequent intervals.
                                   */
        }

        /* consumer_poll() will return either a proper message
         * or a consumer error (rkm->err is set). */
        if (rkm->err) {
            /* Consumer errors are generally to be considered
             * informational as the consumer will automatically
             * try to recover from all types of errors. */
            fprintf(stderr,
                    "%% Consumer error: %s\n",
                    rd_kafka_message_errstr(rkm));
            rd_kafka_message_destroy(rkm);
            continue;
        }

        now_time = UA_DateTime_now();

        /* Print the message value/payload. */
        struct json_object *object, *time_send_object,  *time_send_gateway_object, *time_recv_object, *latency_object;
        int64_t time_send, time_send_gateway, time_recv, latency;
        if (rkm->payload) {
            object = json_tokener_parse(rkm->payload);

            time_recv_object = json_object_object_get(object, TIME_RECV);
            time_recv = json_object_get_int64(time_recv_object);

            time_send_object = json_object_object_get(object, TIME_SENT);
            time_send = json_object_get_int64(time_send_object);

            time_send_gateway_object = json_object_object_get(object, TIME_SENT_GATEWAY);
            time_send_gateway = json_object_get_int64(time_send_gateway_object);

            latency_object = json_object_object_get(object, LATENCY);
            latency = json_object_get_int64(latency_object);

            messages[current_msg_num].num = current_msg_num + 1;
            messages[current_msg_num].ts_recv = time_recv;
            messages[current_msg_num].ts_send = time_send;
            messages[current_msg_num].time_send_gateway = time_send_gateway;
            messages[current_msg_num].latency = latency;

            messages[current_msg_num].ts_recv_kafka = now_time;
            messages[current_msg_num].latency_kafka = now_time - time_send;

            current_msg_num += 1;

            if (current_msg_num == total_messages) {
                run = UA_FALSE;
            }
        }

        for (int i = 0; i < total_messages - 1; i++) {
            messages[i].gateway_time = 
                messages[i + 1].time_send_gateway - messages[i].ts_recv;
            messages[i].jitter = 
                messages[i + 1].ts_send - messages[i].ts_recv;
        }

        rd_kafka_message_destroy(rkm);
    }

    /* Close the consumer: commit final offsets and leave the group. */
    fprintf(stderr, "%% Closing consumer\n");
    rd_kafka_consumer_close(rk);

    save_timestamps_in_file(output_filename, topics[0]);

    /* Destroy the consumer */
    rd_kafka_destroy(rk);

    return 0;
}

void rand_string(char *str, int size) {
    const char charset[] = "abcdefghijklmnopqrstuvwxyz";
    if (size) {
        --size;
        for (size_t n = 0; n < size; n++) {
            int key = rand() % (int) (sizeof charset - 1);
            str[n] = charset[key];
        }
        str[size] = '\0';
    }
    return;
}

void save_timestamps_in_file(char *out_filename, char *topic) {
    printf("\nSaving timestamps in file...\n");
    time_t t = time(0);
    struct tm *lt = localtime(&t);
    char time_str[64];
    sprintf(time_str, "%04d-%02d-%02d_%02d-%02d-%02d",
            lt->tm_year + 1900, lt->tm_mon + 1, lt->tm_mday,
            lt->tm_hour, lt->tm_min, lt->tm_sec);

    char filename[FILENAME_LENGTH] = {0};
    snprintf(filename, FILENAME_LENGTH - 1, "%s_%s_%s.csv", out_filename, topic, time_str);

    FILE *fd = fopen(filename, "w");
    if (!fd) {
        UA_LOG_ERROR(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND, "Impossible to create CSV file: %s", filename);
    }

    fprintf(fd, "msg_num, ts_send, ts_recv, latency, jitter, time_send_gateway, gateway_time, ts_recv_kafka, latency_kafka\n");

    for (size_t i = 0; i < total_messages; i++) {
        struct message_info *info = &messages[i];
        fprintf(fd, "%ld, %ld, %ld, %ld, %ld, %ld, %ld, %ld, %ld\n",
                info->num, info->ts_send, info->ts_recv,
                info->latency, info->jitter,
                info->time_send_gateway, info->gateway_time,
                info->ts_recv_kafka, info->latency_kafka);
    }

    fclose(fd);
    printf("Terminating...\n");
}