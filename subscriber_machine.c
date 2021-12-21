#include <signal.h>
#include <time.h>
#include <open62541/plugin/pubsub.h>
#include <open62541/plugin/pubsub_udp.h>
#include <open62541/plugin/log_stdout.h>
#include <open62541/server.h>

#include <ua_pubsub_networkmessage.h>
#include <json-c/json.h>

#define TOTAL_MESSAGES "TOTAL_MESSAGES"
#define OUTPUT_FILE "OUTPUT_FILE"

#define FILENAME_LENGTH 128

const char *NETWORK_ADDRESS_URL = "network_address_url";
const char *TRANSPORT_PROFILE = "transport_profile";
const char *PUBLISHERS = "publishers";

struct message_info {
    int64_t num;
    int64_t ts_send;
    int64_t ts_recv;
    int64_t latency;
};

int n_entries;
struct publisher_info {
    char *data_group_name;
    int id;
    int n_registers;
    char **registers;
    double interval;
};

size_t total_messages;
char* output_filename;
static struct message_info* messages =  NULL;
static int64_t current_msg_num = 0;

static UA_Boolean running = UA_TRUE;

void stop_handler(int sig) {
    (void) sig;
    running = UA_FALSE;
}

void save_timestamps_in_file();

UA_StatusCode subscriber_listen(UA_PubSubChannel *channel,
                                struct publisher_info *publisher_infos) {
    UA_ByteString buffer;
    UA_StatusCode retval = UA_ByteString_allocBuffer(&buffer, 512);
    if (retval != UA_STATUSCODE_GOOD) {
        UA_LOG_ERROR(UA_Log_Stdout, UA_LOGCATEGORY_SERVER,
                     "Message buffer allocation failed!");
        return retval;
    }

    retval = channel->receive(channel, &buffer, NULL, 100);
    if (retval != UA_STATUSCODE_GOOD || buffer.length == 0) {
        /* Workaraound! Reset buffer length. Receive can set the length to zero.
         * Then the buffer is not deleted because no memory allocation is
         * assumed.
         */
        buffer.length = 512;
        UA_ByteString_clear(&buffer);

        return UA_STATUSCODE_GOOD;
    }

    UA_DateTime now_time = UA_DateTime_now();

    UA_NetworkMessage network_message = {0};
    size_t current_position = 0;
    UA_NetworkMessage_decodeBinary(&buffer, &current_position, &network_message);

    if (network_message.networkMessageType != UA_NETWORKMESSAGE_DATASET) {
        UA_NetworkMessage_clear(&network_message);
        return retval;
    }

    // At least one DataSetMessage in the NetworkMessage?
    if (network_message.payloadHeaderEnabled
        && network_message.payloadHeader.dataSetPayloadHeader.count < 1) {
        UA_NetworkMessage_clear(&network_message);
        return retval;
    }

    // Check on writer_group_id
    int writer_group_id = network_message.groupHeader.writerGroupId;
    struct publisher_info *publisher_info = NULL;
    for (int i = 0; i < n_entries; ++i) {
        if (publisher_infos[i].id == writer_group_id) {
            publisher_info = &publisher_infos[i];
        }
    }
    if (!publisher_info) {
        return retval;
    }

    for (size_t dataset_index = 0;
         dataset_index < network_message.payloadHeader.dataSetPayloadHeader.count;
         dataset_index++
            ) {
        UA_DataSetMessage *dataset_message =
                &network_message.payload.dataSetPayload.dataSetMessages[dataset_index];

        if (dataset_message->header.dataSetMessageType != UA_DATASETMESSAGE_DATAKEYFRAME) {
            continue;
        }

        if (dataset_message->header.timestampEnabled) {
            UA_UtcTime datetime = dataset_message->header.timestamp;
            messages[current_msg_num].num = current_msg_num + 1;
            messages[current_msg_num].ts_recv = now_time;
            messages[current_msg_num].ts_send = datetime;
            messages[current_msg_num].latency = now_time - datetime;
            current_msg_num += 1;

            if (current_msg_num == total_messages) {
                running = UA_FALSE;
                return retval;
            }
        };


        if (dataset_message->header.fieldEncoding == UA_FIELDENCODING_RAWDATA) {
            // TODO(garbu): Decode RAW payload (doesn't contain fieldCount information)
        } else {
            for (int field_index = 0;
                 field_index < dataset_message->data.keyFrameData.fieldCount;
                 field_index++
                    ) {
                const UA_DataType *current_type =
                        dataset_message->data.keyFrameData
                                .dataSetFields[field_index]
                                .value.type;

                if (current_type == &UA_TYPES[UA_TYPES_INT16]) {
                    UA_Int16 value = *(UA_Int16 *) dataset_message->data.keyFrameData
                            .dataSetFields[field_index]
                            .value
                            .data;
                    UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                                "Message content: [Int16 \tReceived data: %u\t\t register: %s]", value,
                                publisher_info->registers[field_index]);
                } else if (current_type == &UA_TYPES[UA_TYPES_INT32]) {
                    UA_Int32 value = *(UA_Int32 *) dataset_message->data.keyFrameData
                            .dataSetFields[field_index]
                            .value
                            .data;
                    UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                                "Message content: [Int32 \tReceived data: %u\t\t register: %s]", value,
                                publisher_info->registers[field_index]);
                } else if (current_type == &UA_TYPES[UA_TYPES_FLOAT]) {
                    UA_Float value = *(UA_Float *) dataset_message->data.keyFrameData
                            .dataSetFields[field_index]
                            .value
                            .data;
                    UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                                "Message content: [Float \tReceived data: %0.2f\t register: %s]", value,
                                publisher_info->registers[field_index]);
                } else if (current_type == &UA_TYPES[UA_TYPES_UINT32]) {
                    UA_UInt32 value = *(UA_UInt32 *) dataset_message->data.keyFrameData
                            .dataSetFields[field_index]
                            .value
                            .data;
                    UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                                "Message content: [UInt32 \tReceived data: %u\t\t register: %s]", value,
                                publisher_info->registers[field_index]);
                } else {
                    UA_LOG_WARNING(UA_Log_Stdout, UA_LOGCATEGORY_SERVER, "Data type %s currently not defined.",
                                   current_type->typeName);
                }
            }
        }
    }

    UA_ByteString_clear(&buffer);
    UA_NetworkMessage_clear(&network_message);

    return retval;
}


int main(int argc, char *argv[]) {
    signal(SIGINT, stop_handler);
    signal(SIGINT, stop_handler);

    char* endptr;
    total_messages = strtol(getenv(TOTAL_MESSAGES), &endptr, 10);
    output_filename = getenv(OUTPUT_FILE);

    messages = malloc(sizeof(struct message_info) * total_messages);

    if (argc - 1 < 1) {
        UA_LOG_ERROR(UA_Log_Stdout, UA_LOGCATEGORY_SERVER, "N_file %d < 1. Usage: gateway \"JSON pubsub config file\"",
                     argc - 1);
        exit(1);
    }
    char *pubsub_config_file = argv[1];

    UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND, "Simulator file: %s", pubsub_config_file);

    struct json_object *pubsub_json_configs = json_object_from_file(pubsub_config_file);
    //PubSub configuration
    struct lh_table *table = json_object_get_object(pubsub_json_configs);
    struct json_object *object;

    //Read transport_profile
    struct lh_entry *entry = lh_table_lookup_entry(table, TRANSPORT_PROFILE);
    if (!entry) {
        UA_LOG_ERROR(UA_Log_Stdout, UA_LOGCATEGORY_SERVER,
                     "PubSub config file malformed: TRANSPORT_PROFILE not found.");
        exit(1);
    }

    object = (json_object *) entry->v;
    char *transport_profile_string = (char *) json_object_get_string(object);

    //Read network_address_url
    entry = lh_table_lookup_entry(table, NETWORK_ADDRESS_URL);
    if (!entry) {
        UA_LOG_ERROR(UA_Log_Stdout, UA_LOGCATEGORY_SERVER,
                     "PubSub config file malformed: NETWORK_ADDRESS_URL not found."
        );
        exit(1);
    }

    object = (json_object *) entry->v;
    char *network_address = (char *) json_object_get_string(object);

    UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND, "Transport profile string: %s\t network address url: %s",
                transport_profile_string, network_address);

    entry = lh_table_lookup_entry(table, PUBLISHERS);
    if (!entry) {
        UA_LOG_ERROR(UA_Log_Stdout, UA_LOGCATEGORY_SERVER,
                     "PubSub config file malformed: PUBLISHERS not found."
        );
        exit(1);
    }

    object = (json_object *) entry->v;

    n_entries = (int) json_object_array_length(object);

    struct json_object *publisher_obj, *register_obj, *data_group_name_obj, *writer_group_id_obj, *publisher_interval_obj, *publisher_registers_obj;
    struct publisher_info *publisher_infos = (struct publisher_info *)
            malloc(json_object_array_length(object) * sizeof(struct publisher_info));
    for (size_t i = 0; i < json_object_array_length(object); i++) {
        // get the i-th object in medi_array
        publisher_obj = json_object_array_get_idx(object, i);

        // get the name attribute in the i-th object
        data_group_name_obj = json_object_object_get(publisher_obj, "data_group_name");
        writer_group_id_obj = json_object_object_get(publisher_obj, "writer_group_id");
        publisher_interval_obj = json_object_object_get(publisher_obj, "interval");
        publisher_registers_obj = json_object_object_get(publisher_obj, "registers");

        publisher_infos[i].data_group_name = (char *) json_object_get_string(data_group_name_obj);
        publisher_infos[i].id = json_object_get_int(writer_group_id_obj);
        publisher_infos[i].interval = json_object_get_double(publisher_interval_obj);

        publisher_infos[i].n_registers = (int) json_object_array_length(publisher_registers_obj);
        publisher_infos[i].registers = (char **) malloc(publisher_infos[i].n_registers);
        for (size_t j = 0; j < json_object_array_length(publisher_registers_obj); j++) {
            register_obj = json_object_array_get_idx(publisher_registers_obj, j);
            publisher_infos[i].registers[j] = (char *) json_object_get_string(register_obj);
        }

        char log_register[4096], buff[512];
        sprintf(log_register, "Writer group id: %d, data group name: '%s', interval: %f, n_registers: %d, registers: [",
                publisher_infos[i].id, publisher_infos[i].data_group_name, publisher_infos[i].interval,
                publisher_infos[i].n_registers);

        int k;
        for (k = 0; k < publisher_infos[i].n_registers - 1; k++) {
            sprintf(buff, "'%s', ", publisher_infos[i].registers[k]);
            strncat(log_register, buff, sizeof(log_register) - strlen(log_register) - 1);
        }
        sprintf(buff, "'%s']", publisher_infos[i].registers[k]);
        strncat(log_register, buff, sizeof(log_register) - strlen(log_register) - 1);

        UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND, "%s", log_register);
    }

    UA_PubSubTransportLayer udp_layer = UA_PubSubTransportLayerUDPMP();

    UA_PubSubConnectionConfig connection_config = {0};
    connection_config.name = UA_STRING("UADP Connection 1");
    connection_config.transportProfileUri = UA_STRING(transport_profile_string);
    connection_config.enabled = UA_TRUE;

    UA_NetworkAddressUrlDataType network_address_url = {
            .networkInterface = UA_STRING_NULL,
            .url = UA_STRING(network_address),
    };
    UA_Variant_setScalar(
            &connection_config.address, &network_address_url,
            &UA_TYPES[UA_TYPES_NETWORKADDRESSURLDATATYPE]
    );

    UA_PubSubChannel *pub_sub_channel = udp_layer.createPubSubChannel(&connection_config);
    pub_sub_channel->regist(pub_sub_channel, NULL, NULL);

    UA_StatusCode retval = UA_STATUSCODE_GOOD;

    while (running && retval == UA_STATUSCODE_GOOD) {
        retval = subscriber_listen(pub_sub_channel, publisher_infos);
    }

    pub_sub_channel->close(pub_sub_channel);

    save_timestamps_in_file(output_filename);

    free(messages);

    return 0;
}

void save_timestamps_in_file(char* out_filename) {
    printf("\nSaving timestamps in file...\n");
    time_t t = time(0);
    struct tm *lt = localtime(&t);
    char time_str[64];
    sprintf(time_str, "%04d-%02d-%02d_%02d-%02d-%02d",
            lt->tm_year + 1900, lt->tm_mon + 1, lt->tm_mday,
            lt->tm_hour, lt->tm_min, lt->tm_sec);

    char filename[FILENAME_LENGTH] = {0};
    snprintf(filename, FILENAME_LENGTH - 1, "%s_%s.csv", out_filename, time_str);

    FILE *fd = fopen(filename, "w");
    if (!fd) {
        UA_LOG_ERROR(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND, "Impossible to create CSV file: %s", filename);
    }

    fprintf(fd, "msg_num, ts_send, ts_recv, latency\n");

    for (size_t i = 0; i < total_messages; i++) {
        struct message_info *info = &messages[i];
        fprintf(fd, "%ld, %ld, %ld, %ld\n",
                info->num, info->ts_send, info->ts_recv, info->latency);
    }

    fclose(fd);
    printf("\nTerminating...\n");
}
