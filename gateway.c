#include <signal.h>

#include <open62541/plugin/pubsub.h>
#include <open62541/plugin/pubsub_udp.h>
#include <open62541/plugin/log_stdout.h>
#include <open62541/server.h>
#include <ua_pubsub_networkmessage.h>
#include <json-c/json.h>

#include <librdkafka/rdkafka.h>
#include <time.h>

#define KAFKA_URL "KAFKA_URL"

const char *JSON_KAFKA_ID = "id";
const char *JSON_KAFKA_VALUE = "value";
const char *JSON_KAFKA_TYPE = "type";
const char *JSON_TIME_SENT = "time_sent";
const char *JSON_TIME_SENT_GATEWAY = "time_sent_gateway";
const char *JSON_TIME_RECV = "time_recv";
const char *JSON_LATENCY = "latency";

const char *NETWORK_ADDRESS_URL = "network_address_url";
const char *TRANSPORT_PROFILE = "transport_profile";
const char *PUBLISHERS = "publishers";

static UA_Boolean running = UA_TRUE;

char *kafka_url;

int n_entries;
struct publisher_info {
    char *data_group_name;
    int id;
    int n_registers;
    char **registers;
    double interval;
    UA_DateTime last_send_time; 
};

void stop_handler(int sig) {
    (void) sig;
    running = UA_FALSE;
}

UA_DateTime last_send_time = 0; 

static void kafka_dr_msg_cb(rd_kafka_t *rk, const rd_kafka_message_t *rkmessage, void *opaque);

int send_to_kafka(rd_kafka_t *rk, struct publisher_info *publisher_info, struct json_object *jobj) {
    const char *buf = json_object_to_json_string(jobj);
    unsigned long len = strlen(buf);

    int err = rd_kafka_producev(
            /* Producer handle */
            rk,
            /* Topic name */
            RD_KAFKA_V_TOPIC(publisher_info->data_group_name),
            /* Make a copy of the payload. */
            RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
            /* Message value and length */
            RD_KAFKA_V_VALUE((void *) buf, len),
            /* Per-Message opaque, provided in
                * delivery report callback as
                * msg_opaque. */
            RD_KAFKA_V_OPAQUE(NULL),
            /* End sentinel */
            RD_KAFKA_V_END
    );

    if (err) {
        fprintf(stderr,
                "%% Failed to produce to topic %s: %s\n", publisher_info->data_group_name, rd_kafka_err2str(err));
    } else {
        UA_LOG_DEBUG(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND, "JSON Obj sent: %s", json_object_to_json_string(jobj));
    }

    rd_kafka_poll(rk, 0/*non-blocking*/);

    publisher_info->last_send_time = UA_DateTime_now();

    return err;
}

UA_StatusCode subscriber_listen(
        UA_PubSubChannel *channel,
        struct publisher_info *publisher_infos,
        rd_kafka_t *rk) {
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

        struct json_object *jobj = json_object_new_object();

        if (dataset_message->header.timestampEnabled) {
            UA_UtcTime datetime = dataset_message->header.timestamp;
            json_object_object_add(jobj, JSON_TIME_SENT, json_object_new_int64(datetime));
            json_object_object_add(jobj, JSON_TIME_RECV, json_object_new_int64(now_time));
            json_object_object_add(jobj, JSON_LATENCY, json_object_new_int64(now_time - datetime));
        };

        json_object_object_add(jobj, JSON_TIME_SENT_GATEWAY, json_object_new_int64(publisher_info->last_send_time));

        if (dataset_message->header.fieldEncoding == UA_FIELDENCODING_RAWDATA) {
            // TODO(garbu): Decode RAW payload (doesn't contain fieldCount information)
        } else {
            for (int field_index = 0;
                 field_index < dataset_message->data.keyFrameData.fieldCount;
                 field_index++) {
                const UA_DataType *current_type =
                        dataset_message->data.keyFrameData
                                .dataSetFields[field_index]
                                .value.type;

                struct json_object *json_sub_obj = json_object_new_object();

                json_object_object_add(json_sub_obj, JSON_KAFKA_ID,
                                       json_object_new_string(publisher_info->registers[field_index]));

                if (current_type == &UA_TYPES[UA_TYPES_INT16]) {
                    UA_Int16 value = *(UA_Int16 *) dataset_message->data.keyFrameData
                            .dataSetFields[field_index]
                            .value
                            .data;
                    json_object_object_add(json_sub_obj, JSON_KAFKA_VALUE, json_object_new_int(value));
                    json_object_object_add(json_sub_obj, JSON_KAFKA_TYPE, json_object_new_string("Int16"));
                } else if (current_type == &UA_TYPES[UA_TYPES_INT32]) {
                    UA_Int32 value = *(UA_Int32 *) dataset_message->data.keyFrameData
                            .dataSetFields[field_index]
                            .value
                            .data;
                    json_object_object_add(json_sub_obj, JSON_KAFKA_VALUE, json_object_new_int(value));
                    json_object_object_add(json_sub_obj, JSON_KAFKA_TYPE, json_object_new_string("Int32"));
                } else if (current_type == &UA_TYPES[UA_TYPES_FLOAT]) {
                    UA_Float value = *(UA_Float *) dataset_message->data.keyFrameData
                            .dataSetFields[field_index]
                            .value
                            .data;
                    json_object_object_add(json_sub_obj, JSON_KAFKA_VALUE, json_object_new_double(value));
                    json_object_object_add(json_sub_obj, JSON_KAFKA_TYPE, json_object_new_string("Float"));
                } else if (current_type == &UA_TYPES[UA_TYPES_UINT32]) {
                    UA_UInt32 value = *(UA_UInt32 *) dataset_message->data.keyFrameData
                            .dataSetFields[field_index]
                            .value
                            .data;
                    json_object_object_add(json_sub_obj, JSON_KAFKA_VALUE, json_object_new_int64(value));
                    json_object_object_add(json_sub_obj, JSON_KAFKA_TYPE, json_object_new_string("UInt32"));
                } else {
                    UA_LOG_WARNING(UA_Log_Stdout, UA_LOGCATEGORY_SERVER, "Data type %s currently not defined.",
                                   current_type->typeName);
                }
                json_object_object_add(jobj, publisher_info->registers[field_index], json_sub_obj);
            }

            send_to_kafka(rk, publisher_info, jobj);
        }
    }

    UA_ByteString_clear(&buffer);
    UA_NetworkMessage_clear(&network_message);

    return retval;
}

int main(int argc, char *argv[]) {
    signal(SIGINT, stop_handler);

    kafka_url = getenv(KAFKA_URL);

    if (argc - 1 < 1 || !kafka_url) {
        UA_LOG_ERROR(UA_Log_Stdout, UA_LOGCATEGORY_SERVER,
                     "N_file %d < 1. Usage: gateway \"JSON pubsub config file\". %s must be set as env variable.",
                     argc - 1, KAFKA_URL);
        exit(1);
    }

    char *pubsub_config_file = argv[1];

    UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                "Simulator file: %s\t Kafka URL: %s",
                pubsub_config_file, kafka_url);

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
        publisher_infos[i].last_send_time = 0;

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

    // Create a Kafka connection
    rd_kafka_conf_t *kafka_conf = rd_kafka_conf_new();

    char kafka_err_str[512];
    if (rd_kafka_conf_set(kafka_conf, "bootstrap.servers", kafka_url,
                          kafka_err_str, sizeof(kafka_err_str)) != RD_KAFKA_CONF_OK) {
        UA_LOG_ERROR(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                     "kafka set error: %s", kafka_err_str);
        goto clean;
    }

    rd_kafka_conf_set_dr_msg_cb(kafka_conf, kafka_dr_msg_cb);

    rd_kafka_t *rk = rd_kafka_new(
            RD_KAFKA_PRODUCER, kafka_conf,
            kafka_err_str, sizeof(kafka_err_str)
    );
    if (!rk) {
        UA_LOG_ERROR(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND, "kafka FAILED to create new producer: %s", kafka_err_str);
        goto clean;
    }

    UA_StatusCode retval = UA_STATUSCODE_GOOD;

    while (running && retval == UA_STATUSCODE_GOOD) {
        retval = subscriber_listen(pub_sub_channel, publisher_infos, rk);
    }

    clean:
    pub_sub_channel->close(pub_sub_channel);
    return 0;
}

/**
 * @brief Message delivery report callback.
 *
 * This callback is called exactly once per message, indicating if
 * the message was succesfully delivered
 * (rkmessage->err == RD_KAFKA_RESP_ERR_NO_ERROR) or permanently
 * failed delivery (rkmessage->err != RD_KAFKA_RESP_ERR_NO_ERROR).
 *
 * The callback is triggered from rd_kafka_poll() and executes on
 * the application's thread.
 */
static void kafka_dr_msg_cb(rd_kafka_t *rk,
                            const rd_kafka_message_t *rkmessage, void *opaque) {
    (void) rk;
    (void) opaque;
    if (rkmessage->err)
        fprintf(stderr, "%% Message delivery failed: %s\n",
                rd_kafka_err2str(rkmessage->err));
    else
    {
        // fprintf(stderr, "%% Message delivered (%zd bytes, partition %"PRId32")\n",
        //     rkmessage->len, rkmessage->partition);
    }

    /* The rkmessage is destroyed automatically by librdkafka */
}
