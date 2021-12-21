#include <open62541/plugin/log.h>
#include <open62541/plugin/log_stdout.h>
#include <open62541/plugin/pubsub_udp.h>
#include <open62541/server.h>
#include <open62541/server_config_default.h>
#include <open62541/types_generated.h>

#include <json-c/json.h>
#include <time.h>

#define MAX_REGISTER_VALUE 100
#define UPDATE_RATE 1000
#define REGISTER_MAXLEN 128

const char *DESCRIPTION_KEY = "description";
const char *EXPECTED_TYPE_KEY = "expected_type";

const char *PUBLISH_INTERVAL = "publish_interval";
const char *NETWORK_ADDRESS_URL = "network_address_url";
const char *TRANSPORT_PROFILE = "transport_profile";
const char *PUBLISHERS = "publishers";

const char *UNIT_KEY = "unit";
const char *STRING = "string";
const char *UINT_32_TYPE = "unsigned_int";
const char *INT_16_TYPE = "short";
const char *INT_32_TYPE = "int";
const char *FLOAT_32_TYPE = "float";

struct register_info {
    char *id;
    UA_Int32 type;
};

struct register_json_info {
    struct register_info *info;
};

int n_entries;
struct publisher_info {
    char *data_group_name;
    int id;
    int n_registers;
    char **registers;
    double interval;
};

struct writer_group {
    UA_NodeId ident;
    UA_NodeId published_dataset_ident;
    UA_NodeId dataset_writer_ident;
    UA_NodeId *dataset_fields;
};

UA_NodeId connection_ident;

struct writer_group *writer_group_arr;

static void add_pubsub_connection(
        UA_Server *server, UA_String *transport_profile,
        UA_NetworkAddressUrlDataType *network_address_url
) {
    UA_PubSubConnectionConfig connection_config;
    memset(&connection_config, 0, sizeof(connection_config));
    connection_config.name = UA_STRING("UADP Connection 1");
    connection_config.transportProfileUri = *transport_profile;
    connection_config.enabled = UA_TRUE;
    UA_Variant_setScalar(
            &connection_config.address, network_address_url,
            &UA_TYPES[UA_TYPES_NETWORKADDRESSURLDATATYPE]
    );

    // TODO(garbu): Remove static publisherId assignment
    connection_config.publisherId.numeric = 2234;

    UA_Server_addPubSubConnection(server, &connection_config, &connection_ident);
}

static void
add_published_dataset(UA_Server *server, UA_NodeId *published_dataset_ident, struct publisher_info *publisher_info) {
    UA_PublishedDataSetConfig published_dataset_config = {0};
    published_dataset_config.publishedDataSetType = UA_PUBSUB_DATASET_PUBLISHEDITEMS;
    char buf[128];
    snprintf(buf, sizeof(buf) - 1, "PDS %d", publisher_info->id);
    published_dataset_config.name = UA_STRING(buf);

    UA_Server_addPublishedDataSet(server, &published_dataset_config, published_dataset_ident);
}

static void add_dataset_field(UA_Server *server, char *variable_name, UA_NodeId *published_dataset_ident,
                              UA_NodeId *dataset_field_ident) {
    UA_QualifiedName qualified_name = UA_QUALIFIEDNAME(1, variable_name);
    UA_DataSetFieldConfig dataset_field_config = {0};
    dataset_field_config.dataSetFieldType = UA_PUBSUB_DATASETFIELD_VARIABLE;
    dataset_field_config.field.variable.promotedField = UA_FALSE;
    dataset_field_config.field.variable.publishParameters.metaDataProperties = &qualified_name;
    dataset_field_config.field.variable.publishParameters.publishedVariable =
            UA_NODEID_STRING(1, variable_name);
    dataset_field_config.field.variable.publishParameters.attributeId = UA_ATTRIBUTEID_VALUE;

    UA_Server_addDataSetField(
            server, *published_dataset_ident,
            &dataset_field_config, dataset_field_ident
    );
}

static void
add_writer_group(UA_Server *server, struct publisher_info *writer_group_info, UA_NodeId *writer_group_ident) {
    UA_WriterGroupConfig writer_group_config = {0};
    writer_group_config.name = UA_STRING(writer_group_info->data_group_name);
    writer_group_config.publishingInterval = writer_group_info->interval;
    writer_group_config.enabled = UA_FALSE;
    writer_group_config.writerGroupId = writer_group_info->id;
    writer_group_config.encodingMimeType = UA_PUBSUB_ENCODING_UADP;
    writer_group_config.messageSettings.encoding = UA_EXTENSIONOBJECT_DECODED;
    writer_group_config.messageSettings.content.decoded.type =
            &UA_TYPES[UA_TYPES_UADPWRITERGROUPMESSAGEDATATYPE];

    UA_UadpWriterGroupMessageDataType *writer_group_message =
            UA_UadpWriterGroupMessageDataType_new();
    writer_group_message->networkMessageContentMask =
            (UA_UadpNetworkMessageContentMask) UA_UADPNETWORKMESSAGECONTENTMASK_PUBLISHERID
            | (UA_UadpNetworkMessageContentMask) UA_UADPNETWORKMESSAGECONTENTMASK_GROUPHEADER
            | (UA_UadpNetworkMessageContentMask) UA_UADPNETWORKMESSAGECONTENTMASK_WRITERGROUPID
            | (UA_UadpNetworkMessageContentMask) UA_UADPNETWORKMESSAGECONTENTMASK_TIMESTAMP
            | (UA_UadpNetworkMessageContentMask) UA_UADPNETWORKMESSAGECONTENTMASK_PICOSECONDS
            | (UA_UadpNetworkMessageContentMask) UA_UADPNETWORKMESSAGECONTENTMASK_PAYLOADHEADER;
    writer_group_config.messageSettings.content.decoded.data = writer_group_message;

    UA_Server_addWriterGroup(
            server, connection_ident,
            &writer_group_config, writer_group_ident
    );
    UA_Server_setWriterGroupOperational(server, *writer_group_ident);
    UA_UadpWriterGroupMessageDataType_delete(writer_group_message);
}

static void
add_dataset_writer(UA_Server *server, struct publisher_info *writer_group_info,
                   UA_NodeId *writer_group_ident,
                   UA_NodeId *published_dataset_ident,
                   UA_NodeId *dataset_writer_ident) {
    UA_DataSetWriterConfig dataset_writer_config = {0};
    dataset_writer_config.name = UA_STRING("Demo DataSetWriter");
    dataset_writer_config.dataSetWriterId = writer_group_info->id + 1000;
    dataset_writer_config.keyFrameCount = 1;

    UA_Server_addDataSetWriter(
            server, *writer_group_ident,
            *published_dataset_ident, &dataset_writer_config,
            dataset_writer_ident
    );
}

static void add_variable(
        UA_Server *server, const char *register_id,
        const char *description, const char *unit,
        void *value, int ua_type
) {
    (void) unit;
    UA_VariableAttributes attr = UA_VariableAttributes_default;
    attr.displayName = UA_LOCALIZEDTEXT("it-IT", (char *) register_id);
    attr.accessLevel = UA_ACCESSLEVELMASK_READ | UA_ACCESSLEVELMASK_WRITE;
    attr.description = UA_LOCALIZEDTEXT("it-IT", (char *) description);
    attr.dataType = UA_TYPES[ua_type].typeId;
    UA_Variant_setScalar(&attr.value, value, &UA_TYPES[ua_type]);

    UA_NodeId current_node_id = UA_NODEID_STRING(1, (char *) register_id);
    UA_QualifiedName current_name = UA_QUALIFIEDNAME(1, (char *) register_id);
    UA_NodeId parent_node_id = UA_NODEID_NUMERIC(0, UA_NS0ID_OBJECTSFOLDER);
    UA_NodeId parent_reference_node_id = UA_NODEID_NUMERIC(0, UA_NS0ID_ORGANIZES);
    UA_NodeId variable_type_node_id = UA_NODEID_NUMERIC(0, UA_NS0ID_BASEDATAVARIABLETYPE);

    UA_Server_addVariableNode(
            server, current_node_id, parent_node_id,
            parent_reference_node_id, current_name,
            variable_type_node_id, attr, NULL, NULL
    );
}

static void cycle_number_callback(UA_Server *server, void *data) {
    struct register_info *reg_info = (struct register_info *) data;
    UA_NodeId currentNodeId = UA_NODEID_STRING(1, reg_info->id);

    UA_Variant variant;
    UA_StatusCode retval = UA_Server_readValue(server, currentNodeId, &variant);
    if (retval != UA_STATUSCODE_GOOD) {
    }

    switch (reg_info->type) {
        case UA_TYPES_INT16:
        case UA_TYPES_INT32:
        case UA_TYPES_UINT32: {
            int value = *((int *) variant.data);
            if (value >= MAX_REGISTER_VALUE) {
                value = 0;
            } else {
                value += 1;
            }

            UA_Variant new_variant;
            UA_Variant_setScalar(&new_variant, &value, &UA_TYPES[reg_info->type]);
            UA_Server_writeValue(server, currentNodeId, new_variant);

            break;
        }
        case UA_TYPES_FLOAT: {
            float value = *((float *) variant.data);
            if (value >= MAX_REGISTER_VALUE) {
                value = 0;
            } else {
                value += 1.0f + (float) ((drand48()) / RAND_MAX);
            }

            UA_Variant new_variant;
            UA_Variant_setScalar(&new_variant, &value, &UA_TYPES[reg_info->type]);
            UA_Server_writeValue(server, currentNodeId, new_variant);
            break;
        }
        default: {
            UA_LOG_WARNING(UA_Log_Stdout, UA_LOGCATEGORY_SERVER, "[UpdateCallback] unknown data type.");
        }
            return;
    }
}

static volatile UA_Boolean running = true;

static void stop_handler(int sign) {
    (void) sign;
    UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_SERVER, "received ctrl-c");
    running = false;
}

static int run(
        UA_Server *server,
        UA_ServerConfig *config,
        UA_String *transport_profile,
        UA_NetworkAddressUrlDataType *network_address_url,
        struct publisher_info *publisher_infos) {
    UA_PubSubTransportLayer pubsub_transport_layer = UA_PubSubTransportLayerUDPMP();
    UA_ServerConfig_addPubSubTransportLayer(config, &pubsub_transport_layer);

    add_pubsub_connection(server, transport_profile, network_address_url);

    writer_group_arr = (struct writer_group *) calloc(n_entries, sizeof(struct writer_group));

    for (int i = 0; i < n_entries; i++) {
        writer_group_arr[i].dataset_fields = (UA_NodeId *) calloc(publisher_infos[i].n_registers, sizeof(UA_NodeId));

        add_published_dataset(server, &writer_group_arr[i].published_dataset_ident, &publisher_infos[i]);

        for (int j = 0; j < publisher_infos[i].n_registers; j++) {
            add_dataset_field(server, publisher_infos[i].registers[j], &writer_group_arr[i].published_dataset_ident,
                              &writer_group_arr[i].dataset_fields[j]);
        }

        add_writer_group(server, &publisher_infos[i], &writer_group_arr[i].ident);
        add_dataset_writer(server, &publisher_infos[i], &writer_group_arr[i].ident,
                           &writer_group_arr[i].published_dataset_ident,
                           &writer_group_arr[i].dataset_writer_ident);
    }

    UA_StatusCode retval = UA_Server_run(server, &running);

    for (int i = 0; i < n_entries; i++) {
        free(writer_group_arr[i].dataset_fields);
    }
    free(writer_group_arr);

    UA_Server_delete(server);
    return retval == UA_STATUSCODE_GOOD ? EXIT_SUCCESS : EXIT_FAILURE;
}

int main(int argc, char *argv[]) {
    signal(SIGINT, stop_handler);
    signal(SIGTERM, stop_handler);

    if (argc - 1 < 2) {
        UA_LOG_ERROR(UA_Log_Stdout, UA_LOGCATEGORY_SERVER, "N_file %d < 2. Usage: simulator \"JSON pubsub config file\""
                                                           "\"JSON simulator mapping file\""
                                                           "[\"JSON simulator mapping file\"]...", argc - 1);
        exit(1);
    }

    int n_simulator_file = argc - 2;
    char *pubsub_config_file = argv[1];
    UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND, "Simulator file: %s\t Number of config files: %d",
                pubsub_config_file, n_simulator_file);

    struct json_object *pubsub_json_configs = json_object_from_file(pubsub_config_file);
    if (!pubsub_json_configs) {
        UA_LOG_ERROR(UA_Log_Stdout, UA_LOGCATEGORY_SERVER, "JSON File '%s' not parsable.", pubsub_config_file);
        exit(1);
    }

    srand48(time(NULL));
    UA_Server *server = UA_Server_new();
    UA_ServerConfig *config = UA_Server_getConfig(server);
    UA_ServerConfig_setDefault(config);

    struct register_json_info *reg_json_infos =
            (struct register_json_info *) calloc(n_simulator_file, sizeof(struct register_json_info));

    for (int i = 0; i < n_simulator_file; i++) {
        struct json_object *config_obj = json_object_from_file(argv[i + 2]);
        if (!config_obj) {
            UA_LOG_WARNING(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND, "File not found %s", argv[i + 2]);
            continue;
        }

        struct lh_table *table = json_object_get_object(config_obj);

        struct lh_entry *ent;
        struct json_object *object, *tmp_obj;

        reg_json_infos[i].info = (struct register_info *) calloc(table->count, sizeof(struct register_info));

        int reg_count = 0;
        lh_foreach(table, ent) {
            char *register_id = (char *) ent->k;

            object = (struct json_object *) ent->v;
            tmp_obj = json_object_object_get(object, DESCRIPTION_KEY);
            const char *description = json_object_get_string(tmp_obj);

            tmp_obj = json_object_object_get(object, EXPECTED_TYPE_KEY);
            const char *expected_type = json_object_get_string(tmp_obj);
            if (!expected_type) {
                continue;
            }

            tmp_obj = json_object_object_get(object, UNIT_KEY);
            const char *unit = json_object_get_string(tmp_obj);
            int value_int = (int) (((drand48()) / RAND_MAX) * MAX_REGISTER_VALUE);
            float value_float = (float) (((drand48()) / (RAND_MAX)) * MAX_REGISTER_VALUE);
            UA_UInt64 callbackId = 0;

            reg_json_infos[i].info[reg_count].id = register_id;
            char buff[100] = {0};
            if (strcmp(expected_type, STRING) == 0) {
                snprintf(buff, sizeof(buff) - 1, "Default try string: %d", value_int);
                UA_String string_value = UA_STRING(buff);
                add_variable(server, register_id, description, unit, &string_value, UA_TYPES_STRING);
            } else if (strcmp(expected_type, INT_16_TYPE) == 0) {
                reg_json_infos[i].info[reg_count].type = UA_TYPES_INT16;
                add_variable(server, register_id, description, unit, &value_int, UA_TYPES_INT16);
                UA_Server_addRepeatedCallback(server, cycle_number_callback,
                                              (void *) &reg_json_infos[i].info[reg_count],
                                              UPDATE_RATE,
                                              &callbackId); // call every 2s
            } else if (strcmp(expected_type, INT_32_TYPE) == 0) {
                reg_json_infos[i].info[reg_count].type = UA_TYPES_INT32;
                add_variable(server, register_id, description, unit, &value_int, UA_TYPES_INT32);
                UA_Server_addRepeatedCallback(server, cycle_number_callback,
                                              (void *) &reg_json_infos[i].info[reg_count],
                                              UPDATE_RATE,
                                              &callbackId); // call every 2s
            } else if (strcmp(expected_type, FLOAT_32_TYPE) == 0) {
                reg_json_infos[i].info[reg_count].type = UA_TYPES_FLOAT;
                add_variable(server, register_id, description, unit, &value_float, UA_TYPES_FLOAT);
                UA_Server_addRepeatedCallback(server, cycle_number_callback,
                                              (void *) &reg_json_infos[i].info[reg_count],
                                              UPDATE_RATE,
                                              &callbackId); // call every 2s
            } else if (strcmp(expected_type, UINT_32_TYPE) == 0) {
                reg_json_infos[i].info[reg_count].type = UA_TYPES_UINT32;
                add_variable(server, register_id, description, unit, &value_int, UA_TYPES_UINT32);
                UA_Server_addRepeatedCallback(server, cycle_number_callback,
                                              (void *) &reg_json_infos[i].info[reg_count],
                                              UPDATE_RATE,
                                              &callbackId); // call every 2s
            } else {
                UA_LOG_WARNING(UA_Log_Stdout, UA_LOGCATEGORY_SERVER, "Data type %s currently not defined.",
                               expected_type);
            }
            reg_count += 1;
        }
    }

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

    struct json_object *publisher_obj, *register_obj, *data_group_name_obj;
    struct json_object *writer_group_id_obj, *publisher_interval_obj, *publisher_registers_obj;

    struct publisher_info *publisher_infos = (struct publisher_info *)
            calloc(json_object_array_length(object), sizeof(struct publisher_info));

    for (size_t i = 0; i < json_object_array_length(object); i++) {
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
        publisher_infos[i].registers = (char **) calloc(publisher_infos[i].n_registers, sizeof(char *));

        for (size_t j = 0; j < json_object_array_length(publisher_registers_obj); j++) {
            register_obj = json_object_array_get_idx(publisher_registers_obj, j);
            publisher_infos[i].registers[j] = (char *) calloc(REGISTER_MAXLEN, sizeof(char));
            strncpy(publisher_infos[i].registers[j], json_object_get_string(register_obj), REGISTER_MAXLEN - 1);
        }

        char log_register[4096], buff[512];
        snprintf(log_register, sizeof(log_register) - 1,
                 "Writer group id: %d, data group name: '%s', interval: %f, n_registers: %d, registers: [",
                 publisher_infos[i].id, publisher_infos[i].data_group_name, publisher_infos[i].interval,
                 publisher_infos[i].n_registers);

        int k;
        for (k = 0; k < publisher_infos[i].n_registers - 1; k++) {
            snprintf(buff, sizeof(buff) - 1, "'%s', ", publisher_infos[i].registers[k]);
            strncat(log_register, buff, sizeof(log_register) - strnlen(log_register, 4095) - 1);
        }

        sprintf(buff, "'%s']", publisher_infos[i].registers[k]);
        strncat(log_register, buff, sizeof(log_register) - strlen(log_register) - 1);

        UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND, "%s", log_register);
    }

    UA_String transport_profile = UA_STRING(transport_profile_string);
    UA_NetworkAddressUrlDataType network_address_url = {UA_STRING_NULL, UA_STRING(network_address)};

    UA_StatusCode retval = run(server, config, &transport_profile, &network_address_url, publisher_infos);

    for (int file_count = 0; file_count < n_simulator_file; file_count++) {
        free(reg_json_infos[file_count].info);
    }
    free(reg_json_infos);

    for (int i = 0; i < n_entries; i++) {
        for (int j = 0; j < publisher_infos[i].n_registers; j++) {
            free(publisher_infos[i].registers[j]);
        }
        free(publisher_infos[i].registers);
    }
    free(publisher_infos);

    return (int) retval;
}