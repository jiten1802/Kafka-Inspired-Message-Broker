#include "KafkaApis.h"
#include "MessageDefs.h"

KafkaApis::KafkaApis(const Fd& _client_fd, const TCPManager& _tcp_manager,
    const ClusterMetadata& _cluster_metadata)
    : client_fd(_client_fd), tcp_manager(_tcp_manager),
    cluster_metadata(_cluster_metadata) {
}

void KafkaApis::classifyRequest(const char* buf, const size_t buf_size) const {
    RequestHeader request_header = RequestHeader::fromBuffer(buf, buf_size);

    std::cout << "Received Request: " << request_header.toString() << "\n";

    switch (request_header.request_api_key) {
    case API_VERSIONS_REQUEST:
        checkApiVersions(buf, buf_size);
        break;
    case DESCRIBE_TOPIC_PARTITIONS_REQUEST:
        describeTopicPartitions(buf, buf_size);
        break;
    case FETCH_REQUEST:
        fetchTopicMessages(buf, buf_size);
        break;
    default:
        std::cout << "Unsupported API key: " << request_header.request_api_key
            << "\n";
    }
}

void KafkaApis::checkApiVersions(const char* buf, const size_t buf_size) const {
    ApiVersionsRequestMessage request_message =
        ApiVersionsRequestMessage::fromBuffer(buf, buf_size);

    std::cout << "Received API Versions Request: " << request_message.toString()
        << "\n";

    ApiVersionsResponseMessage api_versions_response_message;
    api_versions_response_message.corellation_id =
        request_message.corellation_id;

    if (request_message.request_api_version < 0 ||
        request_message.request_api_version > 4) {
        api_versions_response_message.error_code = UNSUPPORTED_VERSION;
        std::cout << "Unsupported version: "
            << request_message.request_api_version << "\n";
    }
    else {
        std::cout << "Supported version: "
            << request_message.request_api_version << "\n";

        {
            ApiVersionsResponseMessage::ApiKey api_key;
            api_key.api_key = API_VERSIONS_REQUEST;
            api_key.min_version = 3;
            api_key.max_version = 4;
            api_versions_response_message.api_keys.push_back(api_key);
        }

        {
            ApiVersionsResponseMessage::ApiKey api_key;
            api_key.api_key = DESCRIBE_TOPIC_PARTITIONS_REQUEST;
            api_key.min_version = 0;
            api_key.max_version = 0;
            api_versions_response_message.api_keys.push_back(api_key);
        }

        {
            ApiVersionsResponseMessage::ApiKey api_key;
            api_key.api_key = FETCH_REQUEST;
            api_key.min_version = 0;
            api_key.max_version = 16;
            api_versions_response_message.api_keys.push_back(api_key);
        }
    }

    tcp_manager.writeBufferOnClientFd(client_fd, api_versions_response_message);
}

void KafkaApis::describeTopicPartitions(const char* buf,
    const size_t buf_size) const {
    DescribeTopicPartitionsRequest request_message =
        DescribeTopicPartitionsRequest::fromBuffer(buf, buf_size);

    std::cout << "Received Describe Topic Partitions Request: "
        << request_message.toString() << "\n";

    DescribeTopicPartitionsResponse describe_topic_partitions_response;

    describe_topic_partitions_response.corellation_id =
        request_message.corellation_id;

    for (const auto& topic : request_message.topics) {
        DescribeTopicPartitionsResponse::Topic topic1;
        topic1.topic_name = topic.topic_name;

        const std::string topic_name_str =
            std::string(topic.topic_name.toString());
        if (cluster_metadata.topic_name_uuid_map.contains(topic_name_str)) {
            topic1.error_code = NO_ERROR;
            topic1.topic_uuid =
                cluster_metadata.topic_name_uuid_map.at(topic_name_str);

            for (const Value::PartitionRecord& partition_record :
                cluster_metadata.topic_uuid_partition_id_map.at(
                    topic1.topic_uuid)) {
                DescribeTopicPartitionsResponse::Partition partition;

                partition.error_code = NO_ERROR;
                partition.partition_id = partition_record.partition_id;
                partition.leader_id = partition_record.leader_id;
                partition.leader_epoch = partition_record.leader_epoch;
                partition.replica_array = partition_record.replica_array;
                partition.in_sync_replica_array =
                    partition_record.in_sync_replica_array;

                topic1.partitions.push_back(partition);
            }
        }
        else {
            topic1.error_code = UNKNOWN_TOPIC_OR_PARTITION;
        }

        topic1.authorizedOperations = { 0, 0, 0x0d, char(0xf8) };
        topic1.tagged_fields = topic.tagged_fields;

        describe_topic_partitions_response.topics.push_back(topic1);
    }

    describe_topic_partitions_response.cursor = request_message.cursor;
    describe_topic_partitions_response.tagged_field =
        request_message.tagged_fields;

    tcp_manager.writeBufferOnClientFd(client_fd,
        describe_topic_partitions_response);
}

void KafkaApis::fetchTopicMessages(const char* buf,
    const size_t buf_size) const {
    FetchRequest request_message = FetchRequest::fromBuffer(buf, buf_size);

    std::cout << "Received Fetch Request: " << request_message.toString()
        << "\n";

    FetchResponse fetch_response;
    fetch_response.corellation_id = request_message.corellation_id;
    fetch_response.error_code = NO_ERROR;

    for (const FetchRequest::Topic& request_topic : request_message.topics) {
        FetchResponse::Topic topic;
        topic.topic_uuid = request_topic.topic_uuid;

        const auto it =
            cluster_metadata.topic_uuid_partition_id_map.find(topic.topic_uuid);
        if (it != cluster_metadata.topic_uuid_partition_id_map.end()) {
            for (const auto& partition_val : request_topic.partitions) {

                auto record_batch = cluster_metadata.readPartitionTopicsFile(
                    partition_val.partition_id, topic.topic_uuid);

                FetchResponse::Partition partition;
                partition.error_code = NO_ERROR;
                partition.partition_id = partition_val.partition_id;
                partition.record_batches.push_back(record_batch);
                topic.partitions.push_back(partition);
            }
        }
        else {
            FetchResponse::Partition partition;
            partition.error_code = UNKNOWN_TOPIC;
            topic.partitions.push_back(partition);
        }

        fetch_response.topics.push_back(topic);
    }

    tcp_manager.writeBufferOnClientFd(client_fd, fetch_response);
}