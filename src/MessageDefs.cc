#include "MessageDefs.h"

#pragma GCC diagnostic ignored "-Winvalid-offsetof"

void RequestHeader::fromBufferLocal(const char* buffer, size_t buffer_size) {
    if (buffer_size < RequestHeader::MIN_HEADER_SIZE) {
        throw std::runtime_error("Buffer size is too small");
    }

#define READ(field)                                                            \
    field = ::fromBuffer<decltype(RequestHeader::field)>(                      \
        buffer + offsetof(RequestHeader, field));

    READ(message_size);
    READ(request_api_key);
    READ(request_api_version);
    READ(corellation_id);

    client_id = NullableString<2>::fromBuffer(
        buffer + offsetof(RequestHeader, client_id),
        buffer_size - offsetof(RequestHeader, client_id));

    tagged_fields.fromBuffer(buffer + offsetof(RequestHeader, client_id) +
        client_id.value.size(),
        buffer_size - offsetof(RequestHeader, client_id) -
        client_id.value.size());

#undef READ
}

RequestHeader RequestHeader::fromBuffer(const char* buffer,
    size_t buffer_size) {
    RequestHeader request_header;
    request_header.fromBufferLocal(buffer, buffer_size);
    return request_header;
}

size_t RequestHeader::requestHeaderSize() const {
    return offsetof(RequestHeader, client_id) + client_id.size() +
        sizeof(TaggedFields);
}

ApiVersionsRequestMessage
ApiVersionsRequestMessage::fromBuffer(const char* buffer, size_t buffer_size) {
    if (buffer_size < ApiVersionsRequestMessage::MIN_HEADER_SIZE) {
        throw std::runtime_error("Buffer size is too small");
    }

    ApiVersionsRequestMessage api_versions_request_message;
    api_versions_request_message.fromBufferLocal(buffer, buffer_size);
    return api_versions_request_message;
}

void DescribeTopicPartitionsRequest::Topic::fromBufferLocal(
    const char* buffer, size_t buffer_size) {
    if (buffer_size < NullableString<1>::LEN_SIZE + sizeof(TaggedFields)) {
        throw std::runtime_error("Buffer size is too small");
    }

    topic_name = NullableString<1>::fromBuffer(buffer, buffer_size);
    tagged_fields.fromBuffer(buffer + topic_name.size(),
        buffer_size - topic_name.size());
}

DescribeTopicPartitionsRequest::Topic
DescribeTopicPartitionsRequest::Topic::fromBuffer(const char* buffer,
    size_t buffer_size) {
    Topic topic;
    topic.fromBufferLocal(buffer, buffer_size);
    return topic;
}

size_t DescribeTopicPartitionsRequest::Topic::size() const {
    return topic_name.size() + sizeof(TaggedFields);
}

DescribeTopicPartitionsRequest
DescribeTopicPartitionsRequest::fromBuffer(const char* buffer,
    size_t buffer_size) {
    if (buffer_size < DescribeTopicPartitionsRequest::MIN_HEADER_SIZE) {
        throw std::runtime_error("Buffer size is too small");
    }

    DescribeTopicPartitionsRequest describe_topic_partitions_request;
    describe_topic_partitions_request.fromBufferLocal(buffer, buffer_size);
    buffer = buffer + describe_topic_partitions_request.requestHeaderSize();
    buffer_size =
        buffer_size - describe_topic_partitions_request.requestHeaderSize();

    uint8_t array_length = ::fromBuffer<uint8_t>(buffer);
    buffer = buffer + sizeof(uint8_t);
    buffer_size = buffer_size - sizeof(uint8_t);

    for (size_t i = 0; i < array_length - 1; ++i) {
        DescribeTopicPartitionsRequest::Topic topic =
            DescribeTopicPartitionsRequest::Topic::fromBuffer(buffer,
                buffer_size);
        describe_topic_partitions_request.topics.push_back(topic);
        buffer = buffer + topic.size();
        buffer_size = buffer_size - topic.size();
    }

    describe_topic_partitions_request.responsePartitionLimit =
        ::fromBuffer<uint32_t>(buffer);
    buffer = buffer + sizeof(uint32_t);
    buffer_size = buffer_size - sizeof(uint32_t);

    describe_topic_partitions_request.cursor = ::fromBuffer<uint8_t>(buffer);
    buffer = buffer + sizeof(uint8_t);
    buffer_size = buffer_size - sizeof(uint8_t);

    describe_topic_partitions_request.tagged_fields.fromBuffer(buffer,
        buffer_size);
    return describe_topic_partitions_request;
}

void FetchRequest::Partition::fromBufferLocal(const char* buffer,
    size_t buffer_size) {
    if (buffer_size < sizeof(int32_t)) {
        throw std::runtime_error("Buffer size is too small");
    }

    partition_id = ::fromBuffer<int32_t>(buffer);
    buffer += sizeof(int32_t);
    buffer_size -= sizeof(int32_t);

    current_leader_epoch = ::fromBuffer<int32_t>(buffer);
    buffer += sizeof(int32_t);
    buffer_size -= sizeof(int32_t);

    fetch_offset = ::fromBuffer<int64_t>(buffer);
    buffer += sizeof(int64_t);
    buffer_size -= sizeof(int64_t);

    last_fetched_epoch = ::fromBuffer<int32_t>(buffer);
    buffer += sizeof(int32_t);
    buffer_size -= sizeof(int32_t);

    log_start_offset = ::fromBuffer<int64_t>(buffer);
    buffer += sizeof(int64_t);
    buffer_size -= sizeof(int64_t);

    partition_max_bytes = ::fromBuffer<int32_t>(buffer);
    buffer += sizeof(int32_t);
    buffer_size -= sizeof(int32_t);

    tagged_fields.fromBuffer(buffer, buffer_size);
}

size_t FetchRequest::Partition::size() const {
    return offsetof(FetchRequest::Partition, tagged_fields) +
        sizeof(TaggedFields);
}

void FetchRequest::Topic::fromBufferLocal(const char* buffer,
    size_t buffer_size) {
    if (buffer_size < sizeof(std::array<char, 16>)) {
        throw std::runtime_error("Buffer size is too small");
    }

    std::copy_n(buffer, topic_uuid.size(), topic_uuid.begin());
    buffer += topic_uuid.size();
    buffer_size -= topic_uuid.size();

    uint8_t array_length = ::fromBuffer<uint8_t>(buffer);
    buffer += sizeof(uint8_t);
    buffer_size -= sizeof(uint8_t);

    for (size_t i = 0; i < array_length - 1; ++i) {
        Partition partition;
        partition.fromBufferLocal(buffer, buffer_size);
        partitions.push_back(partition);
        buffer += partition.size();
        buffer_size -= partition.size();
    }

    tagged_fields.fromBuffer(buffer, buffer_size);
}

size_t FetchRequest::Topic::size() const {
    size_t size = topic_uuid.size() + sizeof(uint8_t);
    for (const auto& partition : partitions) {
        size += partition.size();
    }
    return size + sizeof(TaggedFields);
}

void FetchRequest::ForgottenTopic::fromBufferLocal(const char* buffer,
    size_t buffer_size) {
    if (buffer_size < sizeof(std::array<char, 16>)) {
        throw std::runtime_error("Buffer size is too small");
    }

    std::copy_n(buffer, topic_uuid.size(), topic_uuid.begin());
    buffer += topic_uuid.size();
    buffer_size -= topic_uuid.size();

    uint8_t array_length = ::fromBuffer<uint8_t>(buffer);
    buffer += sizeof(uint8_t);
    buffer_size -= sizeof(uint8_t);

    for (size_t i = 0; i < array_length - 1; ++i) {
        partitions.push_back(::fromBuffer<int32_t>(buffer));
        buffer += sizeof(int32_t);
        buffer_size -= sizeof(int32_t);
    }
}

size_t FetchRequest::ForgottenTopic::size() const {
    size_t size = topic_uuid.size() + sizeof(uint8_t);
    size += sizeof(int32_t) * partitions.size();
    return size;
}

FetchRequest FetchRequest::fromBuffer(const char* buffer, size_t buffer_size) {
    FetchRequest fetch_request;
    fetch_request.fromBufferLocal(buffer, buffer_size);
    buffer = buffer + fetch_request.requestHeaderSize();
    buffer_size = buffer_size - fetch_request.requestHeaderSize();

    fetch_request.max_wait_ms = ::fromBuffer<int32_t>(buffer);
    buffer += sizeof(int32_t);
    buffer_size -= sizeof(int32_t);

    fetch_request.min_bytes = ::fromBuffer<int32_t>(buffer);
    buffer += sizeof(int32_t);
    buffer_size -= sizeof(int32_t);

    fetch_request.max_bytes = ::fromBuffer<int32_t>(buffer);
    buffer += sizeof(int32_t);
    buffer_size -= sizeof(int32_t);

    fetch_request.isolation_level = ::fromBuffer<int8_t>(buffer);
    buffer += sizeof(int8_t);
    buffer_size -= sizeof(int8_t);

    fetch_request.session_id = ::fromBuffer<int32_t>(buffer);
    buffer += sizeof(int32_t);
    buffer_size -= sizeof(int32_t);

    fetch_request.session_epoch = ::fromBuffer<int32_t>(buffer);
    buffer += sizeof(int32_t);
    buffer_size -= sizeof(int32_t);

    uint8_t array_length = ::fromBuffer<uint8_t>(buffer);
    buffer += sizeof(uint8_t);
    buffer_size -= sizeof(uint8_t);

    for (size_t i = 0; i < array_length - 1; ++i) {
        Topic topic;
        topic.fromBufferLocal(buffer, buffer_size);
        fetch_request.topics.push_back(topic);
        buffer += topic.size();
        buffer_size -= topic.size();
    }

    array_length = ::fromBuffer<uint8_t>(buffer);
    buffer += sizeof(uint8_t);
    buffer_size -= sizeof(uint8_t);

    for (size_t i = 0; i < array_length - 1; ++i) {
        ForgottenTopic forgotten_topic;
        forgotten_topic.fromBufferLocal(buffer, buffer_size);
        fetch_request.forgotten_topics.push_back(forgotten_topic);
        buffer += forgotten_topic.size();
        buffer_size -= forgotten_topic.size();
    }

    fetch_request.rack_id = NullableString<1>::fromBuffer(buffer, buffer_size);
    buffer += fetch_request.rack_id.size();
    buffer_size -= fetch_request.rack_id.size();

    fetch_request.tagged_fields.fromBuffer(buffer, buffer_size);

    return fetch_request;
}

std::string RequestHeader::toString() const {
    return "RequestHeader{message_size=" + std::to_string(message_size) +
        ", request_api_key=" + std::to_string(request_api_key) +
        ", request_api_version=" + std::to_string(request_api_version) +
        ", corellation_id=" + std::to_string(corellation_id) +
        ", client_id=" + std::string(client_id.toString()) + "}";
}

std::string ApiVersionsRequestMessage::toString() const {
    return "ApiVersionsRequestMessage{" + RequestHeader::toString() + "}";
}

std::string DescribeTopicPartitionsRequest::Topic::toString() const {
    return "Topic{topic_name=" + std::string(topic_name.toString()) +
        ", tagged_fields=" + tagged_fields.toString() + "}";
}

std::string DescribeTopicPartitionsRequest::toString() const {
    std::string topics_str = "Topics{";
    for (const auto& topic : topics) {
        topics_str += topic.toString() + ", ";
    }
    topics_str += "}";

    return "DescribeTopicPartitionsRequest{" + RequestHeader::toString() +
        ", array_length=" + std::to_string(topics.size() + 1) +
        ", topics=" + topics_str + ", responsePartitionLimit=" +
        std::to_string(responsePartitionLimit) +
        ", cursor=" + std::to_string(cursor) +
        ", tagged_fields=" + tagged_fields.toString() + "}";
}

std::string FetchRequest::Partition::toString() const {
    return "Partition{partition_id=" + std::to_string(partition_id) +
        ", current_leader_epoch=" + std::to_string(current_leader_epoch) +
        ", fetch_offset=" + std::to_string(fetch_offset) +
        ", last_fetched_epoch=" + std::to_string(last_fetched_epoch) +
        ", log_start_offset=" + std::to_string(log_start_offset) +
        ", partition_max_bytes=" + std::to_string(partition_max_bytes) +
        ", tagged_fields=" + tagged_fields.toString() + "}";
}

std::string FetchRequest::Topic::toString() const {
    std::string partitions_str = "Partitions{";
    for (const auto& partition : partitions) {
        partitions_str += partition.toString() + ", ";
    }
    partitions_str += "}";

    return "Topic{topic_uuid=" + charArrToHex(topic_uuid) +
        ", partitions=" + partitions_str +
        ", tagged_fields=" + tagged_fields.toString() + "}";
}

std::string FetchRequest::ForgottenTopic::toString() const {
    std::string partitions_str = "Partitions{";
    for (const auto& partition : partitions) {
        partitions_str += std::to_string(partition) + ", ";
    }
    partitions_str += "}";

    return "ForgottenTopic{topic_uuid=" + charArrToHex(topic_uuid) +
        ", partitions=" + partitions_str + "}";
}

std::string FetchRequest::toString() const {
    std::string topics_str = "Topics{";
    for (const auto& topic : topics) {
        topics_str += topic.toString() + ", ";
    }
    topics_str += "}";

    std::string forgotten_topics_str = "ForgottenTopics{";
    for (const auto& forgotten_topic : forgotten_topics) {
        forgotten_topics_str += forgotten_topic.toString() + ", ";
    }
    forgotten_topics_str += "}";

    return "FetchRequest{max_wait_ms=" + std::to_string(max_wait_ms) +
        ", min_bytes=" + std::to_string(min_bytes) +
        ", max_bytes=" + std::to_string(max_bytes) +
        ", isolation_level=" + std::to_string(isolation_level) +
        ", session_id=" + std::to_string(session_id) +
        ", session_epoch=" + std::to_string(session_epoch) +
        ", topics=" + topics_str +
        ", forgotten_topics=" + forgotten_topics_str +
        ", rack_id=" + std::string(rack_id.toString()) +
        ", tagged_fields=" + tagged_fields.toString() + "}";
}

std::string ApiVersionsResponseMessage::ApiKey::toBuffer() const {
    std::string buffer;

    buffer.append(::toBuffer(api_key));
    buffer.append(::toBuffer(min_version));
    buffer.append(::toBuffer(max_version));
    buffer.append(tagged_fields.toBuffer());

    return buffer;
}

std::string ApiVersionsResponseMessage::ApiKey::toString() const {
    return "ApiKey{api_key=" + std::to_string(api_key) +
        ", min_version=" + std::to_string(min_version) +
        ", max_version=" + std::to_string(max_version) +
        ", tagged_fields=" + tagged_fields.toString() + "}";
}

std::string ApiVersionsResponseMessage::toBuffer() const {
    std::string buffer = "XXXX"; // These are 4 bytes for message_size

    buffer.append(::toBuffer(corellation_id));
    buffer.append(::toBuffer(error_code));
    buffer.append(::toBuffer<uint8_t>(api_keys.size() + 1));

    for (const auto& api_key : api_keys) {
        buffer.append(api_key.toBuffer());
    }

    buffer.append(::toBuffer(throttle_time));
    buffer.append(tagged_fields.toBuffer());

    // Update the message size
    *reinterpret_cast<uint32_t*>(buffer.data()) = htonl(buffer.size() - 4);

    return buffer;
}

std::string DescribeTopicPartitionsResponse::Partition::toBuffer() const {
    std::string buffer;

    buffer.append(::toBuffer(error_code));
    buffer.append(::toBuffer(partition_id));
    buffer.append(::toBuffer(leader_id));
    buffer.append(::toBuffer(leader_epoch));

    buffer.append(replica_array.toBuffer());
    buffer.append(in_sync_replica_array.toBuffer());
    buffer.append(eligible_leader_replica_array.toBuffer());
    buffer.append(last_known_elr_array.toBuffer());
    buffer.append(offline_replica_array.toBuffer());

    buffer.append(tagged_fields.toBuffer());
    return buffer;
}

std::string DescribeTopicPartitionsResponse::Topic::toBuffer() const {
    std::string buffer;
    buffer.append(::toBuffer(error_code));

    buffer.append(topic_name.toBuffer());
    buffer.append(topic_uuid.data(), topic_uuid.size());

    buffer.append(::toBuffer<uint8_t>((boolInternal) ? 1 : 0));
    buffer.append(::toBuffer<uint8_t>(partitions.size() + 1));

    for (const auto& partition : partitions) {
        buffer.append(partition.toBuffer());
    }

    buffer.append(authorizedOperations.data(), authorizedOperations.size());
    buffer.append(tagged_fields.toBuffer());

    return buffer;
}

std::string DescribeTopicPartitionsResponse::toBuffer() const {
    std::string buffer = "XXXX"; // These are 4 bytes for message_size

    buffer.append(::toBuffer(corellation_id));
    buffer.append(tagged_fields.toBuffer());
    buffer.append(::toBuffer(throttle_time));
    buffer.append(::toBuffer<uint8_t>(topics.size() + 1));

    for (const auto& topic : topics) {
        buffer.append(topic.toBuffer());
    }

    buffer.append(::toBuffer(cursor));
    buffer.append(tagged_field.toBuffer());

    // Update the message size
    *reinterpret_cast<uint32_t*>(buffer.data()) = htonl(buffer.size() - 4);

    return buffer;
}

std::string FetchResponse::AbortedTransactions::toBuffer() const {
    std::string buffer;

    buffer.append(::toBuffer<int64_t>(producer_id));
    buffer.append(::toBuffer<int64_t>(first_offset));
    buffer.append(tagged_fields.toBuffer());

    return buffer;
}

std::string FetchResponse::Partition::toBuffer() const {
    std::string buffer;

    buffer.append(::toBuffer<uint32_t>(partition_id));
    buffer.append(::toBuffer<uint16_t>(error_code));
    buffer.append(::toBuffer<int64_t>(high_watermark));
    buffer.append(::toBuffer<int64_t>(last_stable_offset));
    buffer.append(::toBuffer<int64_t>(log_start_offset));

    buffer.append(::toBuffer<uint8_t>(aborted_transactions.size() + 1));
    for (const auto& aborted_transaction : aborted_transactions) {
        buffer.append(aborted_transaction.toBuffer());
    }

    buffer.append(::toBuffer<int32_t>(preferred_read_replica));

    buffer.append(::toBuffer<uint8_t>(record_batches.size() + 1));
    for (const auto& record_batch : record_batches) {
        buffer.append(record_batch);
    }

    buffer.append(tagged_fields.toBuffer());

    return buffer;
}

std::string FetchResponse::Topic::toBuffer() const {
    std::string buffer;

    buffer.append(topic_uuid.data(), topic_uuid.size());

    buffer.append(::toBuffer<uint8_t>(partitions.size() + 1));
    for (const auto& partition : partitions) {
        buffer.append(partition.toBuffer());
    }

    buffer.append(tagged_fields.toBuffer());

    return buffer;
}

std::string FetchResponse::toBuffer() const {
    std::string buffer = "XXXX"; // These are 4 bytes for message_size

    buffer.append(::toBuffer(corellation_id));
    buffer.append(tagged_fieldsH.toBuffer());
    buffer.append(::toBuffer<int32_t>(throttle_time));
    buffer.append(::toBuffer<int16_t>(error_code));
    buffer.append(::toBuffer<int32_t>(session_id));

    buffer.append(::toBuffer<uint8_t>(topics.size() + 1));
    for (const auto& topic : topics) {
        buffer.append(topic.toBuffer());
    }

    buffer.append(tagged_fieldsT.toBuffer());

    // Update the message size
    *reinterpret_cast<uint32_t*>(buffer.data()) = htonl(buffer.size() - 4);

    return buffer;
}

std::string ApiVersionsResponseMessage::toString() const {
    std::string api_keys_str = "ApiKeys{";
    for (const auto& api_key : api_keys) {
        api_keys_str += api_key.toString() + ", ";
    }
    api_keys_str += "}";

    return "ApiVersionsResponseMessage{message_size=" +
        std::to_string(message_size) +
        ", corellation_id=" + std::to_string(corellation_id) +
        ", error_code=" + std::to_string(error_code) +
        ", api_keys_count=" + std::to_string(api_keys.size() + 1) +
        ", api_keys=" + api_keys_str +
        ", throttle_time=" + std::to_string(throttle_time) +
        ", tagged_fields=" + tagged_fields.toString() + "}";
}

std::string DescribeTopicPartitionsResponse::Partition::toString() const {
    return "Partition{error_code=" + std::to_string(error_code) +
        ", partition_id=" + std::to_string(partition_id) +
        ", leader_id=" + std::to_string(leader_id) +
        ", leader_epoch=" + std::to_string(leader_epoch) +
        ", replica_array=" + replica_array.toString() +
        ", in_sync_replica_array=" + in_sync_replica_array.toString() +
        ", eligible_leader_replica_array=" +
        eligible_leader_replica_array.toString() +
        ", last_known_elr_array=" + last_known_elr_array.toString() +
        ", offline_replica_array=" + offline_replica_array.toString() +
        ", tagged_fields=" + tagged_fields.toString() + "}";
}

std::string DescribeTopicPartitionsResponse::Topic::toString() const {
    std::string partitions_str = "Partitions{";
    for (const auto& partition : partitions) {
        partitions_str += partition.toString() + ", ";
    }
    partitions_str += "}";

    return "Topic{error_code=" + std::to_string(error_code) +
        ", topic_name=" + std::string(topic_name.toString()) +
        ", topic_id=" + charArrToHex(topic_uuid) +
        ", boolInternal=" + std::to_string(boolInternal) +
        ", array_length=" + std::to_string(partitions.size() + 1) +
        ", partitions=" + partitions_str +
        ", authorizedOperations=" + charArrToHex(authorizedOperations) +
        ", tagged_fields=" + tagged_fields.toString() + "}";
}

std::string DescribeTopicPartitionsResponse::toString() const {
    std::string topics_str = "Topics{";
    for (const auto& topic : topics) {
        topics_str += topic.toString() + ", ";
    }
    topics_str += "}";

    return "DescribeTopicPartitionsResponse{message_size=" +
        std::to_string(message_size) +
        ", corellation_id=" + std::to_string(corellation_id) +
        ", tagged_fields=" + tagged_fields.toString() +
        ", throttle_time=" + std::to_string(throttle_time) +
        ", array_length=" + std::to_string(topics.size() + 1) +
        ", topics=" + topics_str + ", cursor=" + std::to_string(cursor) +
        ", tagged_fields=" + tagged_field.toString() + "}";
}

std::string FetchResponse::AbortedTransactions::toString() const {
    return "AbortedTransactions{producer_id=" + std::to_string(producer_id) +
        ", first_offset=" + std::to_string(first_offset) +
        ", tagged_fields=" + tagged_fields.toString() + "}";
}

std::string FetchResponse::Partition::toString() const {
    std::string aborted_transactions_str = "AbortedTransactions{";
    for (const auto& aborted_transaction : aborted_transactions) {
        aborted_transactions_str += aborted_transaction.toString() + ", ";
    }
    aborted_transactions_str += "}";

    std::string record_batches_str = "RecordBatches{";
    for (const auto& record_batch_str : record_batches) {
        record_batches_str +=
            "< " + std::to_string(record_batches_str.size()) + "  >, ";
    }
    record_batches_str += "}";

    return "Partition{partition_id=" + std::to_string(partition_id) +
        ", error_code=" + std::to_string(error_code) +
        ", high_watermark=" + std::to_string(high_watermark) +
        ", last_stable_offset=" + std::to_string(last_stable_offset) +
        ", log_start_offset=" + std::to_string(log_start_offset) +
        ", array_length=" + std::to_string(aborted_transactions.size() + 1) +
        ", aborted_transactions=" + aborted_transactions_str +
        ", preferred_read_replica=" +
        std::to_string(preferred_read_replica) +
        ", records=" + record_batches_str +
        ", tagged_fields=" + tagged_fields.toString() + "}";
}

std::string FetchResponse::Topic::toString() const {
    std::string partitions_str = "Partitions{";
    for (const auto& partition : partitions) {
        partitions_str += partition.toString() + ", ";
    }
    partitions_str += "}";

    return "Topic{topic_uuid=" + charArrToHex(topic_uuid) +
        ", array_length=" + std::to_string(partitions.size() + 1) +
        ", partitions=" + partitions_str +
        ", tagged_fields=" + tagged_fields.toString() + "}";
}

std::string FetchResponse::toString() const {
    std::string topics_str = "Topics{";
    for (const auto& topic : topics) {
        topics_str += topic.toString() + ", ";
    }
    topics_str += "}";

    return "FetchResponse{message_size=" + std::to_string(message_size) +
        ", corellation_id=" + std::to_string(corellation_id) +
        ", tagged_fields (head)=" + tagged_fieldsH.toString() +
        ", throttle_time=" + std::to_string(throttle_time) +
        ", error_code=" + std::to_string(error_code) +
        ", session_id=" + std::to_string(session_id) +
        ", array_length=" + std::to_string(topics.size() + 1) +
        ", topics=" + topics_str +
        ", tagged_fields (tail)=" + tagged_fieldsT.toString() + "}";
}

#pragma diagnostic(pop)