#include "ClusterMetadata.h"

size_t Key::size() const { return length.size() + key.size(); }

void Key::fromBuffer(const char* buffer, size_t buffer_size) {
    if (buffer_size < sizeof(int8_t)) {
        throw std::runtime_error("Buffer size is too small");
    }

    length.fromBuffer(buffer);
    buffer += length.size();
    buffer_size -= length.size();

    if (length != -1) {
        key = std::string(buffer, length);
    }
}

std::string Key::toString() const {
    return "Key{key=" + (key.empty() ? "<null>" : key) + "}";
}

size_t Value::PartitionRecord::size() const {
    return sizeof(uint32_t) + topic_uuid.size() + replica_array.size() +
        in_sync_replica_array.size() + removing_replica_array.size() +
        adding_replica_array.size() + sizeof(uint32_t) + sizeof(uint32_t) +
        sizeof(uint32_t) + directories_array.size();
}

void Value::PartitionRecord::fromBuffer(const char* buffer,
    size_t buffer_size) {
    if (buffer_size < sizeof(uint32_t)) {
        throw std::runtime_error("Buffer size is too small");
    }

    partition_id = ::fromBuffer<uint32_t>(buffer);
    buffer += sizeof(uint32_t);
    buffer_size -= sizeof(uint32_t);

    std::copy_n(buffer, topic_uuid.size(), topic_uuid.begin());
    buffer += topic_uuid.size();
    buffer_size -= topic_uuid.size();

    replica_array.fromBuffer(buffer, buffer_size);
    buffer += replica_array.size();
    buffer_size -= replica_array.size();

    in_sync_replica_array.fromBuffer(buffer, buffer_size);
    buffer += in_sync_replica_array.size();
    buffer_size -= in_sync_replica_array.size();

    removing_replica_array.fromBuffer(buffer, buffer_size);
    buffer += removing_replica_array.size();
    buffer_size -= removing_replica_array.size();

    adding_replica_array.fromBuffer(buffer, buffer_size);
    buffer += adding_replica_array.size();
    buffer_size -= adding_replica_array.size();

    leader_id = ::fromBuffer<uint32_t>(buffer);
    buffer += sizeof(uint32_t);
    buffer_size -= sizeof(uint32_t);

    leader_epoch = ::fromBuffer<uint32_t>(buffer);
    buffer += sizeof(uint32_t);
    buffer_size -= sizeof(uint32_t);

    partition_epoch = ::fromBuffer<uint32_t>(buffer);
    buffer += sizeof(uint32_t);
    buffer_size -= sizeof(uint32_t);

    directories_array.fromBuffer(buffer, buffer_size);
    buffer += directories_array.size();
    buffer_size -= directories_array.size();

    tagged_fields_count = ::fromBuffer<uint8_t>(buffer);
}

std::string Value::PartitionRecord::toString() const {
    return "PartitionRecord{partition_id=" + std::to_string(partition_id) +
        ", topic_uuid=" + charArrToHex(topic_uuid) +
        ", replica_array=" + replica_array.toString() +
        ", in_sync_replica_array=" + in_sync_replica_array.toString() +
        ", removing_replica_array=" + removing_replica_array.toString() +
        ", adding_replica_array=" + adding_replica_array.toString() +
        ", leader_id=" + std::to_string(leader_id) +
        ", leader_epoch=" + std::to_string(leader_epoch) +
        ", partition_epoch=" + std::to_string(partition_epoch) +
        ", directories_array=" + directories_array.toString() +
        ", tagged_fields_count=" + std::to_string(tagged_fields_count) + "}";
    "}";
}

size_t Value::TopicRecord::size() const {
    return topic_name.size() + topic_uuid.size();
}

void Value::TopicRecord::fromBuffer(const char* buffer, size_t buffer_size) {
    topic_name = NullableString<1>::fromBuffer(buffer, buffer_size);
    buffer += topic_name.size();
    buffer_size -= topic_name.size();

    std::copy_n(buffer, topic_uuid.size(), topic_uuid.begin());
    buffer += topic_uuid.size();
    buffer_size -= topic_uuid.size();

    tagged_fields_count = ::fromBuffer<uint8_t>(buffer);
}

std::string Value::TopicRecord::toString() const {
    return "TopicRecord{topic_name=" + std::string(topic_name.toString()) +
        ", topic_id=" + charArrToHex(topic_uuid) +
        ", tagged_fields_count=" + std::to_string(tagged_fields_count) + "}";
}

size_t Value::size() const { return length.size() + length; }

void Value::fromBuffer(const char* buffer, size_t buffer_size) {
    if (buffer_size < sizeof(int8_t)) {
        throw std::runtime_error("Buffer size is too small");
    }

    length.fromBuffer(buffer);
    buffer += length.size();
    buffer_size -= length.size();

    if (buffer_size < length) {
        throw std::runtime_error("Buffer size is too small for Value");
    }

    frame_version = ::fromBuffer<uint8_t>(buffer);
    buffer += sizeof(uint8_t);
    buffer_size -= sizeof(uint8_t);

    record_type = ::fromBuffer<uint8_t>(buffer);
    buffer += sizeof(uint8_t);
    buffer_size -= sizeof(uint8_t);

    record_version = ::fromBuffer<uint8_t>(buffer);
    buffer += sizeof(uint8_t);
    buffer_size -= sizeof(uint8_t);

    switch (record_type) {
    case Value::PARTITION_RECORD: {
        PartitionRecord partition_record;
        partition_record.fromBuffer(buffer, buffer_size);
        record = partition_record;
        break;
    }
    case Value::TOPIC_RECORD: {
        TopicRecord topic_record;
        topic_record.fromBuffer(buffer, buffer_size);
        record = topic_record;
        break;
    }
    default: {
        std::cout << "Unsupported record type: " << std::to_string(record_type)
            << std::endl;
    }
    }
}

std::string Value::toString() const {
    return "Value{length=" + std::to_string(length) +
        ", frame_version=" + std::to_string(frame_version) +
        ", record_type=" + std::to_string(record_type) +
        ", record_version=" + std::to_string(record_version) + ", record=" +
        std::visit([](const auto& record) { return record.toString(); },
            record) +
        "}";
}

size_t Record::size() const { return length.size() + length; }

void Record::fromBuffer(const char* buffer, size_t buffer_size) {
    if (buffer_size < sizeof(uint8_t)) {
        throw std::runtime_error("Buffer size is too small");
    }

    length.fromBuffer(buffer);
    buffer += length.size();
    buffer_size -= length.size();

    if (buffer_size < length) {
        throw std::runtime_error("Buffer size is too small for Record");
    }

    attributes = ::fromBuffer<uint8_t>(buffer);
    buffer += sizeof(uint8_t);
    buffer_size -= sizeof(uint8_t);

    timestamp_delta = ::fromBuffer<int8_t>(buffer);
    buffer += sizeof(int8_t);
    buffer_size -= sizeof(int8_t);

    offset_delta = ::fromBuffer<int8_t>(buffer);
    buffer += sizeof(int8_t);
    buffer_size -= sizeof(int8_t);

    key.fromBuffer(buffer, buffer_size);
    buffer += key.size();
    buffer_size -= key.size();

    value.fromBuffer(buffer, buffer_size);
    buffer += value.size();
    buffer_size -= value.size();

    headers_count = ::fromBuffer<uint8_t>(buffer);
    buffer += sizeof(uint8_t);
    buffer_size -= sizeof(uint8_t);
}

std::string Record::toString() const {
    return "Record{length=" + std::to_string(length) +
        ", attributes=" + std::to_string(attributes) +
        ", timestamp_delta=" + std::to_string(timestamp_delta) +
        ", offset_delta=" + std::to_string(offset_delta) +
        ", key=" + key.toString() + ", value=" + value.toString() +
        ", headers_count=" + std::to_string(headers_count) + "}";
}

size_t RecordBatch::size() const {
    return sizeof(uint64_t) + sizeof(uint32_t) + batch_length;
}

void RecordBatch::fromBuffer(const char* buffer, size_t buffer_size) {
    if (buffer_size < sizeof(uint64_t) + sizeof(uint32_t)) {
        throw std::runtime_error("Buffer size is too small");
    }

    base_offset = ::fromBuffer<uint64_t>(buffer);
    buffer += sizeof(uint64_t);
    buffer_size -= sizeof(uint64_t);

    batch_length = ::fromBuffer<uint32_t>(buffer);
    buffer += sizeof(uint32_t);
    buffer_size -= sizeof(uint32_t);

    if (buffer_size < batch_length) {
        throw std::runtime_error("Buffer size is too small for current batch");
    }

    partition_leader_epoch = ::fromBuffer<uint32_t>(buffer);
    buffer += sizeof(uint32_t);
    buffer_size -= sizeof(uint32_t);

    magic = ::fromBuffer<uint8_t>(buffer);
    buffer += sizeof(uint8_t);
    buffer_size -= sizeof(uint8_t);

    crc = ::fromBuffer<uint32_t>(buffer);
    buffer += sizeof(uint32_t);
    buffer_size -= sizeof(uint32_t);

    attributes = ::fromBuffer<uint16_t>(buffer);
    buffer += sizeof(uint16_t);
    buffer_size -= sizeof(uint16_t);

    last_offset_delta = ::fromBuffer<uint32_t>(buffer);
    buffer += sizeof(uint32_t);
    buffer_size -= sizeof(uint32_t);

    base_timestamp = ::fromBuffer<uint64_t>(buffer);
    buffer += sizeof(uint64_t);
    buffer_size -= sizeof(uint64_t);

    max_timestamp = ::fromBuffer<uint64_t>(buffer);
    buffer += sizeof(uint64_t);
    buffer_size -= sizeof(uint64_t);

    producer_id = ::fromBuffer<int64_t>(buffer);
    buffer += sizeof(int64_t);
    buffer_size -= sizeof(int64_t);

    producer_epoch = ::fromBuffer<int16_t>(buffer);
    buffer += sizeof(int16_t);
    buffer_size -= sizeof(int16_t);

    base_sequence = ::fromBuffer<int32_t>(buffer);
    buffer += sizeof(int32_t);
    buffer_size -= sizeof(int32_t);

    uint32_t records_count = ::fromBuffer<uint32_t>(buffer);
    buffer += sizeof(uint32_t);
    buffer_size -= sizeof(uint32_t);

    for (uint32_t i = 0; i < records_count; ++i) {
        Record record;
        record.fromBuffer(buffer, buffer_size);
        records.push_back(record);
        buffer += record.size();
        buffer_size -= record.size();
    }
}

std::string RecordBatch::toString() const {
    std::string res =
        "RecordBatch{base_offset=" + std::to_string(base_offset) +
        ", batch_length=" + std::to_string(batch_length) +
        ", partition_leader_epoch=" + std::to_string(partition_leader_epoch) +
        ", magic=" + std::to_string(magic) + ", crc=" + std::to_string(crc) +
        ", attributes=" + std::to_string(attributes) +
        ", last_offset_delta=" + std::to_string(last_offset_delta) +
        ", base_timestamp=" + std::to_string(base_timestamp) +
        ", max_timestamp=" + std::to_string(max_timestamp) +
        ", producer_id=" + std::to_string(producer_id) +
        ", producer_epoch=" + std::to_string(producer_epoch) +
        ", base_sequence=" + std::to_string(base_sequence) +
        ", records_count=" + std::to_string(records.size()) + ", records={";
    for (const auto& record : records) {
        res += record.toString() + ",";
    }
    res += "}}";
    return res;
}

void ClusterMetadata::waitForFileToExist() const {
    int cnt = 100;
    while (cnt--) {
        if (std::filesystem::exists(medata_file)) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    assert((std::filesystem::exists(medata_file) == true,
        "Tried to sleep for 100 seconds but file still doesn't exist"));

    // Sleep some more to make sure the file is completely written
    std::this_thread::sleep_for(std::chrono::seconds(1));
}

void ClusterMetadata::readClusterMetadata() {
    std::cout << "Reading Cluster Metadata\n";

    waitForFileToExist();

    std::ifstream cluster_metadata_file(medata_file, std::ios::binary);
    if (!cluster_metadata_file.is_open()) {
        throw std::runtime_error("Failed to open cluster metadata file");
    }

    // Get file size
    cluster_metadata_file.seekg(0, std::ios::end);
    std::streampos fileSize = cluster_metadata_file.tellg();
    cluster_metadata_file.seekg(0, std::ios::beg);

    std::string buffer(fileSize, 0);
    cluster_metadata_file.read(buffer.data(), fileSize);
    cluster_metadata_file.close();

    while (fileSize > 0) {
        RecordBatch record_batch;
        record_batch.fromBuffer(buffer.data(), fileSize);
        fileSize -= record_batch.size();
        buffer = buffer.substr(record_batch.size());

        for (Record record : record_batch.records) {
            std::visit(
                [this](const auto& record) {
                    using RecordT = std::decay_t<decltype(record)>;
                    if constexpr (std::is_same_v<RecordT,
                        Value::PartitionRecord>) {
                        std::cout << "Partition Record: " << record.toString()
                            << "\n";
                        topic_uuid_partition_id_map[record.topic_uuid]
                            .push_back(record);
                    }
                    else if constexpr (std::is_same_v<RecordT,
                        Value::TopicRecord>) {
                        std::cout << "Topic Record: " << record.toString()
                            << "\n";
                        std::string topic_name_str =
                            std::string(record.topic_name.toString());
                        topic_name_uuid_map[topic_name_str] = record.topic_uuid;
                        topic_uuid_name_map[record.topic_uuid] = topic_name_str;
                    }
                },
                record.value.record);
        }
    }

    assert((fileSize == 0, "metadata file not read completely"));
}

std::string ClusterMetadata::readPartitionTopicsFile(
    uint32_t partition_id, const std::array<char, 16> topic_id) const {
    std::string topic = topic_uuid_name_map.at(topic_id);
    const std::string file_name = "/tmp/kraft-combined-logs/" + topic + "-" +
        std::to_string(partition_id) +
        "/00000000000000000000.log";

    std::ifstream partition_topics_file(file_name, std::ios::binary);
    if (!partition_topics_file.is_open()) {
        throw std::runtime_error("Failed to open partition topics file");
    }

    std::cout << "Reading Partition Topics File: " << file_name << "\n";

    std::string res =
        std::string(std::istreambuf_iterator<char>(partition_topics_file),
            std::istreambuf_iterator<char>());

    hexdump(res.data(), res.size());

    return res;
}
