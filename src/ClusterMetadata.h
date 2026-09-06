#pragma once

#include "Utils.h"

#pragma pack(push, 1)

struct Key {
    VariableInt length;
    std::string key;

    size_t size() const;
    void fromBuffer(const char* buffer, size_t buffer_size);
    std::string toString() const;
};

struct Value {
    static constexpr uint8_t PARTITION_RECORD = 3;
    static constexpr uint8_t TOPIC_RECORD = 2;

    struct PartitionRecord {
        uint32_t partition_id{};
        std::array<char, 16> topic_uuid{};
        CompactArray<uint32_t> replica_array;
        CompactArray<uint32_t> in_sync_replica_array;
        CompactArray<uint32_t> removing_replica_array;
        CompactArray<uint32_t> adding_replica_array;
        uint32_t leader_id{};
        uint32_t leader_epoch{};
        uint32_t partition_epoch{};
        CompactArray<std::array<char, 16>> directories_array;
        uint8_t tagged_fields_count{};

        PartitionRecord() noexcept = default;
        size_t size() const;
        void fromBuffer(const char* buffer, size_t buffer_size);
        std::string toString() const;
    };

    struct TopicRecord {
        NullableString<1> topic_name;
        std::array<char, 16> topic_uuid;
        uint8_t tagged_fields_count{};

        TopicRecord() noexcept = default;
        size_t size() const;
        void fromBuffer(const char* buffer, size_t buffer_size);
        std::string toString() const;
    };

    struct NoRecord {
        NoRecord() noexcept = default;
        std::string toString() const { return "NoRecord{}"; }
    };

    using RecordT = std::variant<NoRecord, PartitionRecord, TopicRecord>;

    VariableInt length{};
    uint8_t frame_version{};
    uint8_t record_type{};
    uint8_t record_version{};
    RecordT record;

    size_t size() const;
    void fromBuffer(const char* buffer, size_t buffer_size);
    std::string toString() const;
};

struct Record {
    VariableInt length{};
    uint8_t attributes{};
    int8_t timestamp_delta{};
    int8_t offset_delta{};
    Key key;
    Value value;
    uint8_t headers_count{};

    size_t size() const;
    void fromBuffer(const char* buffer, size_t buffer_size);
    std::string toString() const;
};

struct RecordBatch {
    uint64_t base_offset{};
    uint32_t batch_length{};
    uint32_t partition_leader_epoch{};
    uint8_t magic{};
    uint32_t crc{};
    uint16_t attributes{};
    uint32_t last_offset_delta{};
    uint64_t base_timestamp{};
    uint64_t max_timestamp{};
    int64_t producer_id{};
    int16_t producer_epoch{};
    int32_t base_sequence{};
    std::vector<Record> records;

    size_t size() const;
    void fromBuffer(const char* buffer, size_t buffer_size);
    std::string toString() const;
};

#pragma pack(pop)

struct ClusterMetadata {
    ClusterMetadata() { readClusterMetadata(); }

    std::string
        readPartitionTopicsFile(uint32_t partition_id,
            const std::array<char, 16> topic_id) const;

    static inline const std::string medata_file =
        "/tmp/kraft-combined-logs/__cluster_metadata-0/"
        "00000000000000000000.log";

    std::unordered_map<std::string, std::array<char, 16>> topic_name_uuid_map;
    std::map<std::array<char, 16>, std::string> topic_uuid_name_map;
    std::map<std::array<char, 16>, std::vector<Value::PartitionRecord>>
        topic_uuid_partition_id_map;

private:
    void readClusterMetadata();
    void waitForFileToExist() const;
};