#pragma once

#include "ClusterMetadata.h"
#include "Utils.h"
#include <arpa/inet.h>
#include <netdb.h>
#include <sys/socket.h>
#include <sys/types.h>

#pragma pack(push, 1)

struct Header {
    uint32_t message_size{};
};

struct RequestHeader : Header {
    constexpr static uint32_t MIN_HEADER_SIZE = 14;

    int16_t request_api_key{};
    int16_t request_api_version{};
    int32_t corellation_id{};
    NullableString<2> client_id{};
    TaggedFields tagged_fields{};

    static RequestHeader fromBuffer(const char* buffer, size_t buffer_size);
    void fromBufferLocal(const char* buffer, size_t buffer_size);
    std::string toString() const;
    size_t requestHeaderSize() const;
};

struct ApiVersionsRequestMessage : RequestHeader {
    static ApiVersionsRequestMessage fromBuffer(const char* buffer,
        size_t buffer_size);
    std::string toString() const;
};

struct DescribeTopicPartitionsRequest : RequestHeader {
    struct Topic {
        NullableString<1> topic_name{};
        TaggedFields tagged_fields{};

        static Topic fromBuffer(const char* buffer, size_t buffer_size);
        void fromBufferLocal(const char* buffer, size_t buffer_size);
        std::string toString() const;
        size_t size() const;
    };

    std::vector<Topic> topics{};
    uint32_t responsePartitionLimit = 0;
    uint8_t cursor = 0;
    TaggedFields tagged_field{};

    static DescribeTopicPartitionsRequest fromBuffer(const char* buffer,
        size_t buffer_size);
    std::string toString() const;
};

struct FetchRequest : RequestHeader {
    int32_t max_wait_ms;
    int32_t min_bytes;
    int32_t max_bytes;
    int8_t isolation_level;
    int32_t session_id;
    int32_t session_epoch;

    struct Partition {
        int32_t partition_id;
        int32_t current_leader_epoch;
        int64_t fetch_offset;
        int32_t last_fetched_epoch;
        int64_t log_start_offset;
        int32_t partition_max_bytes;
        TaggedFields tagged_fields{};

        void fromBufferLocal(const char* buffer, size_t buffer_size);
        std::string toString() const;
        size_t size() const;
    };

    struct Topic {
        std::array<char, 16> topic_uuid;
        std::vector<Partition> partitions{};
        TaggedFields tagged_fields{};

        void fromBufferLocal(const char* buffer, size_t buffer_size);
        std::string toString() const;
        size_t size() const;
    };

    struct ForgottenTopic {
        std::array<char, 16> topic_uuid;
        std::vector<int32_t> partitions;

        void fromBufferLocal(const char* buffer, size_t buffer_size);
        std::string toString() const;
        size_t size() const;
    };

    std::vector<Topic> topics{};
    std::vector<ForgottenTopic> forgotten_topics;
    NullableString<1> rack_id;
    TaggedFields tagged_fields{};

    static FetchRequest fromBuffer(const char* buffer, size_t buffer_size);
    std::string toString() const;
};

constexpr size_t MAX_BUFFER_SIZE = 1024;

struct ResponseHeader : Header {
    int32_t corellation_id{};
};

struct ApiVersionsResponseMessage : ResponseHeader {
    int16_t error_code{};

    struct ApiKey {
        int16_t api_key{};
        int16_t min_version{};
        int16_t max_version{};
        TaggedFields tagged_fields{};

        std::string toBuffer() const;
        std::string toString() const;
    };

    std::vector<ApiKey> api_keys{};

    int32_t throttle_time = 0;
    TaggedFields tagged_fields{};

    std::string toBuffer() const;
    std::string toString() const;
};

struct DescribeTopicPartitionsResponse : ResponseHeader {
    TaggedFields tagged_fields{};
    int32_t throttle_time = 0;

    struct Partition {
        uint16_t error_code{};
        uint32_t partition_id{};
        uint32_t leader_id{};
        uint32_t leader_epoch{};
        CompactArray<uint32_t> replica_array;
        CompactArray<uint32_t> in_sync_replica_array;
        CompactArray<uint32_t> eligible_leader_replica_array;
        CompactArray<uint32_t> last_known_elr_array;
        CompactArray<uint32_t> offline_replica_array;
        TaggedFields tagged_fields{};

        std::string toBuffer() const;
        std::string toString() const;
    };

    struct Topic {
        int16_t error_code{};
        NullableString<1> topic_name{};
        std::array<char, 16> topic_uuid{};
        bool boolInternal = false;
        std::vector<Partition> partitions{};
        std::array<char, 4> authorizedOperations{};
        TaggedFields tagged_fields{};

        std::string toBuffer() const;
        std::string toString() const;
    };

    std::vector<Topic> topics{};
    uint8_t cursor = 0;
    TaggedFields tagged_field{};

    std::string toBuffer() const;
    std::string toString() const;
};

struct FetchResponse : ResponseHeader {
    TaggedFields tagged_fieldsH{};
    int32_t throttle_time = 0;
    int16_t error_code = 0;
    int32_t session_id = 0;

    struct AbortedTransactions {
        int64_t producer_id;
        int64_t first_offset;
        TaggedFields tagged_fields{};

        std::string toBuffer() const;
        std::string toString() const;
    };

    struct Partition {
        uint32_t partition_id{};
        uint16_t error_code{};
        int64_t high_watermark{};
        int64_t last_stable_offset{};
        int64_t log_start_offset{};
        std::vector<AbortedTransactions> aborted_transactions;
        int32_t preferred_read_replica{};
        std::vector<std::string> record_batches;
        TaggedFields tagged_fields{};

        std::string toBuffer() const;
        std::string toString() const;
    };

    struct Topic {
        std::array<char, 16> topic_uuid;
        std::vector<FetchResponse::Partition> partitions;
        TaggedFields tagged_fields{};

        std::string toBuffer() const;
        std::string toString() const;
    };

    std::vector<Topic> topics;
    TaggedFields tagged_fieldsT{};

    std::string toBuffer() const;
    std::string toString() const;
};

#pragma pack(pop)