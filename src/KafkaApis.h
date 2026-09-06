#pragma once

#include "ClusterMetadata.h"
#include "TCPManager.h"

struct KafkaApis {
  KafkaApis(const Fd&, const TCPManager&, const ClusterMetadata&);
  ~KafkaApis() = default;

  static constexpr uint32_t UNSUPPORTED_VERSION = 35;
  static constexpr uint16_t UNKNOWN_TOPIC_OR_PARTITION = 3;
  static constexpr uint16_t NO_ERROR = 0;
  static constexpr uint16_t UNKNOWN_TOPIC = 100;

  static constexpr uint16_t API_VERSIONS_REQUEST = 18;
  static constexpr uint16_t DESCRIBE_TOPIC_PARTITIONS_REQUEST = 75;
  static constexpr uint16_t FETCH_REQUEST = 1;

  void classifyRequest(const char* buf, const size_t buf_size) const;
  void checkApiVersions(const char* buf, const size_t buf_size) const;
  void describeTopicPartitions(const char* buf, const size_t buf_size) const;
  void fetchTopicMessages(const char* buf, const size_t buf_size) const;

private:
  const Fd& client_fd;
  const TCPManager& tcp_manager;
  const ClusterMetadata& cluster_metadata;
};