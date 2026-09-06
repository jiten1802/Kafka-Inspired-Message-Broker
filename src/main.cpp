#include "KafkaApis.h"

namespace {

    std::function<void(int)> shutdown_handler;
    void signal_handler(int signal) { shutdown_handler(signal); }

    bool handleClient(const KafkaApis& kafka_apis, const Fd& client_fd,
        const TCPManager& tcp_manager) {
        return tcp_manager.readBufferFromClientFd(
            client_fd, [&kafka_apis](const char* buf, const size_t buf_size) {
                kafka_apis.classifyRequest(buf, buf_size);
            });
    }

} // namespace

int main(int argc, char* argv[]) {
    // Disable output buffering
    std::cout << std::unitbuf;
    std::cerr << std::unitbuf;

    TCPManager tcp_manager;
    tcp_manager.createSocketAndListen();
    ClusterMetadata cluster_metadata;

    shutdown_handler = [&tcp_manager](int signal) {
        std::cout << "Caught signal " << signal << '\n';
        tcp_manager.~TCPManager();
        exit(1);
        };

    signal(SIGINT, signal_handler);

    while (true) {
        Fd client_fd = tcp_manager.acceptConnections();
        tcp_manager.addClientThread(
            std::jthread([_client_fd = std::move(client_fd), &tcp_manager,
                &cluster_metadata](std::stop_token st) {
                    std::cout << "Attaching a new client thread. client-fd: "
                        << _client_fd << " \n";
                    KafkaApis kafka_apis(_client_fd, tcp_manager, cluster_metadata);

                    while (!st.stop_requested()) {
                        bool done =
                            handleClient(kafka_apis, _client_fd, tcp_manager);
                        if (done == false) { /// Client disconnected
                            break;
                        }
                    }

                    shutdown(_client_fd, SHUT_RDWR);
                    std::cout << "Client thread stopped\n";
                }));
    }

    return 0;
}