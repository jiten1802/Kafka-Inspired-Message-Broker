#include "TCPManager.h"

#include "MessageDefs.h"

#include <netinet/tcp.h>
#include <unistd.h>

Fd& Fd::operator=(Fd&& other) noexcept {
    if (this != &other) {
        if (fd >= 0)
            close(fd);
        fd = other.fd;
        other.fd = -1;
    }
    return *this;
}

Fd::~Fd() {
    if (fd != -1) {
        std::cout << "Closing file descriptor " << fd << "\n";
        close(fd);
    }
    else {
        std::cout << "File descriptor already closed " << fd << "\n";
    }
    fd = -1;
}

void TCPManager::createSocketAndListen() {
    server_fd.setFd(socket(AF_INET, SOCK_STREAM, 0));
    if (server_fd < 0) {
        perror("socket failed: ");
        throw std::runtime_error("Failed to create server socket: ");
    }

    // Since the tester restarts your program quite often, setting SO_REUSEADDR
    // ensures that we don't run into 'Address already in use' errors
    int reuse = 1;
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) <
        0) {
        perror("setsockopt failed: ");
        close(server_fd);
        throw std::runtime_error("setsockopt failed: ");
    }

    struct sockaddr_in server_addr = getSocketAddr();

    if (bind(server_fd, reinterpret_cast<struct sockaddr*>(&server_addr),
        sizeof(server_addr)) != 0) {
        perror("bind failed: ");
        close(server_fd);
        throw std::runtime_error("Failed to bind to port 9092");
    }

    std::cout << "Waiting for a client to connect...\n";

    int connection_backlog = 5;
    if (listen(server_fd, connection_backlog) != 0) {
        perror("listen failed: ");
        close(server_fd);
        throw std::runtime_error("listen failed");
    }

    // You can use print statements as follows for debugging, they'll be visible
    // when running tests.
    std::cerr << "Logs from your program will appear here!\n";
}

Fd TCPManager::acceptConnections() const {
    struct sockaddr_in client_addr {};
    socklen_t client_addr_len = sizeof(client_addr);

    struct sockaddr* addr = reinterpret_cast<struct sockaddr*>(&client_addr);
    Fd client_fd(accept(server_fd, addr, &client_addr_len));

    if (client_fd < 0) {
        perror("accept failed: ");
        throw std::runtime_error("Failed to accept connection: ");
    }

    std::cout << "Client connected\n";
    return client_fd;
}

void TCPManager::writeBufferOnClientFd(const Fd& client_fd,
    const auto& response_message) const {

    std::cout << "Sending msg to client: " << response_message.toString()
        << "\n";

    std::string buffer = response_message.toBuffer();

    // Write message Length
    if (send(client_fd, buffer.data(), sizeof(uint32_t), 0) !=
        sizeof(uint32_t)) {
        perror("send failed: ");
        throw std::runtime_error("Failed to send msgLen to client: ");
    }

    if (send(client_fd, buffer.data() + 4, buffer.size() - 4, 0) !=
        buffer.size() - 4) {
        perror("send failed: ");
        throw std::runtime_error("Failed to send msgLen to client: ");
    }

    std::cout << "Message sent to client: " << buffer.size() << " bytes\n";

    // Just flush the write buffer
    int optval = 1;
    setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, &optval, sizeof(optval));
}

bool TCPManager::readBufferFromClientFd(
    const Fd& client_fd,
    const std::function<void(const char*, const size_t)>& func) const {
    char buffer[MAX_BUFFER_SIZE];
    size_t bytes_received = recv(client_fd, buffer, MAX_BUFFER_SIZE, 0);

    if (bytes_received < 0) {
        perror("recv failed: ");
        throw std::runtime_error("Failed to read from client: ");
    }

    if (bytes_received == 0) {
        std::cout << "Client disconnected\n";
        return false;
    }

    std::cout << "Received " << bytes_received << " bytes from client\n";
    func(buffer, bytes_received);
    return true;
}

template void
TCPManager::writeBufferOnClientFd(const Fd&,
    const ApiVersionsResponseMessage&) const;
template void TCPManager::writeBufferOnClientFd(
    const Fd&, const DescribeTopicPartitionsResponse&) const;
template void TCPManager::writeBufferOnClientFd(const Fd&,
    const FetchResponse&) const;