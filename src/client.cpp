#include <iostream>
#include <cstring>
#include <string>
#include <unistd.h>
#include <sys/socket.h>
#include <arpa/inet.h>

int main(int argc, char **argv) {
  std::cout << std::unitbuf;
  std::cerr << std::unitbuf;

  const char *host = "127.0.0.1";
  int port = 6379;
  if (argc >= 2) host = argv[1];
  if (argc >= 3) port = std::atoi(argv[2]);

  int sock = socket(AF_INET, SOCK_STREAM, 0);
  if (sock < 0) {
    std::cerr << "Failed to create socket\n";
    return 1;
  }

  struct sockaddr_in server_addr;
  std::memset(&server_addr, 0, sizeof(server_addr));
  server_addr.sin_family = AF_INET;
  server_addr.sin_port = htons(port);
  if (inet_pton(AF_INET, host, &server_addr.sin_addr) <= 0) { //parse IP string
    std::cerr << "Invalid address: " << host << "\n";
    close(sock);
    return 1;
  }

  if (connect(sock, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
    std::cerr << "Connection to " << host << ":" << port << " failed\n";
    close(sock);
    return 1;
  }
  std::cout << "Connected to " << host << ":" << port << "\n";

  // Send a RESP-encoded PING command
  const char *request = "*1\r\n$4\r\nPING\r\n";
  ssize_t sent = send(sock, request, std::strlen(request), 0);
  if (sent < 0) {
    std::cerr << "send failed\n";
    close(sock);
    return 1;
  }
  std::cout << "Sent: " << request;

  char buffer[1024];
  ssize_t n = recv(sock, buffer, sizeof(buffer) - 1, 0); //Reading a response into a buffer
  if (n < 0) {
    std::cerr << "recv failed\n";
    close(sock);
    return 1;
  }
  if (n == 0) {
    std::cout << "Server closed connection without responding\n";
    close(sock);
    return 0;
  }
  buffer[n] = '\0';
  std::cout << "Received (" << n << " bytes): " << buffer;

  close(sock);
  return 0;
}
