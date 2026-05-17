#include <iostream>
#include <cstdlib>
#include <string>
#include <cstring>
#include <cerrno>
#include <vector>
#include <unistd.h>
#include <fcntl.h>
#include <poll.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>

static bool set_nonblocking(int fd) {
  int flags = fcntl(fd, F_GETFL, 0);
  if (flags < 0) return false;
  return fcntl(fd, F_SETFL, flags | O_NONBLOCK) >= 0;
}

int main(int argc, char **argv) {
  std::cout << std::unitbuf;
  std::cerr << std::unitbuf;

  int server_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (server_fd < 0) {
    std::cerr << "Failed to create server socket\n";
    return 1;
  }

  int reuse = 1;
  if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
    std::cerr << "setsockopt failed\n";
    return 1;
  }

  struct sockaddr_in server_addr;
  server_addr.sin_family = AF_INET;
  server_addr.sin_addr.s_addr = INADDR_ANY;
  server_addr.sin_port = htons(6379);

  if (bind(server_fd, (struct sockaddr *) &server_addr, sizeof(server_addr)) != 0) {
    std::cerr << "Failed to bind to port 6379\n";
    return 1;
  }

  int connection_backlog = 5;
  if (listen(server_fd, connection_backlog) != 0) {
    std::cerr << "listen failed\n";
    return 1;
  }

  if (!set_nonblocking(server_fd)) {
    std::cerr << "Failed to set listener non-blocking\n";
    return 1;
  }

  std::cout << "Waiting for clients to connect...\n";

  std::vector<pollfd> fds;
  fds.push_back({server_fd, POLLIN, 0});

  while (true) {
    int n = poll(fds.data(), fds.size(), -1);
    if (n < 0) {
      if (errno == EINTR) continue;
      std::cerr << "poll failed\n";
      break;
    }

    // Accept new connections.
    if (fds[0].revents & POLLIN) {
      while (true) {
        struct sockaddr_in client_addr;
        socklen_t client_addr_len = sizeof(client_addr);
        int client_fd = accept(server_fd, (struct sockaddr *)&client_addr, &client_addr_len);
        if (client_fd < 0) {
          if (errno == EAGAIN || errno == EWOULDBLOCK) break;
          std::cerr << "accept failed\n";
          break;
        }
        set_nonblocking(client_fd);
        fds.push_back({client_fd, POLLIN, 0});
        std::cout << "Client connected (fd=" << client_fd << ")\n";
      }
    }

    // Handle existing clients.
    for (size_t i = 1; i < fds.size();) {
      bool drop = false;
      if (fds[i].revents & (POLLERR | POLLHUP | POLLNVAL)) {
        drop = true;
      } else if (fds[i].revents & POLLIN) {
        char buffer[1024];
        int bytes_received = recv(fds[i].fd, buffer, sizeof(buffer), 0);
        if (bytes_received > 0) {
          const char *response = "+PONG\r\n";
          send(fds[i].fd, response, strlen(response), 0);
        } else if (bytes_received == 0) {
          drop = true;
        } else if (errno != EAGAIN && errno != EWOULDBLOCK) {
          drop = true;
        }
      }

      if (drop) {
        close(fds[i].fd);
        fds.erase(fds.begin() + i);
      } else {
        ++i;
      }
    }
  }

  close(server_fd);
  return 0;
}
