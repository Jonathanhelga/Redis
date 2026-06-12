#include <iostream>
#include <cstdlib>
#include <string>
#include <cstring>
#include <cerrno>
#include <vector>
#include <unordered_map>
#include <chrono>
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

// Parse a single RESP command (an array of bulk strings) from `input`.
// On success, fills `args` with the command name + arguments and returns true.
// Returns false if the buffer doesn't contain a complete, well-formed command.
static bool parse_command(const std::string &input, std::vector<std::string> &args) {
  args.clear();
  size_t pos = 0;
  if (pos >= input.size() || input[pos] != '*') return false;
  ++pos;

  size_t crlf = input.find("\r\n", pos);
  if (crlf == std::string::npos) return false;
  long count = std::strtol(input.substr(pos, crlf - pos).c_str(), nullptr, 10);
  if (count <= 0) return false;
  pos = crlf + 2;

  for (long i = 0; i < count; ++i) {
    if (pos >= input.size() || input[pos] != '$') return false;
    ++pos;
    crlf = input.find("\r\n", pos);
    if (crlf == std::string::npos) return false;
    long len = std::strtol(input.substr(pos, crlf - pos).c_str(), nullptr, 10);
    if (len < 0) return false;
    pos = crlf + 2;
    if (pos + (size_t)len + 2 > input.size()) return false;  // incomplete
    args.push_back(input.substr(pos, len));
    pos += len + 2;  // skip data + trailing \r\n
  }
  return true;
}

// Encode `s` as a RESP bulk string: $<len>\r\n<data>\r\n
static std::string encode_bulk_string(const std::string &s) {
  return "$" + std::to_string(s.size()) + "\r\n" + s + "\r\n";
}

// Encode a RESP array of bulk strings: *<count>\r\n followed by each element.
static std::string encode_array(const std::vector<std::string> &items) {
  std::string out = "*" + std::to_string(items.size()) + "\r\n";
  for (const auto &item : items) {
    out += encode_bulk_string(item);
  }
  return out;
}

static bool iequals(const std::string &a, const char *b) {
  return strcasecmp(a.c_str(), b) == 0;
}

using Clock = std::chrono::steady_clock;

// A stored value, with an optional expiry deadline.
struct Entry {
  std::string value;
  bool has_expiry = false;
  Clock::time_point expires_at;
};

int main(int argc, char **argv) {
  std::cout << std::unitbuf;
  std::cerr << std::unitbuf;
  //flushes the stream after every insertion, without this I might see no output before a crash.

  int server_fd = socket(AF_INET, SOCK_STREAM, 0);
  //socket(domain, type, protocol) returns a new socket fd (file descriptor), or -1 on error. is like calling the phone company and saying "I want a phone", they give me back a number
  // server_fd is what we refer to that phone from now
  //AF_INET = IPv4 address family
  //SOCK_STREAM =  a reliable, ordered byte stream → TCP.
  //0 = let the kernel pick the default protocol for that family/type (TCP for AF_INET+SOCK_STREAM).
  if (server_fd < 0) {
    std::cerr << "Failed to create server socket\n";
    return 1;
  }

  int reuse = 1;
  if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
  // setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) it allows rebind to the port quickly after restarts(avoids "Address already in use")
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

  // The key-value store, shared across all clients.
  std::unordered_map<std::string, Entry> store;
  // List store, keyed separately from the string store.
  std::unordered_map<std::string, std::vector<std::string>> lists;

  while (true) {
    int n = poll(fds.data(), fds.size(), -1);
    if (n < 0) {
      if (errno == EINTR) continue;
      std::cerr << "poll failed\n";
      break;
    }
    
    // Accept new connections.
    if (fds[0].revents != 0) {
      --n;
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
    }

    // Handle existing clients — stop once we've found every flagged fd.
    for (size_t i = 1; i < fds.size() && n > 0;) {
      if (fds[i].revents == 0) { ++i; continue; }
      --n;
      bool drop = false;
      if (fds[i].revents & (POLLERR | POLLHUP | POLLNVAL)) {
        drop = true;
      } else if (fds[i].revents & POLLIN) {
        char buffer[1024];
        int bytes_received = recv(fds[i].fd, buffer, sizeof(buffer), 0);
        if (bytes_received > 0) {
          std::string input(buffer, bytes_received);
          std::vector<std::string> args;
          std::string response;
          if (parse_command(input, args) && !args.empty()) {
            if (iequals(args[0], "ECHO") && args.size() >= 2) {
              response = encode_bulk_string(args[1]);
            } else if (iequals(args[0], "SET") && args.size() >= 3) {
              Entry entry;
              entry.value = args[2];
              // Optional: SET key value PX <milliseconds>
              if (args.size() >= 5 && iequals(args[3], "PX")) {
                long ms = std::strtol(args[4].c_str(), nullptr, 10);
                entry.has_expiry = true;
                entry.expires_at = Clock::now() + std::chrono::milliseconds(ms);
              }
              store[args[1]] = std::move(entry);
              response = "+OK\r\n";
            } else if (iequals(args[0], "GET") && args.size() >= 2) {
              auto it = store.find(args[1]);
              if (it != store.end() && it->second.has_expiry &&
                  Clock::now() >= it->second.expires_at) {
                store.erase(it);          // lazily drop the expired key
                it = store.end();
              }
              if (it != store.end()) {
                response = encode_bulk_string(it->second.value);
              } else {
                response = "$-1\r\n";  // null bulk string: key not found / expired
              }
            } else if (iequals(args[0], "RPUSH") && args.size() >= 3) {
              auto &list = lists[args[1]];  // creates an empty list if absent
              for (size_t a = 2; a < args.size(); ++a) {
                list.push_back(args[a]);
              }
              response = ":" + std::to_string(list.size()) + "\r\n";
            } else if (iequals(args[0], "LPUSH") && args.size() >= 3) {
              auto &list = lists[args[1]];
              for (size_t a = 2; a < args.size(); ++a) {
                list.insert(list.begin(), args[a]);
              }
              response = ":" + std::to_string(list.size()) + "\r\n";
            } else if (iequals(args[0], "LLEN") && args.size() >= 2) {
              auto it = lists.find(args[1]);
              long len = (it != lists.end()) ? (long)it->second.size() : 0;
              response = ":" + std::to_string(len) + "\r\n";
            } else if (iequals(args[0], "LRANGE") && args.size() >= 4) {
              long start = std::strtol(args[2].c_str(), nullptr, 10);
              long stop = std::strtol(args[3].c_str(), nullptr, 10);
              std::vector<std::string> range;
              auto it = lists.find(args[1]);
              if (it != lists.end()) {
                long size = (long)it->second.size();
                // Negative indexes count from the end (-1 is the last element).
                if (start < 0) start += size;
                if (stop < 0) stop += size;
                if (start < 0) start = 0;
                if (stop >= size) stop = size - 1;
                for (long idx = start; idx <= stop; ++idx) {
                  range.push_back(it->second[idx]);
                }
              }
              response = encode_array(range);
            } else {
              response = "+PONG\r\n";
            }
          } else {
            response = "+PONG\r\n";
          }
          send(fds[i].fd, response.c_str(), response.size(), 0);
        }
        else if (bytes_received == 0) { drop = true;}
        else if (errno != EAGAIN && errno != EWOULDBLOCK) { drop = true; }
      }

      if (drop) {
        close(fds[i].fd);
        fds.erase(fds.begin() + i);
      }
      else {++i;}
    }
  }

  close(server_fd);
  return 0;
}
