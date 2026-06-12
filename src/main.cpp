#include <iostream>
#include <cstdlib>
#include <string>
#include <cstring>
#include <cerrno>
#include <vector>
#include <unordered_map>
#include <unordered_set>
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

struct BlockedEntry {
  int fd;
  std::vector<std::string> keys;
  Clock::time_point deadline;  // Clock::time_point::max() for indefinite
};

// After a push to `key`, try to unblock the first blocked client waiting on it.
static bool try_unblock(
    const std::string &key,
    std::unordered_map<std::string, std::vector<std::string>> &lists,
    std::vector<BlockedEntry> &blocked,
    std::unordered_set<int> &blocked_fds)
{
  auto lit = lists.find(key);
  if (lit == lists.end() || lit->second.empty()) return false;

  for (auto it = blocked.begin(); it != blocked.end(); ++it) {
    for (const auto &k : it->keys) {
      if (k == key && !lit->second.empty()) {
        std::string value = std::move(lit->second.front());
        lit->second.erase(lit->second.begin());

        std::string resp = "*2\r\n" + encode_bulk_string(key) + encode_bulk_string(value);
        send(it->fd, resp.c_str(), resp.size(), 0);

        blocked_fds.erase(it->fd);
        blocked.erase(it);
        return true;
      }
    }
  }
  return false;
}

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
  std::vector<BlockedEntry> blocked_clients;
  std::unordered_set<int> blocked_fds;

  while (true) {
    // Compute poll timeout from nearest blocked-client deadline (if any).
    int poll_timeout = -1;
    if (!blocked_clients.empty()) {
      auto now = Clock::now();
      for (const auto &bc : blocked_clients) {
        if (bc.deadline == Clock::time_point::max()) continue;
        auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
            bc.deadline - now).count();
        if (remaining < 0) remaining = 0;
        if (poll_timeout == -1 || remaining < poll_timeout)
          poll_timeout = static_cast<int>(remaining);
      }
    }
    int n = poll(fds.data(), fds.size(), poll_timeout);
    if (n < 0) {
      if (errno == EINTR) continue;
      std::cerr << "poll failed\n";
      break;
    }

    // Expire blocked clients whose deadline has passed.
    if (!blocked_clients.empty()) {
      auto now = Clock::now();
      for (auto it = blocked_clients.begin(); it != blocked_clients.end(); ) {
        if (it->deadline != Clock::time_point::max() && now >= it->deadline) {
          std::string resp = "$-1\r\n";
          send(it->fd, resp.c_str(), resp.size(), 0);
          blocked_fds.erase(it->fd);
          it = blocked_clients.erase(it);
        } else {
          ++it;
        }
      }
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
      if (blocked_fds.count(fds[i].fd)) { ++i; continue; }
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
              try_unblock(args[1], lists, blocked_clients, blocked_fds);
            } else if (iequals(args[0], "LPUSH") && args.size() >= 3) {
              auto &list = lists[args[1]];
              for (size_t a = 2; a < args.size(); ++a) {
                list.insert(list.begin(), args[a]);
              }
              response = ":" + std::to_string(list.size()) + "\r\n";
              try_unblock(args[1], lists, blocked_clients, blocked_fds);
            } else if (iequals(args[0], "BLPOP") && args.size() >= 3) {
              double timeout_secs = std::strtod(args.back().c_str(), nullptr);
              bool found = false;
              for (size_t a = 1; a < args.size() - 1; ++a) {
                const std::string &key = args[a];
                auto it = lists.find(key);
                if (it != lists.end() && !it->second.empty()) {
                  std::string val = std::move(it->second.front());
                  it->second.erase(it->second.begin());
                  if (it->second.empty()) lists.erase(it);
                  response = "*2\r\n" + encode_bulk_string(key) + encode_bulk_string(val);
                  found = true;
                  break;
                }
              }
              if (!found) {
                std::vector<std::string> keys;
                for (size_t a = 1; a < args.size() - 1; ++a)
                  keys.push_back(args[a]);
                Clock::time_point deadline = Clock::time_point::max();
                if (timeout_secs > 0)
                  deadline = Clock::now() + std::chrono::milliseconds(static_cast<long>(timeout_secs * 1000));
                blocked_clients.push_back({fds[i].fd, std::move(keys), deadline});
                blocked_fds.insert(fds[i].fd);
              }
            } else if (iequals(args[0], "LLEN") && args.size() >= 2) {
              auto it = lists.find(args[1]);
              long len = (it != lists.end()) ? (long)it->second.size() : 0;
              response = ":" + std::to_string(len) + "\r\n";
            } else if (iequals(args[0], "LPOP") && args.size() >= 3) {
              long count = std::strtol(args[2].c_str(), nullptr, 10);
              auto it = lists.find(args[1]);
              if (it == lists.end()) {
                response = "*-1\r\n";  // null array: missing list
              } else {
                if (count < 0) count = 0;
                if (count > (long)it->second.size()) count = (long)it->second.size();
                std::vector<std::string> popped(it->second.begin(), it->second.begin() + count);
                it->second.erase(it->second.begin(), it->second.begin() + count);
                if (it->second.empty()) lists.erase(it);  // Redis deletes empty lists
                response = encode_array(popped);
              }
            } else if (iequals(args[0], "LPOP") && args.size() >= 2) {
              auto it = lists.find(args[1]);
              if (it != lists.end() && !it->second.empty()) {
                response = encode_bulk_string(it->second.front());
                it->second.erase(it->second.begin());
                if (it->second.empty()) lists.erase(it);  // Redis deletes empty lists
              } else {
                response = "$-1\r\n";  // null bulk string: empty or missing list
              }
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
          if (!response.empty())
            send(fds[i].fd, response.c_str(), response.size(), 0);
        }
        else if (bytes_received == 0) { drop = true;}
        else if (errno != EAGAIN && errno != EWOULDBLOCK) { drop = true; }
      }

      if (drop) {
        blocked_fds.erase(fds[i].fd);
        for (auto bit = blocked_clients.begin(); bit != blocked_clients.end(); ) {
          if (bit->fd == fds[i].fd) bit = blocked_clients.erase(bit);
          else ++bit;
        }
        close(fds[i].fd);
        fds.erase(fds.begin() + i);
      }
      else {++i;}
    }
  }

  close(server_fd);
  return 0;
}
