#include <iostream>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <cstring>
#include <cerrno>
#include <cstdint>
#include <cctype>
#include <fstream>
#include <sstream>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <algorithm>
#include <chrono>
#include <climits>
#include <cmath>
#include <unistd.h>
#include <fcntl.h>
#include <poll.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <arpa/inet.h>
#include <netdb.h>

static bool set_nonblocking(int fd) {
  int flags = fcntl(fd, F_GETFL, 0);
  if (flags < 0) return false;
  return fcntl(fd, F_SETFL, flags | O_NONBLOCK) >= 0;
}

// Parse a single RESP command (an array of bulk strings) from `input`.
// On success, fills `args` with the command name + arguments, sets `consumed` to the
// number of bytes the command occupied in `input`, and returns true.
// Returns false if the buffer doesn't contain a complete, well-formed command.
static bool parse_command(const std::string &input, std::vector<std::string> &args, size_t &consumed) {
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
  consumed = pos;
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

// Formats a sorted-set score the way Redis does: integral values are printed
// without a decimal point, otherwise the shortest decimal that round-trips.
static std::string format_score(double score) {
  if (score == (double)(long long)score) {
    return std::to_string((long long)score);
  }
  for (int prec = 1; prec <= 17; ++prec) {
    char buf[64];
    snprintf(buf, sizeof(buf), "%.*f", prec, score);
    if (std::strtod(buf, nullptr) == score) {
      return std::string(buf);
    }
  }
  char buf[64];
  snprintf(buf, sizeof(buf), "%.17f", score);
  return std::string(buf);
}

// Commands that mutate the dataset and must be propagated to connected replicas.
static bool is_write_command(const std::string &cmd) {
  static const char *writes[] = {"SET", "INCR", "RPUSH", "LPUSH", "LPOP", "XADD", "ZADD", "ZREM", "GEOADD"};
  for (const char *w : writes) {
    if (iequals(cmd, w)) return true;
  }
  return false;
}

// Inserts/updates `member` at `score` in a sorted set kept ordered by (score, member).
// Returns 1 if a new member was added, 0 if an existing member's score was updated.
static int zset_add(std::vector<std::pair<double, std::string>> &zset, double score, const std::string &member) {
  auto it = std::find_if(zset.begin(), zset.end(),
      [&](const auto &p) { return p.second == member; });
  int added = 0;
  if (it != zset.end()) {
    zset.erase(it);
  } else {
    added = 1;
  }
  auto pos = std::lower_bound(zset.begin(), zset.end(), std::make_pair(score, member),
      [](const auto &a, const auto &b) {
        if (a.first != b.first) return a.first < b.first;
        return a.second < b.second;
      });
  zset.insert(pos, {score, member});
  return added;
}

static const double GEO_LAT_MIN = -85.05112878;
static const double GEO_LAT_MAX = 85.05112878;
static const double GEO_LON_MIN = -180.0;
static const double GEO_LON_MAX = 180.0;

// Interleaves the bits of two 32-bit values into a 64-bit geohash (y in odd bit positions).
static uint64_t interleave64(uint32_t xlo, uint32_t ylo) {
  static const uint64_t B[] = {0x5555555555555555ULL, 0x3333333333333333ULL,
                                0x0F0F0F0F0F0F0F0FULL, 0x00FF00FF00FF00FFULL,
                                0x0000FFFF0000FFFFULL};
  static const unsigned S[] = {1, 2, 4, 8, 16};
  uint64_t x = xlo, y = ylo;
  for (int i = 4; i >= 0; --i) {
    x = (x | (x << S[i])) & B[i];
    y = (y | (y << S[i])) & B[i];
  }
  return x | (y << 1);
}

// Reverses interleave64, splitting a 64-bit geohash back into its two 32-bit values.
static void deinterleave64(uint64_t interleaved, uint32_t &x_out, uint32_t &y_out) {
  static const uint64_t B[] = {0x5555555555555555ULL, 0x3333333333333333ULL,
                                0x0F0F0F0F0F0F0F0FULL, 0x00FF00FF00FF00FFULL,
                                0x0000FFFF0000FFFFULL, 0x00000000FFFFFFFFULL};
  static const unsigned S[] = {0, 1, 2, 4, 8, 16};
  uint64_t x = interleaved;
  uint64_t y = interleaved >> 1;
  x &= B[0];
  y &= B[0];
  for (int i = 1; i <= 5; ++i) {
    x = (x | (x >> S[i])) & B[i];
    y = (y | (y >> S[i])) & B[i];
  }
  x_out = (uint32_t)x;
  y_out = (uint32_t)y;
}

// Encodes longitude/latitude into the 52-bit geohash score Redis stores in sorted sets.
static uint64_t geohash_encode(double longitude, double latitude) {
  double lat_offset = (latitude - GEO_LAT_MIN) / (GEO_LAT_MAX - GEO_LAT_MIN);
  double lon_offset = (longitude - GEO_LON_MIN) / (GEO_LON_MAX - GEO_LON_MIN);
  uint32_t lat_bits = (uint32_t)(lat_offset * (double)(1ULL << 26));
  uint32_t lon_bits = (uint32_t)(lon_offset * (double)(1ULL << 26));
  return interleave64(lat_bits, lon_bits);
}

// Decodes a geohash score back into the (lossy) longitude/latitude at the center of its cell.
static void geohash_decode(uint64_t geo_score, double &longitude, double &latitude) {
  uint32_t lat_bits, lon_bits;
  deinterleave64(geo_score, lat_bits, lon_bits);
  double cell = (double)(1ULL << 26);
  double lat_min = GEO_LAT_MIN + ((double)lat_bits / cell) * (GEO_LAT_MAX - GEO_LAT_MIN);
  double lat_max = GEO_LAT_MIN + ((double)(lat_bits + 1) / cell) * (GEO_LAT_MAX - GEO_LAT_MIN);
  double lon_min = GEO_LON_MIN + ((double)lon_bits / cell) * (GEO_LON_MAX - GEO_LON_MIN);
  double lon_max = GEO_LON_MIN + ((double)(lon_bits + 1) / cell) * (GEO_LON_MAX - GEO_LON_MIN);
  latitude = (lat_min + lat_max) / 2.0;
  longitude = (lon_min + lon_max) / 2.0;
}

// Great-circle distance in meters between two points, using the radius Redis uses internally.
static double haversine_distance(double lon1, double lat1, double lon2, double lat2) {
  const double EARTH_RADIUS_M = 6372797.560856;
  double lat1r = lat1 * M_PI / 180.0;
  double lon1r = lon1 * M_PI / 180.0;
  double lat2r = lat2 * M_PI / 180.0;
  double lon2r = lon2 * M_PI / 180.0;
  double u = std::sin((lat2r - lat1r) / 2.0);
  double v = std::sin((lon2r - lon1r) / 2.0);
  return 2.0 * EARTH_RADIUS_M * std::asin(std::sqrt(u * u + std::cos(lat1r) * std::cos(lat2r) * v * v));
}

// Converts a GEOSEARCH distance unit (m/km/mi/ft) to meters.
static double geo_unit_to_meters(const std::string &unit) {
  if (iequals(unit, "km")) return 1000.0;
  if (iequals(unit, "mi")) return 1609.34;
  if (iequals(unit, "ft")) return 0.3048;
  return 1.0;
}

// The replication ID this server reports as a master.
static const std::string MASTER_REPLID = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";

// RDB persistence config, set from --dir / --dbfilename and reported by CONFIG GET.
static std::string g_dir = ".";
static std::string g_dbfilename = "dump.rdb";

// AOF persistence config, set from --appendonly / --appenddirname / --appendfilename /
// --appendfsync and reported by CONFIG GET.
static std::string g_appendonly = "no";
static std::string g_appenddirname = "appendonlydir";
static std::string g_appendfilename = "appendonly.aof";
static std::string g_appendfsync = "everysec";

// Resolved path of the active incremental AOF file, set at startup if appendonly is "yes".
static std::string g_aof_path;

// Appends a RESP-encoded command to the active AOF file, if AOF persistence is enabled.
static void append_to_aof(const std::vector<std::string> &args) {
  if (g_aof_path.empty()) return;
  std::ofstream out(g_aof_path, std::ios::app | std::ios::binary);
  out << encode_array(args);
}

// Hex-encoded contents of an empty RDB file, sent to replicas on full resync.
static const std::string EMPTY_RDB_HEX =
    "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469"
    "732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0"
    "c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";

// Decode a hex string into its raw binary representation.
static std::string hex_to_binary(const std::string &hex) {
  std::string out;
  out.reserve(hex.size() / 2);
  for (size_t i = 0; i + 1 < hex.size(); i += 2) {
    out.push_back(static_cast<char>(std::strtol(hex.substr(i, 2).c_str(), nullptr, 16)));
  }
  return out;
}

// Sends `data` in full, looping until every byte is written or the connection fails.
static void send_all(int fd, const std::string &data) {
  size_t sent = 0;
  while (sent < data.size()) {
    ssize_t n = send(fd, data.data() + sent, data.size() - sent, 0);
    if (n <= 0) return;
    sent += static_cast<size_t>(n);
  }
}

// Reads bytes from `fd` one at a time until a "\r\n"-terminated line is read.
static std::string recv_line(int fd) {
  std::string result;
  char c;
  while (true) {
    ssize_t n = recv(fd, &c, 1, 0);
    if (n <= 0) break;
    result += c;
    if (result.size() >= 2 && result[result.size() - 2] == '\r' && result[result.size() - 1] == '\n')
      break;
  }
  return result;
}

// Connects to the master and performs the replication handshake (PING, REPLCONF x2, PSYNC).
// Returns the connected socket fd, or -1 on failure.
static int connect_to_master(const std::string &host, int master_port, int our_port) {
  struct addrinfo hints{};
  hints.ai_family = AF_INET;
  hints.ai_socktype = SOCK_STREAM;
  struct addrinfo *res = nullptr;
  if (getaddrinfo(host.c_str(), std::to_string(master_port).c_str(), &hints, &res) != 0) {
    return -1;
  }

  int fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0 || connect(fd, res->ai_addr, res->ai_addrlen) != 0) {
    freeaddrinfo(res);
    if (fd >= 0) close(fd);
    return -1;
  }
  freeaddrinfo(res);

  send_all(fd, encode_array({"PING"}));
  recv_line(fd);

  send_all(fd, encode_array({"REPLCONF", "listening-port", std::to_string(our_port)}));
  recv_line(fd);

  send_all(fd, encode_array({"REPLCONF", "capa", "psync2"}));
  recv_line(fd);

  send_all(fd, encode_array({"PSYNC", "?", "-1"}));
  recv_line(fd);  // +FULLRESYNC <replid> <offset>\r\n

  // Consume the RDB file the master sends next: $<length>\r\n<binary contents> (no trailing \r\n).
  std::string rdb_header = recv_line(fd);  // $<length>\r\n
  long rdb_len = std::strtol(rdb_header.c_str() + 1, nullptr, 10);
  long received = 0;
  char buf[4096];
  while (received < rdb_len) {
    ssize_t n = recv(fd, buf, std::min((long)sizeof(buf), rdb_len - received), 0);
    if (n <= 0) break;
    received += n;
  }

  set_nonblocking(fd);
  return fd;
}

using Clock = std::chrono::steady_clock;

// A stored value, with an optional expiry deadline.
struct Entry {
  std::string value;
  bool has_expiry = false;
  Clock::time_point expires_at;
};

// Reads an RDB length-encoding at `pos`. If the encoding is one of the special
// string types (integer-as-string), sets `is_special` and `special_type` to its
// low 6 bits (0 = 8-bit int, 1 = 16-bit int, 2 = 32-bit int) and returns 0.
static unsigned long long rdb_read_length(const std::string &data, size_t &pos, bool &is_special, int &special_type) {
  is_special = false;
  unsigned char b0 = (unsigned char)data[pos++];
  unsigned char top = b0 >> 6;
  if (top == 0b00) {
    return b0 & 0x3F;
  } else if (top == 0b01) {
    unsigned char b1 = (unsigned char)data[pos++];
    return ((b0 & 0x3F) << 8) | b1;
  } else if (top == 0b10) {
    unsigned long long val = 0;
    for (int i = 0; i < 4; ++i) {
      val = (val << 8) | (unsigned char)data[pos++];
    }
    return val;
  } else {
    is_special = true;
    special_type = b0 & 0x3F;
    return 0;
  }
}

// Reads an RDB string encoding (length-prefixed bytes, or an integer-as-string).
static std::string rdb_read_string(const std::string &data, size_t &pos) {
  bool is_special;
  int special_type;
  unsigned long long len = rdb_read_length(data, pos, is_special, special_type);
  if (is_special) {
    if (special_type == 0) {
      int8_t v = (int8_t)(unsigned char)data[pos++];
      return std::to_string((int)v);
    } else if (special_type == 1) {
      uint16_t u = (unsigned char)data[pos] | ((unsigned char)data[pos + 1] << 8);
      pos += 2;
      return std::to_string((int16_t)u);
    } else if (special_type == 2) {
      uint32_t u = (unsigned char)data[pos] | ((unsigned char)data[pos + 1] << 8) |
          ((unsigned char)data[pos + 2] << 16) | ((unsigned char)data[pos + 3] << 24);
      pos += 4;
      return std::to_string((int32_t)u);
    }
    return ""; // LZF-compressed strings are not expected in this challenge.
  }
  std::string s = data.substr(pos, len);
  pos += len;
  return s;
}

// Loads string keys (and their expiry, if any) from an RDB file into `store`.
// If the file doesn't exist, the database is treated as empty.
static void load_rdb_file(const std::string &path, std::unordered_map<std::string, Entry> &store) {
  std::ifstream file(path, std::ios::binary);
  if (!file) return;
  std::string data((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
  if (data.size() < 9 || data.substr(0, 5) != "REDIS") return;

  size_t pos = 9; // skip "REDIS0011" header
  bool has_expiry = false;
  unsigned long long expiry_ms = 0;

  while (pos < data.size()) {
    unsigned char opcode = (unsigned char)data[pos];
    if (opcode == 0xFF) {
      break;
    } else if (opcode == 0xFE) {
      ++pos;
      bool is_special;
      int special_type;
      rdb_read_length(data, pos, is_special, special_type); // db index
    } else if (opcode == 0xFB) {
      ++pos;
      bool is_special;
      int special_type;
      rdb_read_length(data, pos, is_special, special_type); // hash table size
      rdb_read_length(data, pos, is_special, special_type); // expires table size
    } else if (opcode == 0xFA) {
      ++pos;
      rdb_read_string(data, pos); // aux name
      rdb_read_string(data, pos); // aux value
    } else if (opcode == 0xFC) {
      ++pos;
      expiry_ms = 0;
      for (int i = 0; i < 8; ++i) {
        expiry_ms |= ((unsigned long long)(unsigned char)data[pos++]) << (8 * i);
      }
      has_expiry = true;
    } else if (opcode == 0xFD) {
      ++pos;
      unsigned long long expiry_s = 0;
      for (int i = 0; i < 4; ++i) {
        expiry_s |= ((unsigned long long)(unsigned char)data[pos++]) << (8 * i);
      }
      expiry_ms = expiry_s * 1000;
      has_expiry = true;
    } else {
      unsigned char value_type = opcode;
      ++pos;
      std::string key = rdb_read_string(data, pos);
      if (value_type == 0) {
        std::string value = rdb_read_string(data, pos);
        Entry entry;
        entry.value = value;
        if (has_expiry) {
          entry.has_expiry = true;
          auto target = std::chrono::system_clock::time_point(std::chrono::milliseconds(expiry_ms));
          entry.expires_at = Clock::now() + (target - std::chrono::system_clock::now());
        }
        store[key] = std::move(entry);
      } else {
        break; // value types other than string are not used in this challenge.
      }
      has_expiry = false;
      expiry_ms = 0;
    }
  }
}

// A single entry in a stream: an ID plus an ordered list of field-value pairs.
struct StreamEntry {
  std::string id;
  std::vector<std::pair<std::string, std::string>> fields;
};

struct BlockedEntry {
  int fd;
  std::vector<std::string> keys;
  Clock::time_point deadline;  // Clock::time_point::max() for indefinite
};

// A client blocked in XREAD, waiting for new entries on one or more streams.
struct BlockedXReadEntry {
  int fd;
  // (stream key, start ms, start seq) — only entries with an ID greater than this are returned.
  std::vector<std::tuple<std::string, unsigned long long, unsigned long long>> queries;
  Clock::time_point deadline;  // Clock::time_point::max() for indefinite
};

// Build the "*2\r\n<key><entries...>" portion of an XREAD response for each query whose
// stream has entries with an ID greater than the query's start ID. Returns how many
// streams had new entries.
static size_t build_xread_streams(
    const std::vector<std::tuple<std::string, unsigned long long, unsigned long long>> &queries,
    const std::unordered_map<std::string, std::vector<StreamEntry>> &streams,
    std::string &streams_resp)
{
  size_t active_streams = 0;
  for (const auto &[key, start_ms, start_seq] : queries) {
    auto sit = streams.find(key);
    if (sit == streams.end()) continue;

    std::string entries_resp;
    long entry_count = 0;
    for (const auto &entry : sit->second) {
      auto ed = entry.id.find('-');
      unsigned long long e_ms = std::strtoull(entry.id.substr(0, ed).c_str(), nullptr, 10);
      unsigned long long e_seq = std::strtoull(entry.id.substr(ed + 1).c_str(), nullptr, 10);

      if (e_ms > start_ms || (e_ms == start_ms && e_seq > start_seq)) {
        std::vector<std::string> fields_resp;
        for (const auto &fv : entry.fields) {
          fields_resp.push_back(fv.first);
          fields_resp.push_back(fv.second);
        }
        entries_resp += "*2\r\n" + encode_bulk_string(entry.id) + encode_array(fields_resp);
        ++entry_count;
      }
    }

    if (entry_count > 0) {
      ++active_streams;
      streams_resp += "*2\r\n" + encode_bulk_string(key) + "*" + std::to_string(entry_count) + "\r\n" + entries_resp;
    }
  }
  return active_streams;
}

// After a push to `key`, wake any blocked XREAD clients that now have new entries.
static void try_unblock_xreads(
    const std::string &key,
    std::unordered_map<std::string, std::vector<StreamEntry>> &streams,
    std::vector<BlockedXReadEntry> &blocked_xreads,
    std::unordered_set<int> &blocked_fds)
{
  for (auto it = blocked_xreads.begin(); it != blocked_xreads.end(); ) {
    bool waits_on_key = false;
    for (const auto &q : it->queries) {
      if (std::get<0>(q) == key) { waits_on_key = true; break; }
    }
    if (!waits_on_key) { ++it; continue; }

    std::string streams_resp;
    size_t active = build_xread_streams(it->queries, streams, streams_resp);
    if (active > 0) {
      std::string resp = "*" + std::to_string(active) + "\r\n" + streams_resp;
      send(it->fd, resp.c_str(), resp.size(), 0);
      blocked_fds.erase(it->fd);
      it = blocked_xreads.erase(it);
    } else {
      ++it;
    }
  }
}

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

// Executes a single command (used both for direct dispatch and for commands queued via MULTI/EXEC).
static std::string execute_command(
    const std::vector<std::string> &args,
    int fd,
    std::unordered_map<std::string, Entry> &store,
    std::unordered_map<std::string, std::vector<std::string>> &lists,
    std::unordered_map<std::string, std::vector<StreamEntry>> &streams,
    std::vector<BlockedEntry> &blocked_clients,
    std::vector<BlockedXReadEntry> &blocked_xreads,
    std::unordered_set<int> &blocked_fds,
    std::unordered_map<std::string, unsigned long long> &key_versions,
    std::vector<int> &replica_fds,
    long long &master_repl_offset,
    bool is_replica,
    std::unordered_map<int, std::unordered_set<std::string>> &subscriptions,
    std::unordered_map<std::string, std::vector<std::pair<double, std::string>>> &sorted_sets)
{
  std::string response;
  bool in_subscribed_mode = subscriptions.count(fd) && !subscriptions.at(fd).empty();
  if (in_subscribed_mode &&
      !iequals(args[0], "SUBSCRIBE") && !iequals(args[0], "UNSUBSCRIBE") &&
      !iequals(args[0], "PSUBSCRIBE") && !iequals(args[0], "PUNSUBSCRIBE") &&
      !iequals(args[0], "PING") && !iequals(args[0], "QUIT") && !iequals(args[0], "RESET")) {
    std::string cmd_lower = args[0];
    for (char &c : cmd_lower) c = (char)std::tolower((unsigned char)c);
    response = "-ERR Can't execute '" + cmd_lower +
        "': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context\r\n";
  } else if (iequals(args[0], "ECHO") && args.size() >= 2) {
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
              ++key_versions[args[1]];
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
            } else if (iequals(args[0], "INCR") && args.size() >= 2) {
              auto it = store.find(args[1]);
              if (it != store.end() && it->second.has_expiry &&
                  Clock::now() >= it->second.expires_at) {
                store.erase(it);
                it = store.end();
              }
              if (it != store.end()) {
                char *end = nullptr;
                long long val = std::strtoll(it->second.value.c_str(), &end, 10);
                if (*end != '\0' || it->second.value.empty()) {
                  response = "-ERR value is not an integer or out of range\r\n";
                } else if (val == LLONG_MAX) {
                  response = "-ERR value is not an integer or out of range\r\n";
                } else {
                  val += 1;
                  it->second.value = std::to_string(val);
                  ++key_versions[args[1]];
                  response = ":" + std::to_string(val) + "\r\n";
                }
              } else {
                Entry entry;
                entry.value = "1";
                store[args[1]] = std::move(entry);
                ++key_versions[args[1]];
                response = ":1\r\n";
              }
            } else if (iequals(args[0], "RPUSH") && args.size() >= 3) {
              auto &list = lists[args[1]];  // creates an empty list if absent
              for (size_t a = 2; a < args.size(); ++a) {
                list.push_back(args[a]);
              }
              ++key_versions[args[1]];
              response = ":" + std::to_string(list.size()) + "\r\n";
              try_unblock(args[1], lists, blocked_clients, blocked_fds);
            } else if (iequals(args[0], "LPUSH") && args.size() >= 3) {
              auto &list = lists[args[1]];
              for (size_t a = 2; a < args.size(); ++a) {
                list.insert(list.begin(), args[a]);
              }
              ++key_versions[args[1]];
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
                  ++key_versions[key];
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
                blocked_clients.push_back({fd, std::move(keys), deadline});
                blocked_fds.insert(fd);
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
                ++key_versions[args[1]];
                response = encode_array(popped);
              }
            } else if (iequals(args[0], "LPOP") && args.size() >= 2) {
              auto it = lists.find(args[1]);
              if (it != lists.end() && !it->second.empty()) {
                response = encode_bulk_string(it->second.front());
                it->second.erase(it->second.begin());
                if (it->second.empty()) lists.erase(it);  // Redis deletes empty lists
                ++key_versions[args[1]];
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
            } else if (iequals(args[0], "TYPE") && args.size() >= 2) {
              auto it = store.find(args[1]);
              if (it != store.end() && it->second.has_expiry &&
                  Clock::now() >= it->second.expires_at) {
                store.erase(it);
                it = store.end();
              }
              if (it != store.end()) {
                response = "+string\r\n";
              } else if (lists.find(args[1]) != lists.end()) {
                response = "+list\r\n";
              } else if (streams.find(args[1]) != streams.end()) {
                response = "+stream\r\n";
              } else {
                response = "+none\r\n";
              }
            } else if (iequals(args[0], "XADD") && args.size() >= 5 && (args.size() % 2) == 1) {
              const std::string &key = args[1];
              const std::string &id_str = args[2];

              bool auto_ms = false, auto_seq = false;
              unsigned long long ms = 0, seq = 0;
              bool parse_ok = true;

              if (id_str == "*") {
                auto_ms = true;
                auto_seq = true;
              } else {
                auto dash = id_str.find('-');
                if (dash == std::string::npos) {
                  parse_ok = false;
                } else {
                  std::string ms_part = id_str.substr(0, dash);
                  std::string seq_part = id_str.substr(dash + 1);
                  char *end = nullptr;
                  ms = std::strtoull(ms_part.c_str(), &end, 10);
                  if (*end != '\0' || ms_part.empty()) parse_ok = false;
                  if (parse_ok && seq_part == "*") {
                    auto_seq = true;
                  } else if (parse_ok) {
                    end = nullptr;
                    seq = std::strtoull(seq_part.c_str(), &end, 10);
                    if (*end != '\0' || seq_part.empty()) parse_ok = false;
                  }
                }
              }

              if (!parse_ok) {
                response = "-ERR Invalid stream ID specified\r\n";
              } else {
                auto stream_it = streams.find(key);
                bool stream_exists = stream_it != streams.end();
                unsigned long long last_ms = 0, last_seq = 0;
                if (stream_exists && !stream_it->second.empty()) {
                  const std::string &last_id = stream_it->second.back().id;
                  auto d = last_id.find('-');
                  last_ms = std::strtoull(last_id.substr(0, d).c_str(), nullptr, 10);
                  last_seq = std::strtoull(last_id.substr(d + 1).c_str(), nullptr, 10);
                }

                std::string final_id;
                bool id_valid = true;

                if (auto_ms) {
                  auto now = static_cast<unsigned long long>(
                      std::chrono::duration_cast<std::chrono::milliseconds>(
                          std::chrono::system_clock::now().time_since_epoch()).count());
                  if (now > last_ms) {
                    ms = now;
                    seq = 0;
                  } else {
                    ms = last_ms;
                    seq = last_seq + 1;
                  }
                  final_id = std::to_string(ms) + "-" + std::to_string(seq);
                } else if (auto_seq) {
                  if (ms > last_ms) {
                    seq = 0;
                    final_id = std::to_string(ms) + "-" + std::to_string(seq);
                  } else if (ms == last_ms) {
                    seq = last_seq + 1;
                    final_id = std::to_string(ms) + "-" + std::to_string(seq);
                  } else {
                    id_valid = false;
                    response = "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n";
                  }
                } else {
                  if (ms < last_ms || (ms == last_ms && seq <= last_seq)) {
                    id_valid = false;
                    if (ms == 0 && seq == 0) {
                      response = "-ERR The ID specified in XADD must be greater than 0-0\r\n";
                    } else {
                      response = "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n";
                    }
                  } else {
                    final_id = id_str;
                  }
                }

                if (id_valid) {
                  StreamEntry entry;
                  entry.id = final_id;
                  for (size_t a = 3; a < args.size(); a += 2) {
                    entry.fields.emplace_back(args[a], args[a + 1]);
                  }
                  streams[key].push_back(std::move(entry));
                  ++key_versions[key];
                  response = encode_bulk_string(final_id);
                  try_unblock_xreads(key, streams, blocked_xreads, blocked_fds);
                }
              }
            } else if (iequals(args[0], "XRANGE") && args.size() >= 4) {
              const std::string &key = args[1];
              const std::string &start_str = args[2];
              const std::string &end_str = args[3];

              // Parse an XRANGE bound "<ms>-<seq>" or "<ms>" (seq falls back to default_seq).
              auto parse_range_id = [](const std::string &s, unsigned long long &ms, unsigned long long &seq, unsigned long long default_seq) {
                auto dash = s.find('-');
                if (dash == std::string::npos) {
                  ms = std::strtoull(s.c_str(), nullptr, 10);
                  seq = default_seq;
                } else {
                  ms = std::strtoull(s.substr(0, dash).c_str(), nullptr, 10);
                  seq = std::strtoull(s.substr(dash + 1).c_str(), nullptr, 10);
                }
              };

              unsigned long long start_ms, start_seq, end_ms, end_seq;
              if (start_str == "-") {
                start_ms = 0;
                start_seq = 0;
              } else {
                parse_range_id(start_str, start_ms, start_seq, 0);
              }
              if (end_str == "+") {
                end_ms = ULLONG_MAX;
                end_seq = ULLONG_MAX;
              } else {
                parse_range_id(end_str, end_ms, end_seq, ULLONG_MAX);
              }

              std::string entries_resp;
              long entry_count = 0;
              auto sit = streams.find(key);
              if (sit != streams.end()) {
                for (const auto &entry : sit->second) {
                  auto d = entry.id.find('-');
                  unsigned long long e_ms = std::strtoull(entry.id.substr(0, d).c_str(), nullptr, 10);
                  unsigned long long e_seq = std::strtoull(entry.id.substr(d + 1).c_str(), nullptr, 10);

                  bool ge_start = (e_ms > start_ms) || (e_ms == start_ms && e_seq >= start_seq);
                  bool le_end = (e_ms < end_ms) || (e_ms == end_ms && e_seq <= end_seq);
                  if (ge_start && le_end) {
                    std::vector<std::string> fields_resp;
                    for (const auto &fv : entry.fields) {
                      fields_resp.push_back(fv.first);
                      fields_resp.push_back(fv.second);
                    }
                    entries_resp += "*2\r\n" + encode_bulk_string(entry.id) + encode_array(fields_resp);
                    ++entry_count;
                  }
                }
              }
              response = "*" + std::to_string(entry_count) + "\r\n" + entries_resp;
            } else if (iequals(args[0], "XREAD") && args.size() >= 4) {
              size_t idx = 1;
              bool has_block = false;
              long block_ms = 0;
              if (iequals(args[idx], "BLOCK") && idx + 1 < args.size()) {
                has_block = true;
                block_ms = std::strtol(args[idx + 1].c_str(), nullptr, 10);
                idx += 2;
              }

              if (idx + 1 < args.size() && iequals(args[idx], "STREAMS") &&
                  (args.size() - idx - 1) % 2 == 0) {
                size_t num_streams = (args.size() - idx - 1) / 2;

                std::vector<std::tuple<std::string, unsigned long long, unsigned long long>> queries;
                bool xread_error = false;

                for (size_t s = 0; s < num_streams; ++s) {
                  const std::string &key = args[idx + 1 + s];
                  const std::string &id_str = args[idx + 1 + num_streams + s];

                  unsigned long long start_ms, start_seq;
                  if (id_str == "$") {
                    // "$" means "the last ID currently in the stream" — resolved now,
                    // so only entries added after this command will be returned.
                    auto sit = streams.find(key);
                    if (sit != streams.end() && !sit->second.empty()) {
                      const std::string &last_id = sit->second.back().id;
                      auto d = last_id.find('-');
                      start_ms = std::strtoull(last_id.substr(0, d).c_str(), nullptr, 10);
                      start_seq = std::strtoull(last_id.substr(d + 1).c_str(), nullptr, 10);
                    } else {
                      start_ms = 0;
                      start_seq = 0;
                    }
                  } else {
                    auto dash = id_str.find('-');
                    if (dash == std::string::npos) {
                      response = "-ERR Invalid stream ID specified\r\n";
                      xread_error = true;
                      break;
                    }
                    start_ms = std::strtoull(id_str.substr(0, dash).c_str(), nullptr, 10);
                    start_seq = std::strtoull(id_str.substr(dash + 1).c_str(), nullptr, 10);
                  }
                  queries.emplace_back(key, start_ms, start_seq);
                }

                if (!xread_error) {
                  std::string streams_resp;
                  size_t active_streams = build_xread_streams(queries, streams, streams_resp);

                  if (active_streams > 0) {
                    response = "*" + std::to_string(active_streams) + "\r\n" + streams_resp;
                  } else if (has_block) {
                    Clock::time_point deadline = (block_ms > 0)
                        ? Clock::now() + std::chrono::milliseconds(block_ms)
                        : Clock::time_point::max();
                    blocked_xreads.push_back({fd, std::move(queries), deadline});
                    blocked_fds.insert(fd);
                  } else {
                    response = "*-1\r\n";
                  }
                }
              } else {
                response = "+PONG\r\n";
              }
            } else if (iequals(args[0], "INFO")) {
              std::string role = is_replica ? "slave" : "master";
              std::string info = "role:" + role + "\r\n"
                  "master_replid:" + MASTER_REPLID + "\r\n"
                  "master_repl_offset:0\r\n";
              response = encode_bulk_string(info);
            } else if (iequals(args[0], "REPLCONF")) {
              response = "+OK\r\n";
            } else if (iequals(args[0], "PSYNC")) {
              std::string rdb = hex_to_binary(EMPTY_RDB_HEX);
              response = "+FULLRESYNC " + MASTER_REPLID + " 0\r\n" +
                  "$" + std::to_string(rdb.size()) + "\r\n" + rdb;
              replica_fds.push_back(fd);
            } else if (iequals(args[0], "WAIT") && args.size() >= 3) {
              long needed = std::strtol(args[1].c_str(), nullptr, 10);
              long timeout_ms = std::strtol(args[2].c_str(), nullptr, 10);

              if (master_repl_offset == 0) {
                response = ":" + std::to_string(replica_fds.size()) + "\r\n";
              } else {
                std::string getack = encode_array({"REPLCONF", "GETACK", "*"});
                long long target_offset = master_repl_offset;
                for (int rfd : replica_fds) send_all(rfd, getack);
                master_repl_offset += getack.size();

                std::unordered_map<int, std::string> ack_buffers;
                std::unordered_set<int> acked;
                auto deadline = Clock::now() + std::chrono::milliseconds(timeout_ms);
                while ((long)acked.size() < needed) {
                  auto remaining_ms = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - Clock::now()).count();
                  if (remaining_ms < 0) remaining_ms = 0;
                  std::vector<pollfd> pfds;
                  for (int rfd : replica_fds) pfds.push_back({rfd, POLLIN, 0});
                  int pn = poll(pfds.data(), pfds.size(), (int)remaining_ms);
                  if (pn <= 0) break;
                  for (auto &pf : pfds) {
                    if (!(pf.revents & POLLIN)) continue;
                    char buf[1024];
                    ssize_t n = recv(pf.fd, buf, sizeof(buf), 0);
                    if (n <= 0) continue;
                    ack_buffers[pf.fd].append(buf, n);
                    std::vector<std::string> rargs;
                    size_t consumed;
                    while (parse_command(ack_buffers[pf.fd], rargs, consumed)) {
                      if (rargs.size() >= 3 && iequals(rargs[0], "REPLCONF") && iequals(rargs[1], "ACK")) {
                        long long off = std::strtoll(rargs[2].c_str(), nullptr, 10);
                        if (off >= target_offset) acked.insert(pf.fd);
                      }
                      ack_buffers[pf.fd].erase(0, consumed);
                    }
                  }
                  if (Clock::now() >= deadline) break;
                }
                response = ":" + std::to_string(acked.size()) + "\r\n";
              }
            } else if (iequals(args[0], "CONFIG") && args.size() >= 3 && iequals(args[1], "GET")) {
              if (iequals(args[2], "dir")) {
                response = encode_array({args[2], g_dir});
              } else if (iequals(args[2], "dbfilename")) {
                response = encode_array({args[2], g_dbfilename});
              } else if (iequals(args[2], "appendonly")) {
                response = encode_array({args[2], g_appendonly});
              } else if (iequals(args[2], "appenddirname")) {
                response = encode_array({args[2], g_appenddirname});
              } else if (iequals(args[2], "appendfilename")) {
                response = encode_array({args[2], g_appendfilename});
              } else if (iequals(args[2], "appendfsync")) {
                response = encode_array({args[2], g_appendfsync});
              } else {
                response = "*0\r\n";
              }
            } else if (iequals(args[0], "KEYS") && args.size() >= 2) {
              std::vector<std::string> keys;
              for (const auto &[key, entry] : store) {
                keys.push_back(key);
              }
              response = encode_array(keys);
            } else if (iequals(args[0], "PING")) {
              if (in_subscribed_mode) {
                response = "*2\r\n" + encode_bulk_string("pong") + encode_bulk_string("");
              } else {
                response = "+PONG\r\n";
              }
            } else if (iequals(args[0], "SUBSCRIBE") && args.size() >= 2) {
              subscriptions[fd].insert(args[1]);
              response = "*3\r\n" + encode_bulk_string("subscribe") + encode_bulk_string(args[1]) +
                  ":" + std::to_string(subscriptions[fd].size()) + "\r\n";
            } else if (iequals(args[0], "UNSUBSCRIBE") && args.size() >= 2) {
              subscriptions[fd].erase(args[1]);
              response = "*3\r\n" + encode_bulk_string("unsubscribe") + encode_bulk_string(args[1]) +
                  ":" + std::to_string(subscriptions[fd].size()) + "\r\n";
            } else if (iequals(args[0], "PUBLISH") && args.size() >= 3) {
              std::string message = "*3\r\n" + encode_bulk_string("message") +
                  encode_bulk_string(args[1]) + encode_bulk_string(args[2]);
              int count = 0;
              for (auto &[sub_fd, channels] : subscriptions) {
                if (channels.count(args[1])) {
                  send_all(sub_fd, message);
                  ++count;
                }
              }
              response = ":" + std::to_string(count) + "\r\n";
            } else if (iequals(args[0], "ZADD") && args.size() >= 4) {
              double score = std::strtod(args[2].c_str(), nullptr);
              int added = zset_add(sorted_sets[args[1]], score, args[3]);
              response = ":" + std::to_string(added) + "\r\n";
            } else if (iequals(args[0], "ZRANK") && args.size() >= 3) {
              auto sit = sorted_sets.find(args[1]);
              if (sit == sorted_sets.end()) {
                response = "$-1\r\n";
              } else {
                auto &zset = sit->second;
                auto it = std::find_if(zset.begin(), zset.end(),
                    [&](const auto &p) { return p.second == args[2]; });
                if (it == zset.end()) {
                  response = "$-1\r\n";
                } else {
                  response = ":" + std::to_string(it - zset.begin()) + "\r\n";
                }
              }
            } else if (iequals(args[0], "ZRANGE") && args.size() >= 4) {
              auto sit = sorted_sets.find(args[1]);
              if (sit == sorted_sets.end()) {
                response = "*0\r\n";
              } else {
                auto &zset = sit->second;
                long size = (long)zset.size();
                long start = std::strtol(args[2].c_str(), nullptr, 10);
                long stop = std::strtol(args[3].c_str(), nullptr, 10);
                if (start < 0) start = std::max(0L, size + start);
                if (stop < 0) stop = std::max(0L, size + stop);
                if (stop >= size) stop = size - 1;
                if (start >= size || start > stop) {
                  response = "*0\r\n";
                } else {
                  std::vector<std::string> members;
                  for (long idx = start; idx <= stop; ++idx) members.push_back(zset[idx].second);
                  response = encode_array(members);
                }
              }
            } else if (iequals(args[0], "ZCARD") && args.size() >= 2) {
              auto sit = sorted_sets.find(args[1]);
              long count = (sit == sorted_sets.end()) ? 0 : (long)sit->second.size();
              response = ":" + std::to_string(count) + "\r\n";
            } else if (iequals(args[0], "ZSCORE") && args.size() >= 3) {
              auto sit = sorted_sets.find(args[1]);
              if (sit == sorted_sets.end()) {
                response = "$-1\r\n";
              } else {
                auto &zset = sit->second;
                auto it = std::find_if(zset.begin(), zset.end(),
                    [&](const auto &p) { return p.second == args[2]; });
                response = (it == zset.end()) ? "$-1\r\n" : encode_bulk_string(format_score(it->first));
              }
            } else if (iequals(args[0], "ZREM") && args.size() >= 3) {
              auto sit = sorted_sets.find(args[1]);
              int removed = 0;
              if (sit != sorted_sets.end()) {
                auto &zset = sit->second;
                auto it = std::find_if(zset.begin(), zset.end(),
                    [&](const auto &p) { return p.second == args[2]; });
                if (it != zset.end()) {
                  zset.erase(it);
                  removed = 1;
                }
              }
              response = ":" + std::to_string(removed) + "\r\n";
            } else if (iequals(args[0], "GEOADD") && args.size() >= 5) {
              double longitude = std::strtod(args[2].c_str(), nullptr);
              double latitude = std::strtod(args[3].c_str(), nullptr);
              bool lon_invalid = longitude < GEO_LON_MIN || longitude > GEO_LON_MAX;
              bool lat_invalid = latitude < GEO_LAT_MIN || latitude > GEO_LAT_MAX;
              if (lon_invalid || lat_invalid) {
                std::string what = lon_invalid && lat_invalid ? "longitude,latitude"
                                    : lon_invalid              ? "longitude"
                                                                : "latitude";
                response = "-ERR invalid " + what + " argument\r\n";
              } else {
                uint64_t geo_score = geohash_encode(longitude, latitude);
                int added = zset_add(sorted_sets[args[1]], (double)geo_score, args[4]);
                response = ":" + std::to_string(added) + "\r\n";
              }
            } else if (iequals(args[0], "GEOPOS") && args.size() >= 2) {
              auto sit = sorted_sets.find(args[1]);
              response = "*" + std::to_string(args.size() - 2) + "\r\n";
              for (size_t i = 2; i < args.size(); ++i) {
                std::vector<std::pair<double, std::string>>::iterator it;
                if (sit != sorted_sets.end()) {
                  it = std::find_if(sit->second.begin(), sit->second.end(),
                      [&](const auto &p) { return p.second == args[i]; });
                }
                if (sit == sorted_sets.end() || it == sit->second.end()) {
                  response += "*-1\r\n";
                } else {
                  double longitude, latitude;
                  geohash_decode((uint64_t)it->first, longitude, latitude);
                  char lon_buf[64], lat_buf[64];
                  snprintf(lon_buf, sizeof(lon_buf), "%.17g", longitude);
                  snprintf(lat_buf, sizeof(lat_buf), "%.17g", latitude);
                  response += "*2\r\n" + encode_bulk_string(lon_buf) + encode_bulk_string(lat_buf);
                }
              }
            } else if (iequals(args[0], "GEODIST") && args.size() >= 4) {
              auto sit = sorted_sets.find(args[1]);
              if (sit == sorted_sets.end()) {
                response = "$-1\r\n";
              } else {
              auto it1 = std::find_if(sit->second.begin(), sit->second.end(),
                  [&](const auto &p) { return p.second == args[2]; });
              auto it2 = std::find_if(sit->second.begin(), sit->second.end(),
                  [&](const auto &p) { return p.second == args[3]; });
              if (it1 == sit->second.end() || it2 == sit->second.end()) {
                response = "$-1\r\n";
              } else {
                double lon1, lat1, lon2, lat2;
                geohash_decode((uint64_t)it1->first, lon1, lat1);
                geohash_decode((uint64_t)it2->first, lon2, lat2);
                double dist = haversine_distance(lon1, lat1, lon2, lat2);
                char buf[64];
                snprintf(buf, sizeof(buf), "%.4f", dist);
                response = encode_bulk_string(buf);
              }
              }
            } else if (iequals(args[0], "GEOSEARCH") && args.size() >= 8 &&
                       iequals(args[2], "FROMLONLAT") && iequals(args[5], "BYRADIUS")) {
              double center_lon = std::strtod(args[3].c_str(), nullptr);
              double center_lat = std::strtod(args[4].c_str(), nullptr);
              double radius_m = std::strtod(args[6].c_str(), nullptr) * geo_unit_to_meters(args[7]);
              std::vector<std::string> members;
              auto sit = sorted_sets.find(args[1]);
              if (sit != sorted_sets.end()) {
                for (auto &[score, member] : sit->second) {
                  double longitude, latitude;
                  geohash_decode((uint64_t)score, longitude, latitude);
                  if (haversine_distance(center_lon, center_lat, longitude, latitude) <= radius_m) {
                    members.push_back(member);
                  }
                }
              }
              response = encode_array(members);
            } else {
              response = "+PONG\r\n";
            }
  return response;
}

int main(int argc, char **argv) {
  std::cout << std::unitbuf;
  std::cerr << std::unitbuf;
  //flushes the stream after every insertion, without this I might see no output before a crash.

  // Default "dir" to the current working directory, as real Redis does.
  char cwd_buf[PATH_MAX];
  if (getcwd(cwd_buf, sizeof(cwd_buf))) {
    g_dir = cwd_buf;
  }

  int port = 6379;
  bool is_replica = false;
  std::string master_host;
  int master_port = 0;
  for (int i = 1; i < argc; ++i) {
    if (std::strcmp(argv[i], "--port") == 0 && i + 1 < argc) {
      port = std::atoi(argv[++i]);
    } else if (std::strcmp(argv[i], "--replicaof") == 0 && i + 1 < argc) {
      is_replica = true;
      std::string target = argv[++i];
      size_t space = target.find(' ');
      if (space != std::string::npos) {
        master_host = target.substr(0, space);
        master_port = std::atoi(target.substr(space + 1).c_str());
      }
    } else if (std::strcmp(argv[i], "--dir") == 0 && i + 1 < argc) {
      g_dir = argv[++i];
    } else if (std::strcmp(argv[i], "--dbfilename") == 0 && i + 1 < argc) {
      g_dbfilename = argv[++i];
    } else if (std::strcmp(argv[i], "--appendonly") == 0 && i + 1 < argc) {
      g_appendonly = argv[++i];
    } else if (std::strcmp(argv[i], "--appenddirname") == 0 && i + 1 < argc) {
      g_appenddirname = argv[++i];
    } else if (std::strcmp(argv[i], "--appendfilename") == 0 && i + 1 < argc) {
      g_appendfilename = argv[++i];
    } else if (std::strcmp(argv[i], "--appendfsync") == 0 && i + 1 < argc) {
      g_appendfsync = argv[++i];
    }
  }

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
  server_addr.sin_port = htons(port);

  if (bind(server_fd, (struct sockaddr *) &server_addr, sizeof(server_addr)) != 0) {
    std::cerr << "Failed to bind to port " << port << "\n";
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

  // Replication state.
  int master_fd = -1;              // fd connected to our master, if we're a replica.
  std::string master_buffer;       // buffered bytes received from the master, not yet parsed.
  long long replica_offset = 0;    // bytes of commands processed from the master.
  std::vector<int> replica_fds;    // fds of connected replicas (as a master).
  long long master_repl_offset = 0;  // bytes of commands propagated to replicas.

  if (is_replica) {
    master_fd = connect_to_master(master_host, master_port, port);
    if (master_fd >= 0) {
      fds.push_back({master_fd, POLLIN, 0});
    }
  }

  // The key-value store, shared across all clients.
  std::unordered_map<std::string, Entry> store;
  load_rdb_file(g_dir + "/" + g_dbfilename, store);
  // List store, keyed separately from the string store.
  std::unordered_map<std::string, std::vector<std::string>> lists;
  // Stream store, keyed separately from strings and lists.
  std::unordered_map<std::string, std::vector<StreamEntry>> streams;
  // Sorted-set store: each vector is kept sorted by (score, member).
  std::unordered_map<std::string, std::vector<std::pair<double, std::string>>> sorted_sets;
  std::vector<BlockedEntry> blocked_clients;
  std::vector<BlockedXReadEntry> blocked_xreads;
  std::unordered_set<int> blocked_fds;
  // Connections that have issued MULTI and are queuing commands until EXEC/DISCARD.
  std::unordered_set<int> multi_clients;
  std::unordered_map<int, std::vector<std::vector<std::string>>> queued_commands;
  // Bumped every time a key is written to, so WATCH can detect modifications.
  std::unordered_map<std::string, unsigned long long> key_versions;
  // Per-connection set of watched keys, with the key version observed at WATCH time.
  std::unordered_map<int, std::unordered_map<std::string, unsigned long long>> watched_keys;
  // Per-connection set of subscribed Pub/Sub channels.
  std::unordered_map<int, std::unordered_set<std::string>> subscriptions;

  // AOF persistence setup: create the append-only directory/file/manifest if they
  // don't exist yet, then replay any previously-logged commands to restore state.
  if (g_appendonly == "yes") {
    std::string aof_dir = g_dir + "/" + g_appenddirname;
    mkdir(aof_dir.c_str(), 0755);

    std::string manifest_path = aof_dir + "/" + g_appendfilename + ".manifest";
    std::string incr_filename = g_appendfilename + ".1.incr.aof";

    std::ifstream manifest_in(manifest_path);
    if (manifest_in) {
      std::string line;
      while (std::getline(manifest_in, line)) {
        std::istringstream iss(line);
        std::vector<std::string> tokens;
        std::string tok;
        while (iss >> tok) tokens.push_back(tok);
        if (tokens.size() >= 6 && tokens[0] == "file" && tokens[4] == "type" && tokens[5] == "i") {
          incr_filename = tokens[1];
        }
      }
    } else {
      std::ofstream manifest_out(manifest_path);
      manifest_out << "file " << incr_filename << " seq 1 type i\n";
    }

    g_aof_path = aof_dir + "/" + incr_filename;
    { std::ofstream touch(g_aof_path, std::ios::app); } // ensure the AOF file exists

    std::ifstream aof_in(g_aof_path, std::ios::binary);
    std::string aof_data((std::istreambuf_iterator<char>(aof_in)), std::istreambuf_iterator<char>());
    std::vector<std::string> rargs;
    size_t consumed;
    while (parse_command(aof_data, rargs, consumed)) {
      if (!rargs.empty()) {
        execute_command(rargs, -1, store, lists, streams, blocked_clients, blocked_xreads,
                         blocked_fds, key_versions, replica_fds, master_repl_offset, is_replica, subscriptions, sorted_sets);
      }
      aof_data.erase(0, consumed);
    }
  }

  while (true) {
    // Compute poll timeout from nearest blocked-client deadline (if any).
    int poll_timeout = -1;
    if (!blocked_clients.empty() || !blocked_xreads.empty()) {
      auto now = Clock::now();
      for (const auto &bc : blocked_clients) {
        if (bc.deadline == Clock::time_point::max()) continue;
        auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
            bc.deadline - now).count();
        if (remaining < 0) remaining = 0;
        if (poll_timeout == -1 || remaining < poll_timeout)
          poll_timeout = static_cast<int>(remaining);
      }
      for (const auto &bx : blocked_xreads) {
        if (bx.deadline == Clock::time_point::max()) continue;
        auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
            bx.deadline - now).count();
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
          std::string resp = "*-1\r\n";
          send(it->fd, resp.c_str(), resp.size(), 0);
          blocked_fds.erase(it->fd);
          it = blocked_clients.erase(it);
        } else {
          ++it;
        }
      }
    }

    // Expire blocked XREADs whose deadline has passed.
    if (!blocked_xreads.empty()) {
      auto now = Clock::now();
      for (auto it = blocked_xreads.begin(); it != blocked_xreads.end(); ) {
        if (it->deadline != Clock::time_point::max() && now >= it->deadline) {
          std::string resp = "*-1\r\n";
          send(it->fd, resp.c_str(), resp.size(), 0);
          blocked_fds.erase(it->fd);
          it = blocked_xreads.erase(it);
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
      } else if (fds[i].fd == master_fd && (fds[i].revents & POLLIN)) {
        char buffer[4096];
        int bytes_received = recv(fds[i].fd, buffer, sizeof(buffer), 0);
        if (bytes_received > 0) {
          master_buffer.append(buffer, bytes_received);
          std::vector<std::string> margs;
          size_t consumed;
          while (parse_command(master_buffer, margs, consumed)) {
            if (!margs.empty()) {
              if (margs.size() >= 2 && iequals(margs[0], "REPLCONF") && iequals(margs[1], "GETACK")) {
                std::string ack = encode_array({"REPLCONF", "ACK", std::to_string(replica_offset)});
                send_all(fds[i].fd, ack);
              } else {
                execute_command(margs, fds[i].fd, store, lists, streams, blocked_clients, blocked_xreads, blocked_fds, key_versions, replica_fds, master_repl_offset, is_replica, subscriptions, sorted_sets);
              }
              replica_offset += consumed;
            }
            master_buffer.erase(0, consumed);
          }
        } else if (bytes_received == 0) {
          drop = true;
        } else if (errno != EAGAIN && errno != EWOULDBLOCK) {
          drop = true;
        }
      } else if (std::find(replica_fds.begin(), replica_fds.end(), fds[i].fd) != replica_fds.end() && (fds[i].revents & POLLIN)) {
        // Replica connections only send unsolicited bytes outside of WAIT's ack collection;
        // drain and discard so they don't get mistaken for a regular client command.
        char buffer[1024];
        int bytes_received = recv(fds[i].fd, buffer, sizeof(buffer), 0);
        if (bytes_received == 0) {
          drop = true;
        } else if (bytes_received < 0 && errno != EAGAIN && errno != EWOULDBLOCK) {
          drop = true;
        }
      } else if (fds[i].revents & POLLIN) {
        char buffer[1024];
        int bytes_received = recv(fds[i].fd, buffer, sizeof(buffer), 0);
        if (bytes_received > 0) {
          std::string input(buffer, bytes_received);
          std::vector<std::string> args;
          std::string response;
          size_t consumed;
          if (parse_command(input, args, consumed) && !args.empty()) {
            if (iequals(args[0], "MULTI")) {
              if (!multi_clients.insert(fds[i].fd).second) {
                response = "-ERR MULTI calls can not be nested\r\n";
              } else {
                queued_commands[fds[i].fd].clear();
                response = "+OK\r\n";
              }
            } else if (iequals(args[0], "EXEC")) {
              if (!multi_clients.count(fds[i].fd)) {
                response = "-ERR EXEC without MULTI\r\n";
              } else {
                auto cmds = std::move(queued_commands[fds[i].fd]);
                multi_clients.erase(fds[i].fd);
                queued_commands.erase(fds[i].fd);

                bool conflict = false;
                auto wit = watched_keys.find(fds[i].fd);
                if (wit != watched_keys.end()) {
                  for (const auto &[key, ver] : wit->second) {
                    if (key_versions[key] != ver) { conflict = true; break; }
                  }
                }
                watched_keys.erase(fds[i].fd);

                if (conflict) {
                  response = "*-1\r\n";
                } else {
                  std::string results;
                  for (auto &cmd : cmds) {
                    results += execute_command(cmd, fds[i].fd, store, lists, streams, blocked_clients, blocked_xreads, blocked_fds, key_versions, replica_fds, master_repl_offset, is_replica, subscriptions, sorted_sets);
                  }
                  response = "*" + std::to_string(cmds.size()) + "\r\n" + results;
                }
              }
            } else if (iequals(args[0], "DISCARD")) {
              if (!multi_clients.count(fds[i].fd)) {
                response = "-ERR DISCARD without MULTI\r\n";
              } else {
                multi_clients.erase(fds[i].fd);
                queued_commands.erase(fds[i].fd);
                watched_keys.erase(fds[i].fd);
                response = "+OK\r\n";
              }
            } else if (iequals(args[0], "WATCH") && args.size() >= 2) {
              if (multi_clients.count(fds[i].fd)) {
                response = "-ERR WATCH inside MULTI is not allowed\r\n";
              } else {
                for (size_t a = 1; a < args.size(); ++a) {
                  watched_keys[fds[i].fd][args[a]] = key_versions[args[a]];
                }
                response = "+OK\r\n";
              }
            } else if (iequals(args[0], "UNWATCH")) {
              watched_keys.erase(fds[i].fd);
              response = "+OK\r\n";
            } else if (multi_clients.count(fds[i].fd)) {
              queued_commands[fds[i].fd].push_back(args);
              response = "+QUEUED\r\n";
            } else {
              response = execute_command(args, fds[i].fd, store, lists, streams, blocked_clients, blocked_xreads, blocked_fds, key_versions, replica_fds, master_repl_offset, is_replica, subscriptions, sorted_sets);
              if (is_write_command(args[0])) {
                append_to_aof(args);
                if (!replica_fds.empty()) {
                  std::string propagated = encode_array(args);
                  for (int rfd : replica_fds) send_all(rfd, propagated);
                  master_repl_offset += propagated.size();
                }
              }
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
        for (auto bit = blocked_xreads.begin(); bit != blocked_xreads.end(); ) {
          if (bit->fd == fds[i].fd) bit = blocked_xreads.erase(bit);
          else ++bit;
        }
        multi_clients.erase(fds[i].fd);
        queued_commands.erase(fds[i].fd);
        watched_keys.erase(fds[i].fd);
        if (fds[i].fd == master_fd) master_fd = -1;
        replica_fds.erase(std::remove(replica_fds.begin(), replica_fds.end(), fds[i].fd), replica_fds.end());
        close(fds[i].fd);
        fds.erase(fds.begin() + i);
      }
      else {++i;}
    }
  }

  close(server_fd);
  return 0;
}
