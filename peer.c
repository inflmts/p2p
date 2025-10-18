#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <poll.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#define P2P_CHOKE     0
#define P2P_UNCHOKE   1
#define P2P_INT       2
#define P2P_NOINT     3
#define P2P_HAVE      4
#define P2P_BITFIELD  5
#define P2P_REQUEST   6
#define P2P_PIECE     7

static const char magic[28] = "P2PFILESHARINGPROJ\0\0\0\0\0\0\0\0\0\0";
static const char *config_delim = "\r\n ";
static const int uno = 1;
static const int cero = 0;

static unsigned int num_preferred_neighbors = 3;
static unsigned int unchoking_interval = 5;
static unsigned int optimistic_unchoking_interval = 10;
static char the_file_name[256] = "thefile";
static unsigned int the_file_size;
static unsigned int piece_size = 16384;
static unsigned int num_pieces;
static unsigned int bitfield_size;

static unsigned int self_id;
static int self_sock = -1;
static unsigned char *self_have, *self_pend;
static unsigned int self_num_pieces;
static int logfd = -1;
static char *the_file = MAP_FAILED;

static void __attribute__((format(printf, 1, 2)))
msg(const char *format, ...)
{
  va_list ap;
  char buf[2048], *s;
  struct timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);
  struct tm *tm = localtime(&ts.tv_sec);
  s = buf + sprintf(buf, "%04d-%02d-%02d %02d:%02d:%02d.%06d: Peer %u ",
      tm->tm_year + 1900, tm->tm_mon + 1, tm->tm_mday,
      tm->tm_hour, tm->tm_min, tm->tm_sec, (int)ts.tv_nsec / 1000, self_id);
  va_start(ap, format);
  vsnprintf(s, buf + sizeof(buf) - s, format, ap);
  va_end(ap);
  for (; *s; ++s)
    if (*s < 0x20 || *s == 0x7f)
      *s = '?';
  *(s++) = '\n';
  write(2, buf, s - buf);
  write(logfd, buf, s - buf);
}

#define err(...) msg(__VA_ARGS__)
#define err_sys_(format, ...) err(format ": %s", __VA_ARGS__)
#define err_sys(...) err_sys_(__VA_ARGS__, strerror(errno))
#define die(...) err(__VA_ARGS__), exit(1)
#define die_sys(...) err_sys(__VA_ARGS__), exit(1)

static int
parse_pint(const char *s, unsigned int *dest)
{
  unsigned int value = 0;
  while (*s >= '0' && *s <= '9')
    value = value * 10 + (*(s++) - '0');
  if (*s || !value)
    return -1;
  *dest = value;
  return 0;
}

static uint32_t
read_uint32(const void *buf)
{
  uint32_t value;
  memcpy(&value, buf, sizeof(value));
  return htonl(value);
}

#define CONN_WINT   4
#define CONN_RFLOW  8
#define CONN_WPREF  32
#define CONN_WOPT   64
#define CONN_SHUT   128
#define CONN_REQ    256
#define CONN_WFLOW  (CONN_WPREF | CONN_WOPT)

struct conn
{
  struct conn *next;
  unsigned char *have;
  char *wbuf;
  char *rbuf;
  int sock;
  int id;
  int flags;
  unsigned int wcap;
  unsigned int wsize;
  unsigned int rcap;
  unsigned int rsize;
  unsigned int rwant;
  unsigned int rrate;
  unsigned int num_pieces;
  unsigned int num_want;
  unsigned int req;
  void (*on_read)(struct conn *c, char *buf);
};

static struct conn *conn_head;
static int conn_count;
static struct conn **board;
static struct conn *conn_wopt;

static void
conn_write(struct conn *c, const void *buf, unsigned int size)
{
  c->wsize += size;
  if (c->wcap < c->wsize) {
    do c->wcap *= 2;
    while (c->wcap < c->wsize);
    c->wbuf = realloc(c->wbuf, c->wcap);
  }
  memcpy(c->wbuf + c->wsize - size, buf, size);
}

static void
conn_write_uint32(struct conn *c, uint32_t value)
{
  value = htonl(value);
  conn_write(c, &value, 4);
}

static void
conn_write_header(struct conn *c, uint32_t len, unsigned char type)
{
  conn_write_uint32(c, len + 1);
  conn_write(c, &type, 1);
}

static void
conn_request(struct conn *c)
{
  if (!(c->flags & CONN_RFLOW) || !c->num_want || c->flags & CONN_REQ)
    return;
  unsigned int index = (unsigned int)mrand48() % num_pieces;
  unsigned char mask;
  while (!(c->have[index >> 3] & ~self_pend[index >> 3] & (mask = 128 >> (index & 7))))
    index = (index + 1) % num_pieces;
  self_pend[index >> 3] |= mask;
  conn_write_header(c, 4, P2P_REQUEST);
  conn_write_uint32(c, index);
  c->flags |= CONN_REQ;
  c->req = index;
  for (c = conn_head; c; c = c->next)
    if ((c->have[index >> 3] & mask) && !--c->num_want)
      conn_write_header(c, 0, P2P_NOINT);
}

static void
conn_read_have(struct conn *c, char *buf)
{
  unsigned int index = read_uint32(buf);
  if (index >= num_pieces) {
    die("received invalid 'have' for %u (max is %u) from %u.",
        index, num_pieces - 1, c->id);
  }
  unsigned char mask = 128 >> (index & 7);
  if (c->have[index >> 3] & mask) {
    die("received duplicate 'have' from %u for piece %u.", c->id, index);
  }
  msg("received the 'have' message from %u for the piece %u.", c->id, index);
  c->have[index >> 3] |= mask;
  ++c->num_pieces;
  if (!(self_pend[index >> 3] & mask) && !c->num_want++) {
    conn_write_header(c, 0, P2P_INT);
    conn_request(c);
  }
  if (c->num_pieces == num_pieces && self_num_pieces == num_pieces) {
    c->flags |= CONN_SHUT;
  }
}

static void
conn_read_bitfield(struct conn *c, char *buf)
{
  /* TODO */
  memcpy(c->have, buf, bitfield_size);
  c->num_pieces = 0;
  c->num_want = 0;
  for (unsigned int index = 0; index < num_pieces; ++index) {
    unsigned char mask = 128 >> (index & 7);
    if (!(c->have[index >> 3] & mask))
      continue;
    ++c->num_pieces;
    if (!(self_pend[index >> 3] & mask))
      ++c->num_want;
  }
  if (c->num_want) {
    conn_write_header(c, 0, P2P_INT);
    conn_request(c);
  } else {
    conn_write_header(c, 0, P2P_NOINT);
  }
  if (c->num_pieces == num_pieces && self_num_pieces == num_pieces)
    c->flags |= CONN_SHUT;
}

static void
conn_read_request(struct conn *c, char *buf)
{
  uint32_t index = read_uint32(buf);
  if (!(c->flags & CONN_WFLOW)) {
    return; /* choked */
  }
  if (index >= num_pieces) {
    die("received invalid request for %u from %u (max is %u)",
        index, c->id, num_pieces - 1);
  }
  unsigned int offset = index * piece_size;
  unsigned int size = index == num_pieces - 1 ? the_file_size - offset : piece_size;
  conn_write_header(c, 4 + size, P2P_PIECE);
  conn_write_uint32(c, index);
  /* read piece from file */
  conn_write(c, the_file + offset, size);
}

static void
conn_read_piece(struct conn *c, char *buf)
{
  uint32_t index = read_uint32(buf);
  if (index >= num_pieces) {
    die("received invalid piece %u from %u (max is %u)",
        index, c->id, num_pieces - 1);
  }
  if (!(c->flags & CONN_REQ) || c->req != index) {
    return; /* discard */
  }
  unsigned int offset = index * piece_size;
  unsigned int size = index == num_pieces - 1 ? the_file_size - offset : piece_size;
  /* write piece to file */
  memcpy(the_file + offset, buf + 4, size);
  c->flags &= ~CONN_REQ;
  c->rrate += size;
  self_have[index >> 3] |= 128 >> (index & 7);
  ++self_num_pieces;
  msg("has downloaded the piece %u from %u. "
      "Now the number of pieces it has is %u.",
      index, c->id, self_num_pieces);
  if (self_num_pieces == num_pieces) {
    msg("has downloaded the complete file.");
  }
  conn_request(c);
  for (c = conn_head; c; c = c->next) {
    conn_write_header(c, 4, P2P_HAVE);
    conn_write_uint32(c, index);
    if (c->num_pieces == num_pieces && self_num_pieces == num_pieces)
      c->flags |= CONN_SHUT;
  }
}

static void
conn_read_message(struct conn *c, char *buf)
{
  uint32_t len = read_uint32(buf);
  switch (buf[4]) {
    case P2P_CHOKE:
      if (len != 1) {
        die("received 'choke' length %u, expected 1", len);
      }
      msg("is choked by %u.", c->id);
      c->flags &= ~CONN_RFLOW;
      if (c->flags & CONN_REQ) {
        c->flags &= ~CONN_REQ;
        unsigned char mask = 128 >> (c->req & 7);
        self_pend[c->req >> 3] &= ~mask;
        for (c = conn_head; c; c = c->next) {
          if ((c->have[c->req >> 3] & mask) && !c->num_want++) {
            conn_write_header(c, 0, P2P_INT);
            conn_request(c);
            break;
          }
        }
      }
      break;
    case P2P_UNCHOKE:
      if (len != 1) {
        die("received 'unchoke' length %u, expected 1", len);
      }
      msg("is unchoked by %u.", c->id);
      c->flags |= CONN_RFLOW;
      conn_request(c);
      break;
    case P2P_INT:
      if (len != 1) {
        die("received 'interested' length %u, expected 1", len);
      }
      msg("received the 'interested' message from %u.", c->id);
      c->flags |= CONN_WINT;
      break;
    case P2P_NOINT:
      if (len != 1) {
        die("received 'not interested' length %u, expected 1", len);
      }
      msg("received the 'not interested' message from %u.", c->id);
      c->flags &= ~CONN_WINT;
      break;
    case P2P_HAVE:
      if (len != 5) {
        die("received 'have' length %u, expected 5", len);
      }
      c->rwant = 4;
      c->on_read = conn_read_have;
      break;
    case P2P_BITFIELD:
      if (len != 1 + bitfield_size) {
        die("received 'bitfield' length %u, expected %u", len, 1 + bitfield_size);
      }
      c->rwant = bitfield_size;
      c->on_read = conn_read_bitfield;
      break;
    case P2P_REQUEST:
      if (len != 5) {
        die("received 'request' length %u, expected 5", len);
      }
      c->rwant = 4;
      c->on_read = conn_read_request;
      break;
    case P2P_PIECE:
      if (len > 5 + piece_size) {
        die("received 'piece' length %u, max %u", len, 5 + piece_size);
      }
      c->rwant = len - 1;
      c->on_read = conn_read_piece;
      break;
    default:
      die("received invalid message type %u", (unsigned int)buf[4]);
  }
}

static void
conn_read_handshake(struct conn *c, char *buf)
{
  if (memcmp(magic, buf, 28) || read_uint32(buf + 28) != c->id) {
    die("received invalid handshake from %u", c->id);
  }
  conn_write_header(c, bitfield_size, P2P_BITFIELD);
  conn_write(c, self_have, bitfield_size);
}

static void
conn_handle_write(struct conn *c)
{
  ssize_t len = send(c->sock, c->wbuf, c->wsize, 0);
  if (len < 0)
    die_sys("cannot send to %u", c->id);
  c->wsize -= len;
  if (c->wsize)
    memmove(c->wbuf, c->wbuf + len, c->wsize);
}

static void
conn_handle_read(struct conn *c)
{
  if (c->rcap < c->rwant) {
    c->rbuf = realloc(c->rbuf, (c->rcap = c->rwant));
  }
  ssize_t len = recv(c->sock, c->rbuf + c->rsize, c->rcap - c->rsize, 0);
  if (len < 0)
    die_sys("cannot recv from %u", c->id);
  if (!len)
    die("was disconnected by %u", c->id);
  c->rsize += len;
  char *next = c->rbuf, *cur;
  while (c->rsize >= c->rwant) {
    void (*on_read)(struct conn *c, char *buf) = c->on_read;
    cur = next;
    next += c->rwant;
    c->rsize -= c->rwant;
    c->on_read = conn_read_message;
    on_read(c, cur);
    if (c->on_read == conn_read_message) {
      c->rwant = 5;
    }
  }
  if (c->rsize) {
    memmove(c->rbuf, next, c->rsize);
  }
}

static void
conn_add(int sock, unsigned int id)
{
  struct conn *c = calloc(1, sizeof(struct conn));
  c->have = calloc(1, bitfield_size);
  c->wbuf = malloc(32);
  c->rbuf = malloc(32);
  c->sock = sock;
  c->id = id;
  c->wcap = 32;
  c->rcap = 32;
  c->rwant = 32;
  c->on_read = conn_read_handshake;
  c->next = conn_head;
  conn_head = c;
  ++conn_count;
  /* send handshake */
  conn_write(c, magic, sizeof(magic));
  conn_write_uint32(c, self_id);
  fcntl(sock, F_SETFL, O_NONBLOCK);
}

static void
conn_bind(uint16_t port, int has_file)
{
  struct sockaddr_in6 addr = {
    .sin6_family = AF_INET6,
    .sin6_port = htons(port),
    .sin6_addr = IN6ADDR_ANY_INIT
  };
  self_sock = socket(AF_INET6, SOCK_STREAM, 0);
  if (self_sock == -1)
    die_sys("cannot create server socket");
  setsockopt(self_sock, IPPROTO_IPV6, IPV6_V6ONLY, &cero, sizeof(cero));
  setsockopt(self_sock, SOL_SOCKET, SO_REUSEADDR, &uno, sizeof(uno));
  if (bind(self_sock, (const struct sockaddr *)&addr, sizeof(addr)))
    die_sys("cannot bind to port %u", (unsigned int)port);
  if (listen(self_sock, 16))
    die_sys("cannot listen");

  char filename[1024];
  struct stat st;

  /* open the file */
  snprintf(filename, sizeof(filename), "peer_%u/%s", self_id, the_file_name);
  int the_fd = open(filename, has_file ? O_RDONLY : (O_RDWR | O_CREAT), 0666);
  if (the_fd == -1) {
    die_sys("failed to open '%s'", filename);
  }
  if (has_file) {
    if (fstat(the_fd, &st)) {
      die_sys("failed to stat '%s'", filename);
    }
    if (st.st_size != the_file_size) {
      die("expected '%s' to be %u bytes, got %u",
          filename, the_file_size, (unsigned int)st.st_size);
    }
  } else if (ftruncate(the_fd, the_file_size)) {
    die_sys("could not truncate '%s' to %u bytes", filename, the_file_size);
  }
  int prot = has_file ? PROT_READ : PROT_READ | PROT_WRITE;
  the_file = mmap(NULL, the_file_size, prot, MAP_SHARED, the_fd, 0);
  if (the_file == MAP_FAILED)
    die_sys("failed to mmap '%s'", filename);
  close(the_fd);

  self_have = calloc(2, bitfield_size);
  self_pend = self_have + bitfield_size;
  if (has_file) {
    memset(self_have, -1, bitfield_size * 2);
    if (num_pieces & 7)
      self_have[bitfield_size - 1] = ~(0xff >> (num_pieces & 7));
    self_num_pieces = num_pieces;
  }
}

static void
conn_connect(const char *host, uint16_t port, unsigned int id)
{
  struct addrinfo *ai, *ai_head;
  struct addrinfo hints = { .ai_family = AF_UNSPEC, .ai_socktype = SOCK_STREAM };
  int sock, res;
  res = getaddrinfo(host, NULL, &hints, &ai_head);
  if (res) {
    die("failed to resolve '%s': %s", host, gai_strerror(res));
  }
  for (ai = ai_head; ai; ai = ai->ai_next) {
    if (ai->ai_family == AF_INET) {
      ((struct sockaddr_in *)ai->ai_addr)->sin_port = htons(port);
    } else { /* AF_INET6 */
      ((struct sockaddr_in6 *)ai->ai_addr)->sin6_port = htons(port);
    }
    sock = socket(ai->ai_family, SOCK_STREAM, 0);
    if (sock == -1)
      die_sys("cannot create socket");
    if (!connect(sock, ai->ai_addr, ai->ai_addrlen))
      goto success;
    close(sock);
  }
  die_sys("failed to connect to %s port %u", host, (unsigned int)port);
success:
  freeaddrinfo(ai_head);
  msg("makes a connection to Peer %u.", id);
  conn_add(sock, id);
}

static void
conn_accept(unsigned int id)
{
  int sock = accept(self_sock, NULL, NULL);
  if (sock == -1)
    die_sys("cannot accept");
  msg("is connected from Peer %u.", id);
  conn_add(sock, id);
}

static void
conn_init(const char *filename)
{
  char line[1024];
  FILE *f = fopen(filename, "r");
  if (!f)
    die_sys("could not open '%s'", filename);

  while (fgets(line, sizeof(line), f)) {
    char *fid, *host, *fprt, *fhas;
    unsigned int id, port;
    fid = strtok(line, config_delim);
    if (!fid || fid[0] == '#') {
      continue; /* empty line or comment */
    }
    if (!(host = strtok(NULL, config_delim)) ||
        !(fprt = strtok(NULL, config_delim)) ||
        !(fhas = strtok(NULL, config_delim)) ||
        parse_pint(fid, &id) || !id ||
        parse_pint(fprt, &port) || !port || port > 0xffff) {
      die("could not parse '%s'", filename);
    }
    if (self_sock != -1) {
      conn_accept(id);
    } else if (id == self_id) {
      conn_bind(port, !strcmp(fhas, "1"));
    } else {
      conn_connect(host, port, id);
    }
  }
  fclose(f);
  if (self_sock == -1)
    die("could not find self configuration");
  close(self_sock);
  board = calloc(conn_count, sizeof(*board));
}

static int
conn_ratecmp(const void *a, const void *b)
{
  return (int)(*(struct conn **)b)->rrate - (*(struct conn **)a)->rrate;
}

static void
reselect_preferred_neighbors(void)
{
  struct conn *c, **tail = board + conn_count;
  int count = 0;
  for (c = conn_head; c; c = c->next) {
    if (c->flags & CONN_WINT)
      board[count++] = c;
    else
      *(--tail) = c;
  }

  if (count) {
    /* https://en.wikipedia.org/wiki/Fisher-Yates_shuffle */
    for (int i = count - 1; i > 0; --i) {
      int j = (unsigned int)mrand48() % (i + 1);
      if (i != j) {
        c = board[i];
        board[i] = board[j];
        board[j] = c;
      }
    }
    if (self_num_pieces < num_pieces) {
      qsort(board, count, sizeof(*board), conn_ratecmp);
    }
  }

  char buf[2048];
  int len = 0;
  strcpy(buf, "(none)");
  for (int i = 0; i < conn_count; ++i) {
    c = board[i];
    if (c->flags & CONN_SHUT)
      continue;
    c->rrate = 0;
    if (i < num_preferred_neighbors) {
      if (!(c->flags & CONN_WFLOW))
        conn_write_header(c, 0, P2P_UNCHOKE);
      c->flags |= CONN_WPREF;
      if (len < sizeof(buf))
        len += snprintf(buf + len, sizeof(buf) - len, i ? ", %u" : "%u", board[i]->id);
    } else {
      if ((c->flags & CONN_WFLOW) == CONN_WPREF)
        conn_write_header(c, 0, P2P_CHOKE);
      c->flags &= ~CONN_WPREF;
    }
  }
  msg("has the preferred neighbors %s.", buf);
}

static void
optimistic_unchoke_neighbor(void)
{
  struct conn *c;
  int count = 0;
  for (c = conn_head; c; c = c->next)
    if (c->flags & CONN_WINT && !(c->flags & CONN_WFLOW))
      board[count++] = c;
  if (conn_wopt) {
    if ((conn_wopt->flags & CONN_WFLOW) == CONN_WOPT)
      conn_write_header(conn_wopt, 0, P2P_CHOKE);
    conn_wopt->flags &= ~CONN_WOPT;
    conn_wopt = NULL;
  }
  if (count) {
    conn_wopt = c = board[(unsigned int)mrand48() % count];
    conn_write_header(c, 0, P2P_UNCHOKE);
    c->flags |= CONN_WOPT;
    msg("has the optimistically unchoked neighbor %u.", c->id);
  }
}

static void
config_load(const char *filename)
{
  FILE *f = fopen(filename, "r");
  if (!f)
    die_sys("could not open '%s'", filename);
  char line[1024];
  while (fgets(line, sizeof(line), f)) {
    char *key = strtok(line, config_delim);
    if (!key || key[0] == '#') {
      continue; /* empty line or comment */
    }
    char *value = strtok(NULL, config_delim);
    if (!value || (
      !strcmp(key, "NumberOfPreferredNeighbors") ?
        parse_pint(value, &num_preferred_neighbors) :
      !strcmp(key, "UnchokingInterval") ?
        parse_pint(value, &unchoking_interval) :
      !strcmp(key, "OptimisticUnchokingInterval") ?
        parse_pint(value, &optimistic_unchoking_interval) :
      !strcmp(key, "FileName") ?
        (strcpy(the_file_name, value), 0) :
      !strcmp(key, "FileSize") ?
        parse_pint(value, &the_file_size) :
      !strcmp(key, "PieceSize") ?
        parse_pint(value, &piece_size) :
    1)) die("failed to parse '%s'", filename);
  }
  fclose(f);
  unchoking_interval *= 1000;
  optimistic_unchoking_interval *= 1000;
  num_pieces = (the_file_size + piece_size - 1) / piece_size;
  bitfield_size = (num_pieces + 7) >> 3;
}

int main(int argc, char **argv)
{
  static const char usage[] = "usage: peer <peer_id>\n";
  if (argc != 2 || parse_pint(argv[1], &self_id)) {
    write(2, usage, sizeof(usage) - 1);
    return 2;
  }

  char filename[1024];

  /* open log file */
  snprintf(filename, sizeof(filename), "log_peer_%u.log", self_id);
  logfd = open(filename, O_WRONLY | O_CREAT | O_TRUNC, 0666);
  if (logfd == -1)
    die_sys("failed to open '%s'", filename);

  /* create the directory if necessary */
  snprintf(filename, sizeof(filename), "peer_%u", self_id);
  if (mkdir(filename, 0777) && errno != EEXIST)
    die_sys("failed to create directory '%s'", filename);

  config_load("Common.cfg");
  conn_init("PeerInfo.cfg");

  /* event loop */
  struct timespec ts;
  unsigned int prev_time, cur_time, t1 = -1, t2 = -1;
  struct pollfd *pfds, *pfd;
  struct conn *c;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  prev_time = ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
  srand48(prev_time + self_id);
  pfds = calloc(conn_count, sizeof(*pfds));

loop:
  clock_gettime(CLOCK_MONOTONIC, &ts);
  cur_time = ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
  if ((t1 -= cur_time - prev_time) > unchoking_interval) {
    reselect_preferred_neighbors();
    t1 = unchoking_interval;
  }
  if ((t2 -= cur_time - prev_time) > optimistic_unchoking_interval) {
    optimistic_unchoke_neighbor();
    t2 = optimistic_unchoking_interval;
  }
  prev_time = cur_time;
  int done = 1;
  for (c = conn_head, pfd = pfds; c; c = c->next, ++pfd) {
    pfd->fd = c->sock;
    pfd->events = c->wsize ? (POLLOUT | POLLIN) : POLLIN;
    if (!(c->flags & CONN_SHUT) || c->wsize)
      done = 0;
  }
  if (done) {
    return 0;
  }
  if (poll(pfds, conn_count, (t1 < t2 ? t1 : t2) + 1) < 0) {
    die_sys("cannot poll");
  }
  for (c = conn_head, pfd = pfds; c; c = c->next, ++pfd) {
    if (!pfd->revents)
      continue;
    if (pfd->revents & POLLOUT)
      conn_handle_write(c);
    if (pfd->revents & ~POLLOUT)
      conn_handle_read(c);
  }
  goto loop;
}
