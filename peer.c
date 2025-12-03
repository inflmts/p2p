#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>

#ifndef _WIN32
# include <netdb.h>
# include <netinet/in.h>
# include <poll.h>
# include <sys/socket.h>
# include <unistd.h>
# define closesocket(sock) close(sock)
#else
# include <io.h>
# include <winsock2.h>
# include <ws2tcpip.h>
# define SHUT_WR SD_SEND
# define mkdir(path, mode) mkdir(path)
# define ftruncate(fd, size) chsize(fd, size)
# define poll(pfds, n, t) WSAPoll(pfds, n, t)
#endif

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

struct peerinfo
{
  struct peerinfo *next;
  unsigned int id;
  unsigned int port;
  int has_file;
  char host[256];
};

static struct peerinfo *peerinfo;
static int num_peers;

struct ev_handler
{
  int (*callback)(void *);
  void *userdata;
};

struct ev_timer
{
  int (*callback)(void *);
  void *userdata;
  unsigned int expire;
  unsigned int timeout;
};

struct conn;

static struct pollfd *ev_pollfds;
static struct ev_handler *ev_handlers;
static struct ev_timer *ev_timers;
static int ev_cap;
static int ev_count;
static int ev_timer_cap;
static int ev_timer_count;
static unsigned int current_time;
static struct conn **board;

struct peer
{
  int id;
  int sock;
  int logfd;
  int filefd;
  unsigned char *have;
  unsigned char *pend;
  unsigned int num_have;
  struct peerinfo *next_info;
  struct conn *conn_head;
};

#define CONN_WINT   4
#define CONN_RFLOW  8
#define CONN_WPREF  32
#define CONN_WOPT   64
#define CONN_REQ    256
#define CONN_WFLOW  (CONN_WPREF | CONN_WOPT)

struct conn
{
  struct peer *peer;
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
  unsigned int num_have;
  unsigned int num_want;
  unsigned int req;
  void (*on_read)(struct conn *c, char *buf);
};

#define REPORT_ERRNO 1
#define REPORT_FATAL 2

__attribute__((format(printf, 3, 4)))
static void report(int flags, struct peer *p, const char *format, ...)
{
  va_list ap;
  char buf[2048], *s;
  struct timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);
  struct tm *tm = localtime(&ts.tv_sec);

  s = buf + sprintf(buf, "%04d-%02d-%02d %02d:%02d:%02d.%06d ",
      tm->tm_year + 1900, tm->tm_mon + 1, tm->tm_mday,
      tm->tm_hour, tm->tm_min, tm->tm_sec, (int)ts.tv_nsec / 1000);
  if (p)
    s += sprintf(s, "[%u] ", p->id);
  if (flags & REPORT_FATAL)
    s += sprintf(s, "error: ");
  if (flags & REPORT_ERRNO)
    s += sprintf(s, "%s, ", strerror(errno));
  va_start(ap, format);
  vsnprintf(s, buf + sizeof(buf) - s, format, ap);
  va_end(ap);

  /* scrub control characters and append newline */
  for (; *s; ++s)
    if (*s < 0x20 || *s == 0x7f)
      *s = '?';
  *(s++) = '\n';

  write(2, buf, s - buf);
  if (p)
    write(p->logfd, buf, s - buf);
}

#define msg(...) report(0, __VA_ARGS__)
#define msg_sys(...) report(REPORT_ERRNO, __VA_ARGS__)
#define die(...) report(REPORT_FATAL, __VA_ARGS__), exit(1)
#define die_sys(...) report(REPORT_ERRNO | REPORT_FATAL, __VA_ARGS__), exit(1)

#ifndef _WIN32
# define die_socket(...) die_sys(__VA_ARGS__)
#else
# define die_socket(...) die(__VA_ARGS__)
#endif

static int parse_pint(const char *s, unsigned int *dest)
{
  unsigned int value = 0;
  while (*s >= '0' && *s <= '9')
    value = value * 10 + (*(s++) - '0');
  if (*s || !value)
    return -1;
  *dest = value;
  return 0;
}

static uint32_t read_uint32(const void *buf)
{
  uint32_t value;
  memcpy(&value, buf, sizeof(value));
  return htonl(value);
}

/*
 * Register an event handler. The callback will be invoked when
 * any of the requested events occurs on the socket.
 * The callback should return nonzero to continue receiving events
 * or zero to be removed from the handler list.
 */
static void ev_register(int sock, int events, int (*callback)(void *), void *userdata)
{
  if (ev_count == ev_cap) {
    ev_cap = ev_cap ? ev_cap * 2 : 32;
    ev_pollfds = realloc(ev_pollfds, ev_cap * sizeof(*ev_pollfds));
    ev_handlers = realloc(ev_handlers, ev_cap * sizeof(*ev_handlers));
  }

  ev_pollfds[ev_count].fd = sock;
  ev_pollfds[ev_count].events = events;
  ev_pollfds[ev_count].revents = 0;
  ev_handlers[ev_count].callback = callback;
  ev_handlers[ev_count].userdata = userdata;
  ++ev_count;
}

/*
 * Schedule a function to be executed after a specified time in milliseconds.
 * The function should return nonzero to be scheduled again after the same timeout.
 */
static void ev_schedule(int (*callback)(void *), void *userdata, unsigned int timeout)
{
  if (ev_timer_count == ev_timer_cap) {
    ev_timer_cap = ev_timer_cap ? ev_timer_cap * 2 : 32;
    ev_timers = realloc(ev_timers, ev_timer_cap * sizeof(*ev_timers));
  }

  struct ev_timer *timer = &ev_timers[ev_timer_count++];
  timer->callback = callback;
  timer->userdata = userdata;
  timer->expire = current_time + timeout;
  timer->timeout = timeout;
}

static int conn_handle_write(void *userdata)
{
  struct conn *c = userdata;
  ssize_t len = send(c->sock, c->wbuf, c->wsize, 0);
  if (len < 0)
    die_socket(c->peer, "send to %u failed", c->id);
  c->wsize -= len;
  if (c->wsize) {
    /* wsize is now the size of the remaining data in the write buffer.
     * Shift it all to the front. */
    memmove(c->wbuf, c->wbuf + len, c->wsize);
  } else if (c->peer->num_have == num_pieces && c->num_have == num_pieces) {
    msg(c->peer, "closing connection with %u", c->id);
    shutdown(c->sock, SHUT_WR);
  }
  return c->wsize;
}

static void *conn_write_alloc(struct conn *c, unsigned int size)
{
  unsigned int oldsize = c->wsize;
  if (!c->wsize) {
    ev_register(c->sock, POLLOUT, conn_handle_write, c);
  }
  c->wsize += size;
  if (c->wcap < c->wsize) {
    do c->wcap *= 2;
    while (c->wcap < c->wsize);
    c->wbuf = realloc(c->wbuf, c->wcap);
  }
  return c->wbuf + oldsize;
}

static void conn_write(struct conn *c, const void *buf, unsigned int size)
{
  memcpy(conn_write_alloc(c, size), buf, size);
}

static void conn_write_uint32(struct conn *c, uint32_t value)
{
  value = htonl(value);
  conn_write(c, &value, 4);
}

static void conn_write_header(struct conn *c, uint32_t len, unsigned char type)
{
  conn_write_uint32(c, len + 1);
  conn_write(c, &type, 1);
}

static void conn_request(struct conn *c)
{
  struct peer *p = c->peer;
  if (!(c->flags & CONN_RFLOW) || !c->num_want || c->flags & CONN_REQ)
    return;
  unsigned int index = (unsigned int)rand() % num_pieces;
  unsigned char mask;
  while (!(c->have[index >> 3] & ~p->pend[index >> 3] & (mask = 128 >> (index & 7))))
    index = (index + 1) % num_pieces;
  p->pend[index >> 3] |= mask;
  conn_write_header(c, 4, P2P_REQUEST);
  conn_write_uint32(c, index);
  c->flags |= CONN_REQ;
  c->req = index;
  for (c = p->conn_head; c; c = c->next)
    if ((c->have[index >> 3] & mask) && !--c->num_want)
      conn_write_header(c, 0, P2P_NOINT);
}

static void conn_read_have(struct conn *c, char *buf)
{
  struct peer *p = c->peer;
  unsigned int index = read_uint32(buf);
  if (index >= num_pieces) {
    die(p, "received invalid 'have' for %u (max is %u) from %u.",
        index, num_pieces - 1, c->id);
  }
  unsigned char mask = 128 >> (index & 7);
  if (c->have[index >> 3] & mask) {
    die(p, "received duplicate 'have' from %u for %u.", c->id, index);
  }
  msg(p, "%u has %u", c->id, index);
  c->have[index >> 3] |= mask;
  ++c->num_have;
  if (!(p->pend[index >> 3] & mask) && !c->num_want++) {
    conn_write_header(c, 0, P2P_INT);
    conn_request(c);
  }
  if (!c->wsize && p->num_have == num_pieces && c->num_have == num_pieces) {
    msg(p, "closing connection with %u", c->id);
    shutdown(c->sock, SHUT_WR);
  }
}

static void conn_read_bitfield(struct conn *c, char *buf)
{
  /* TODO */
  struct peer *p = c->peer;
  memcpy(c->have, buf, bitfield_size);
  c->num_have = 0;
  c->num_want = 0;
  for (unsigned int index = 0; index < num_pieces; ++index) {
    unsigned char mask = 128 >> (index & 7);
    if (!(c->have[index >> 3] & mask))
      continue;
    ++c->num_have;
    if (!(p->pend[index >> 3] & mask))
      ++c->num_want;
  }
  if (c->num_want) {
    conn_write_header(c, 0, P2P_INT);
    conn_request(c);
  } else {
    conn_write_header(c, 0, P2P_NOINT);
  }
}

static void conn_read_request(struct conn *c, char *buf)
{
  struct peer *p = c->peer;
  uint32_t index = read_uint32(buf);
  if (!(c->flags & CONN_WFLOW)) {
    return; /* choked */
  }
  if (index >= num_pieces) {
    die(p, "received invalid request for %u from %u (max is %u)",
        index, c->id, num_pieces - 1);
  }
  unsigned int offset = index * piece_size;
  unsigned int size = index == num_pieces - 1 ? the_file_size - offset : piece_size;
  conn_write_header(c, 4 + size, P2P_PIECE);
  conn_write_uint32(c, index);

  /* read piece from file */
  lseek(p->filefd, offset, SEEK_SET);
  read(p->filefd, conn_write_alloc(c, size), size);
}

static void conn_read_piece(struct conn *c, char *buf)
{
  struct peer *p = c->peer;
  uint32_t index = read_uint32(buf);
  if (index >= num_pieces) {
    die(p, "received invalid piece %u from %u (max is %u)",
        index, c->id, num_pieces - 1);
  }
  if (!(c->flags & CONN_REQ) || c->req != index) {
    return; /* discard */
  }
  unsigned int offset = index * piece_size;
  unsigned int size = index == num_pieces - 1 ? the_file_size - offset : piece_size;

  msg(p, "downloaded piece %u from %u (%u/%u)",
      index, c->id, p->num_have, num_pieces);

  /* write piece to file */
  lseek(p->filefd, offset, SEEK_SET);
  write(p->filefd, buf + 4, size);

  c->flags &= ~CONN_REQ;
  c->rrate += size;
  p->have[index >> 3] |= 128 >> (index & 7);
  ++p->num_have;
  if (p->num_have == num_pieces) {
    msg(p, "download complete");
  }
  conn_request(c);
  for (c = p->conn_head; c; c = c->next) {
    conn_write_header(c, 4, P2P_HAVE);
    conn_write_uint32(c, index);
  }
}

static void conn_read_message(struct conn *c, char *buf)
{
  struct peer *p = c->peer;
  uint32_t len = read_uint32(buf);
  switch (buf[4]) {
    case P2P_CHOKE:
      if (len != 1) {
        die(p, "received 'choke' length %u, expected 1", len);
      }
      msg(p, "choked by %u", c->id);
      c->flags &= ~CONN_RFLOW;
      if (c->flags & CONN_REQ) {
        c->flags &= ~CONN_REQ;
        unsigned char mask = 128 >> (c->req & 7);
        p->pend[c->req >> 3] &= ~mask;
        for (c = p->conn_head; c; c = c->next)
          if ((c->have[c->req >> 3] & mask) && !c->num_want++)
            conn_write_header(c, 0, P2P_INT);
        for (c = p->conn_head; c; c = c->next)
          conn_request(c);
      }
      break;
    case P2P_UNCHOKE:
      if (len != 1) {
        die(p, "received 'unchoke' length %u, expected 1", len);
      }
      msg(p, "unchoked by %u", c->id);
      c->flags |= CONN_RFLOW;
      conn_request(c);
      break;
    case P2P_INT:
      if (len != 1) {
        die(p, "received 'interested' length %u, expected 1", len);
      }
      msg(p, "%u is interested", c->id);
      c->flags |= CONN_WINT;
      break;
    case P2P_NOINT:
      if (len != 1) {
        die(p, "received 'not interested' length %u, expected 1", len);
      }
      msg(p, "%u is not interested", c->id);
      c->flags &= ~CONN_WINT;
      break;
    case P2P_HAVE:
      if (len != 5) {
        die(p, "received 'have' length %u, expected 5", len);
      }
      c->rwant = 4;
      c->on_read = conn_read_have;
      break;
    case P2P_BITFIELD:
      if (len != 1 + bitfield_size) {
        die(p, "received 'bitfield' length %u, expected %u", len, 1 + bitfield_size);
      }
      c->rwant = bitfield_size;
      c->on_read = conn_read_bitfield;
      break;
    case P2P_REQUEST:
      if (len != 5) {
        die(p, "received 'request' length %u, expected 5", len);
      }
      c->rwant = 4;
      c->on_read = conn_read_request;
      break;
    case P2P_PIECE:
      if (len > 5 + piece_size) {
        die(p, "received 'piece' length %u, max %u", len, 5 + piece_size);
      }
      c->rwant = len - 1;
      c->on_read = conn_read_piece;
      break;
    default:
      die(p, "received invalid message type %u", (unsigned int)buf[4]);
  }
}

static void conn_read_handshake(struct conn *c, char *buf)
{
  struct peer *p = c->peer;
  if (memcmp(magic, buf, 28) || read_uint32(buf + 28) != c->id) {
    die(p, "received invalid handshake from %u", c->id);
  }
  msg(p, "received handshake from %u", c->id);
  conn_write_header(c, bitfield_size, P2P_BITFIELD);
  conn_write(c, p->have, bitfield_size);
}

static int conn_handle_read(void *userdata)
{
  struct conn *c = userdata;
  if (c->rcap < c->rwant)
    c->rbuf = realloc(c->rbuf, (c->rcap = c->rwant));

  ssize_t len = recv(c->sock, c->rbuf + c->rsize, c->rcap - c->rsize, 0);
  if (len < 0)
    die_socket(c->peer, "recv from %u failed", c->id);
  if (len == 0)
    return 0;

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
  if (c->rsize)
    memmove(c->rbuf, next, c->rsize);
  return 1;
}

static void peer_add(struct peer *p, int sock, struct peerinfo *info)
{
  struct conn *c = calloc(1, sizeof(struct conn));
  c->peer = p;
  c->have = calloc(1, bitfield_size);
  c->wbuf = malloc(32);
  c->rbuf = malloc(32);
  c->sock = sock;
  c->id = info->id;
  c->wcap = 32;
  c->rcap = 32;
  c->rwant = 32;
  c->on_read = conn_read_handshake;
  c->next = p->conn_head;
  p->conn_head = c;

  /* send handshake */
  conn_write(c, magic, sizeof(magic));
  conn_write_uint32(c, p->id);
  ev_register(c->sock, POLLIN, conn_handle_read, c);

#ifndef _WIN32
  fcntl(sock, F_SETFL, O_NONBLOCK);
#endif
}

static void peer_connect(struct peer *p, struct peerinfo *info)
{
  struct addrinfo *ai, *ai_head;
  struct addrinfo hints = { .ai_family = AF_UNSPEC, .ai_socktype = SOCK_STREAM };
  int sock, res;
  res = getaddrinfo(info->host, NULL, &hints, &ai_head);
  if (res)
    die(p, "failed to resolve '%s': %s", info->host, gai_strerror(res));

  for (ai = ai_head; ai; ai = ai->ai_next) {
    if (ai->ai_family == AF_INET) {
      ((struct sockaddr_in *)ai->ai_addr)->sin_port = htons(info->port);
    } else { /* AF_INET6 */
      ((struct sockaddr_in6 *)ai->ai_addr)->sin6_port = htons(info->port);
    }
    sock = socket(ai->ai_family, SOCK_STREAM, 0);
    if (sock == -1)
      die_socket(p, "socket");
    if (!connect(sock, ai->ai_addr, ai->ai_addrlen))
      goto success;
    closesocket(sock);
  }
  die_socket(p, "failed to connect to %u (%s:%u)", info->id, info->host, info->port);

success:
  freeaddrinfo(ai_head);
  msg(p, "connecting to %u (%s:%u)", info->id, info->host, info->port);
  peer_add(p, sock, info);
}

static int peer_handle_accept(void *userdata)
{
  struct peer *p = userdata;
  struct peerinfo *info = p->next_info;
  int sock = accept(p->sock, NULL, NULL);
  if (sock == -1)
    die_socket(p, "accept");
  msg(p, "accepted connection from %u", info->id);
  peer_add(p, sock, info);
  return (p->next_info = info->next) != NULL;
}

static int conn_ratecmp(const void *a, const void *b)
{
  return (int)(*(struct conn **)b)->rrate - (*(struct conn **)a)->rrate;
}

static int reselect_preferred_neighbors(void *userdata)
{
  struct peer *p = userdata;
  struct conn *c;
  int total = 0;
  int count = 0;

  for (c = p->conn_head; c; c = c->next)
    ++total;

  struct conn **tail = board + total;
  for (c = p->conn_head; c; c = c->next) {
    if (c->flags & CONN_WINT)
      board[count++] = c;
    else
      *(--tail) = c;
  }

  if (count) {
    /* https://en.wikipedia.org/wiki/Fisher-Yates_shuffle */
    for (int i = count - 1; i > 0; --i) {
      int j = (unsigned int)rand() % (i + 1);
      if (i != j) {
        c = board[i];
        board[i] = board[j];
        board[j] = c;
      }
    }
    if (p->num_have < num_pieces) {
      qsort(board, count, sizeof(*board), conn_ratecmp);
    }
  }

  char buf[2048];
  int len = 0;
  strcpy(buf, "(none)");
  for (int i = 0; i < total; ++i) {
    c = board[i];
    c->rrate = 0;
    if (i < count && i < num_preferred_neighbors) {
      if (len < sizeof(buf))
        len += snprintf(buf + len, sizeof(buf) - len, i ? ", %u" : "%u", board[i]->id);
      if (!(c->flags & CONN_WFLOW))
        conn_write_header(c, 0, P2P_UNCHOKE);
      c->flags |= CONN_WPREF;
    } else {
      if ((c->flags & CONN_WFLOW) == CONN_WPREF)
        conn_write_header(c, 0, P2P_CHOKE);
      c->flags &= ~CONN_WPREF;
    }
  }
  msg(p, "preferred neighbors: %s", buf);
  return 1;
}

static int optimistic_unchoke_neighbor(void *userdata)
{
  struct peer *p = userdata;
  struct conn *c;
  struct conn *wopt = NULL;
  int count = 0;
  for (c = p->conn_head; c; c = c->next) {
    if (c->flags & CONN_WOPT)
      wopt = c;
    if (c->flags & CONN_WINT && !(c->flags & CONN_WFLOW))
      board[count++] = c;
  }
  if (wopt) {
    if ((wopt->flags & CONN_WFLOW) == CONN_WOPT)
      conn_write_header(wopt, 0, P2P_CHOKE);
    wopt->flags &= ~CONN_WOPT;
  }
  if (count) {
    wopt = board[(unsigned int)rand() % count];
    conn_write_header(wopt, 0, P2P_UNCHOKE);
    wopt->flags |= CONN_WOPT;
    msg(p, "optimistically unchoked neighbor: %u", wopt->id);
  } else {
    msg(p, "no optimistically unchoked neighbor");
  }
  return 1;
}

static void peer_start(struct peerinfo *info)
{
  struct peer *p = calloc(1, sizeof(struct peer));
  p->id = info->id;
  p->have = calloc(2, bitfield_size);
  p->pend = p->have + bitfield_size;

  if (info->has_file) {
    memset(p->have, -1, bitfield_size * 2);
    p->num_have = num_pieces;
    /* the have bitfield needs to have the right padding bits
     * so we can copy directly onto the wire */
    if (num_pieces & 7)
      p->have[bitfield_size - 1] = ~(0xff >> (num_pieces & 7));
  }

  char filename[1024];
  struct stat st;

  /* open log file */
  snprintf(filename, sizeof(filename), "log_peer_%u.log", p->id);
  p->logfd = open(filename, O_WRONLY | O_CREAT | O_TRUNC, 0666);
  if (p->logfd == -1)
    die_sys(p, "failed to open '%s'", filename);

  msg(p, "starting: interval=%u,%u num_pieces=%u",
      unchoking_interval, optimistic_unchoking_interval, num_pieces);

  /* create the directory if necessary */
  snprintf(filename, sizeof(filename), "peer_%u", p->id);
  if (mkdir(filename, 0777) && errno != EEXIST)
    die_sys(p, "failed to create directory '%s'", filename);

  /* open the file */
  snprintf(filename, sizeof(filename), "peer_%u/%s", p->id, the_file_name);
  p->filefd = open(filename, info->has_file ? O_RDONLY : (O_RDWR | O_CREAT), 0666);
  if (p->filefd == -1)
    die_sys(p, "failed to open '%s'", filename);

  /* ensure the file has the right size */
  if (info->has_file) {
    if (fstat(p->filefd, &st)) {
      die_sys(p, "failed to stat '%s'", filename);
    }
    if (st.st_size != the_file_size) {
      die(p, "expected '%s' to be %u bytes, got %u",
          filename, the_file_size, (unsigned int)st.st_size);
    }
  } else if (ftruncate(p->filefd, the_file_size)) {
    die_sys(p, "could not resize '%s' to %u bytes", filename, the_file_size);
  }

  /* connect to all previous peers */
  for (struct peerinfo *prev = peerinfo; prev != info; prev = prev->next)
    peer_connect(p, prev);

  ev_schedule(reselect_preferred_neighbors, p, unchoking_interval);
  ev_schedule(optimistic_unchoke_neighbor, p, optimistic_unchoking_interval);

  if (!(p->next_info = info->next))
    return;

  struct sockaddr_in6 addr = {
    .sin6_family = AF_INET6,
    .sin6_port = htons(info->port),
    .sin6_addr = IN6ADDR_ANY_INIT
  };

  p->sock = socket(AF_INET6, SOCK_STREAM, 0);
  if (p->sock == -1)
    die_socket(p, "socket");
  if (setsockopt(p->sock, IPPROTO_IPV6, IPV6_V6ONLY, (const void *)&cero, sizeof(cero)))
    die_socket(p, "setsockopt");
  if (setsockopt(p->sock, SOL_SOCKET, SO_REUSEADDR, (const void *)&uno, sizeof(uno)))
    die_socket(p, "setsockopt");
  if (bind(p->sock, (const struct sockaddr *)&addr, sizeof(addr)))
    die_socket(p, "could not bind to port %u", (unsigned int)info->port);
  if (listen(p->sock, 64))
    die_socket(p, "listen");

  msg(p, "listening on port %u", info->port);
  ev_register(p->sock, POLLIN, peer_handle_accept, p);
}

static void config_load(const char *filename)
{
  FILE *f = fopen(filename, "r");
  if (!f)
    die_sys(NULL, "could not open '%s'", filename);
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
    1)) die(NULL, "failed to parse '%s'", filename);
  }
  fclose(f);
  unchoking_interval *= 1000;
  optimistic_unchoking_interval *= 1000;
  num_pieces = (the_file_size + piece_size - 1) / piece_size;
  bitfield_size = (num_pieces + 7) >> 3;
}

static void peerinfo_load(const char *filename)
{
  char line[1024];
  FILE *f = fopen(filename, "r");
  if (!f)
    die_sys(NULL, "could not open '%s'", filename);

  struct peerinfo **tail = &peerinfo;

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
      die(NULL, "could not parse '%s'", filename);
    }
    struct peerinfo *info = calloc(1, sizeof(*info));
    info->id = id;
    info->port = port;
    info->has_file = !strcmp(fhas, "1");
    strcpy(info->host, host);
    *tail = info;
    tail = &info->next;
    ++num_peers;
  }
  fclose(f);
//if (self_sock == -1)
//  die(p, "could not find self configuration");
//close(self_sock);
//board = calloc(conn_count, sizeof(*board));
}

int main(int argc, char **argv)
{
  unsigned int id = 0;
  static const char usage[] = "usage: peer [id]\n";
  if (argc > 2 || (argc == 2 && parse_pint(argv[1], &id))) {
    write(2, usage, sizeof(usage) - 1);
    return 2;
  }

#ifdef _WIN32
  WSADATA wsa_data;
  if (WSAStartup(MAKEWORD(2, 2), &wsa_data)) {
    die(NULL, "WSAStartup failed");
  }
#endif

  config_load("Common.cfg");
  peerinfo_load("PeerInfo.cfg");

  board = calloc(num_peers, sizeof(*board));

  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  current_time = ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
  srand(current_time + id);

  /* start peers */
  for (struct peerinfo *info = peerinfo; info; info = info->next)
    if (!id || id == info->id)
      peer_start(info);

  /* event loop */
  while (ev_count) {
    /* update current time */
    clock_gettime(CLOCK_MONOTONIC, &ts);
    current_time = ts.tv_sec * 1000 + ts.tv_nsec / 1000000;

    /* process timers */
    while (ev_timer_count && ev_timers[0].expire - current_time > ev_timers[0].timeout) {
      if (ev_timers[0].callback(ev_timers[0].userdata))
        ev_timers[0].expire += ev_timers[0].timeout;
      else
        ev_timers[0] = ev_timers[--ev_timer_count];

      unsigned int min = -1u;
      int m = 0;
      for (int i = 0; i < ev_timer_count; ++i) {
        if (ev_timers[i].expire - current_time > ev_timers[i].timeout) {
          m = i;
          break;
        }
        if (ev_timers[i].expire - current_time < min) {
          min = ev_timers[i].expire - current_time;
          m = i;
        }
        ++i;
      }
      if (m) {
        struct ev_timer tmp = ev_timers[0];
        ev_timers[0] = ev_timers[m];
        ev_timers[m] = tmp;
      }
    }

    /* wait for an event or timeout */
    int timeout = ev_timer_count ? ev_timers[0].expire - current_time + 1 : -1;
    int events = poll(ev_pollfds, ev_count, timeout);
    if (events < 0)
      die_socket(NULL, "poll");

    struct pollfd *pfd = ev_pollfds;
    struct ev_handler *handler = ev_handlers;

    while (events) {
      if (pfd->revents) {
        --events;
        if (!handler->callback(handler->userdata)) {
          --ev_count;
          *pfd = ev_pollfds[ev_count];
          *handler = ev_handlers[ev_count];
          continue;
        }
      }
      ++pfd;
      ++handler;
    }
  }

  msg(NULL, "exiting...");
  return 0;
}
