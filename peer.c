#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <poll.h>
#include <setjmp.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
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
static unsigned char bitfield_tailmask;

static unsigned int self_id;
static uint16_t self_port;
static int self_has_file;
static int self_sock = -1;
static unsigned char *self_bitfield;
static unsigned int self_num_pieces;
static int logfd = -1;
static char *the_file = MAP_FAILED;

#define NBR_ISCONN  1
#define NBR_WINT    4
#define NBR_RFLOW   8
#define NBR_DOCONN  16
#define NBR_WPREF   32
#define NBR_WOPT    64
#define NBR_SHUT    128
#define NBR_WFLOW   (NBR_WPREF | NBR_WOPT)

struct neighbor
{
  struct neighbor *next;
  struct neighbor **slot;
  unsigned int id;
  int flags;
  char host[256];
  uint16_t port;
  unsigned char *bitfield;
  unsigned int num_pieces;
  unsigned int num_want;
  unsigned int cur_req;
};

static struct neighbor *nbr_head;
static unsigned int nbr_count;

/* Functions */

static void __attribute__((format(printf, 1, 2)))
err(const char *format, ...)
{
  va_list ap;
  char buf[2048], *s = buf;
  s += snprintf(buf, sizeof(buf), "[%u] ", self_id);
  va_start(ap, format);
  vsnprintf(s, buf + sizeof(buf) - s, format, ap);
  va_end(ap);
  for (; *s; ++s)
    if (*s < 0x20 || *s == 0x7f)
      *s = '?';
  *s = '\n';
  write(2, buf, ++s - buf);
}

#define err_sys_(format, ...) err(format ": %s", __VA_ARGS__)
#define err_sys(...) err_sys_(__VA_ARGS__, strerror(errno))
#define die(...) err(__VA_ARGS__), exit(1)
#define die_sys(...) err_sys(__VA_ARGS__), exit(1)

static void __attribute__((format(printf, 1, 2)))
shit(const char *format, ...)
{
  va_list ap;
  char buf[2048], *s = buf;
  time_t tim = time(NULL);
  struct tm *tm = localtime(&tim);
  s += strftime(buf, sizeof(buf), "%Y-%m-%d %I:%M:%S %p: ", tm);
  va_start(ap, format);
  vsnprintf(s, buf + sizeof(buf) - s, format, ap);
  va_end(ap);
  for (; *s; ++s)
    if (*s < 0x20 || *s == 0x7f)
      *s = '?';
  *s = '\n';
  write(logfd, buf, ++s - buf);
}

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

static void
neighbor_destroy(struct neighbor *nbr)
{
  *nbr->slot = nbr->next;
  if (nbr->next)
    nbr->next->slot = nbr->slot;
  free(nbr->bitfield);
  free(nbr);
  --nbr_count;
}

/* Connection */

struct conn
{
  struct conn *next;
  char *wbuf;
  char *rbuf;
  struct neighbor *nbr;
  void (*on_read)(struct conn *c, char *buf);
  int sock;
  unsigned int wcap;
  unsigned int wsize;
  unsigned int rcap;
  unsigned int rsize;
  unsigned int rwant;
  unsigned int rrate;
};

static struct conn *conn_head;
static unsigned int conn_count;
static jmp_buf conn_abort_jmp_buf;
static struct conn **leaderboard;

static void
conn_abort(void)
{
  longjmp(conn_abort_jmp_buf, 1);
}

static void
conn_shutdown(struct conn *c)
{
  c->nbr->flags |= NBR_SHUT;
  if (!c->wsize)
    shutdown(c->sock, SHUT_WR);
}

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
conn_request_any(struct conn *c)
{
  struct neighbor *nbr = c->nbr;
  unsigned int off = (unsigned int)mrand48() % nbr->num_want;
  for (unsigned int index = 0; index < num_pieces; ++index) {
    unsigned char mask = 128 >> (index & 7);
    /* TODO: don't send already requested */
    if ((nbr->bitfield[index >> 3] & ~self_bitfield[index >> 3] & mask) && !off--) {
      conn_write_header(c, 4, P2P_REQUEST);
      conn_write_uint32(c, index);
      return;
    }
  }
}

static void
conn_read_have(struct conn *c, char *buf)
{
  struct neighbor *nbr = c->nbr;
  unsigned int index = read_uint32(buf);
  if (index >= num_pieces) {
    err("requested piece %u, max is %u", index, num_pieces - 1);
    conn_abort();
  }
  shit("Peer %u received the 'have' message from %u for the piece %u.", self_id, nbr->id, index);
  unsigned char mask = 128 >> (index & 7);
  if (nbr->bitfield[index >> 3] & mask) {
    return;
  }
  nbr->bitfield[index >> 3] |= mask;
  ++nbr->num_pieces;
  if (!(self_bitfield[index >> 3] & mask) && !nbr->num_want++) {
    conn_write_header(c, 0, P2P_INT);
  }
  if (nbr->num_pieces == num_pieces && self_num_pieces == num_pieces) {
    conn_shutdown(c);
  }
}

static void
conn_read_bitfield(struct conn *c, char *buf)
{
  struct neighbor *nbr = c->nbr;
  memcpy(nbr->bitfield, buf, bitfield_size);
  if (bitfield_tailmask)
    nbr->bitfield[bitfield_size - 1] &= bitfield_tailmask;
  nbr->num_pieces = 0;
  nbr->num_want = 0;
  for (unsigned int index = 0; index < num_pieces; ++index) {
    unsigned char mask = 128 >> (index & 7);
    if (!(nbr->bitfield[index >> 3] & mask))
      continue;
    ++nbr->num_pieces;
    if (!(self_bitfield[index >> 3] & mask))
      ++nbr->num_want;
  }
  if (nbr->num_want)
    conn_write_header(c, 0, P2P_INT);
  else
    conn_write_header(c, 0, P2P_NOINT);
  // TODO: maybe close connection
}

static void
conn_read_request(struct conn *c, char *buf)
{
  struct neighbor *nbr = c->nbr;
  uint32_t index = read_uint32(buf);
  if (!(nbr->flags & NBR_WFLOW))
    return; /* choked */
  if (index >= num_pieces) {
    err("requested piece %u, max is %u", index, num_pieces - 1);
    conn_abort();
  }
  unsigned int offset = index * piece_size;
  unsigned int size = index == num_pieces - 1 ? the_file_size - offset : piece_size;
  /* TODO: instrument read */
  conn_write_header(c, 4 + size, P2P_PIECE);
  conn_write_uint32(c, index);
  conn_write(c, the_file + offset, size);
}

static void
conn_read_piece(struct conn *c, char *buf)
{
  struct neighbor *nbr = c->nbr;
  uint32_t index = read_uint32(buf);
  if (index >= num_pieces) {
    err("sent piece %u, max is %u", index, num_pieces - 1);
    conn_abort();
  }
  unsigned char mask = 128 >> (index & 7);
  if (!(self_bitfield[index >> 3] & mask)) {
    unsigned int offset = index * piece_size;
    /* TODO: instrument write */
    memcpy(the_file + offset, buf + 4, index == num_pieces - 1 ?
        the_file_size - offset : piece_size);
    self_bitfield[index >> 3] |= mask;
    ++self_num_pieces;
    shit("Peer %u has downloaded the piece %u from %u. "
         "Now the number of pieces it has is %u.",
         self_id, index, nbr->id, self_num_pieces);
    if (self_num_pieces == num_pieces) {
      shit("Peer %u has downloaded the complete file.", self_id);
    }
    for (struct conn *o = conn_head; o; o = o->next) {
      struct neighbor *oth = o->nbr;
      if (!oth)
        continue;
      conn_write_header(o, 4, P2P_HAVE);
      conn_write_uint32(o, index);
      if (oth->num_want && (oth->bitfield[index >> 3] & mask) && !--oth->num_want)
        conn_write_header(o, 0, P2P_NOINT);
      if (oth->num_pieces == num_pieces && self_num_pieces == num_pieces)
        conn_shutdown(o);
    }
  }
  if (nbr->num_want && (nbr->flags & NBR_RFLOW))
    conn_request_any(c);
}

static void
conn_read_message(struct conn *c, char *buf)
{
  uint32_t len = read_uint32(buf);
  switch (buf[4]) {
    case P2P_CHOKE:
      if (len != 1) {
        err("CHOKE: expected length 1, got %u", len);
        conn_abort();
      }
      shit("Peer %u is choked by %u.", self_id, c->nbr->id);
      c->nbr->flags &= ~NBR_RFLOW;
      break;
    case P2P_UNCHOKE:
      if (len != 1) {
        err("UNCHOKE: expected length 1, got %u", len);
        conn_abort();
      }
      shit("Peer %u is unchoked by %u.", self_id, c->nbr->id);
      c->nbr->flags |= NBR_RFLOW;
      conn_request_any(c);
      break;
    case P2P_INT:
      if (len != 1) {
        err("INT: expected length 1, got %u", len);
        conn_abort();
      }
      shit("Peer %u received the 'interested' message from %u.", self_id, c->nbr->id);
      c->nbr->flags |= NBR_WINT;
      break;
    case P2P_NOINT:
      if (len != 1) {
        err("NOINT: expected length 1, got %u", len);
        conn_abort();
      }
      shit("Peer %u received the 'not interested' message from %u.", self_id, c->nbr->id);
      c->nbr->flags &= ~NBR_WINT;
      break;
    case P2P_HAVE:
      if (len != 5) {
        err("HAVE: expected length 5, got %u", len);
        conn_abort();
      }
      c->rwant = 4;
      c->on_read = conn_read_have;
      break;
    case P2P_BITFIELD:
      if (len != 1 + bitfield_size) {
        err("BITFIELD: expected length %u, got %u", 1 + bitfield_size, len);
        conn_abort();
      }
      c->rwant = bitfield_size;
      c->on_read = conn_read_bitfield;
      break;
    case P2P_REQUEST:
      if (len != 5) {
        err("REQUEST: expected length 5, got %u", len);
        conn_abort();
      }
      c->rwant = 4;
      c->on_read = conn_read_request;
      break;
    case P2P_PIECE:
      /* TODO: validate! */
      c->rwant = len - 1;
      c->on_read = conn_read_piece;
      break;
    default:
      err("invalid message type %u", buf[4]);
      conn_abort();
  }
}

static void
conn_read_handshake(struct conn *c, char *buf)
{
  struct neighbor *nbr = c->nbr;
  uint32_t id = read_uint32(buf + sizeof(magic));
  if (memcmp(magic, buf, sizeof(magic)) || (nbr && nbr->id != id)) {
    err("invalid handshake");
    conn_abort();
  }
  if (!nbr) {
    for (nbr = nbr_head; nbr; nbr = nbr->next) {
      if (nbr->id == id) {
        if (!nbr->flags & NBR_ISCONN)
          goto found;
        err("%u already connected", id);
        conn_abort();
      }
    }
    err("unrecognized peer: %u", id);
    conn_abort();
found:
    shit("Peer %u is connected from Peer %u", self_id, id);
    nbr->flags |= NBR_ISCONN;
    c->nbr = nbr;
  }
  conn_write_header(c, bitfield_size, P2P_BITFIELD);
  conn_write(c, self_bitfield, bitfield_size);
}

static void
conn_init(int sock, struct neighbor *nbr)
{
  fcntl(sock, F_SETFL, O_NONBLOCK);
  struct conn *c = calloc(1, sizeof(struct conn));
  c->sock = sock;
  c->wcap = 32;
  c->rcap = 32;
  c->rwant = 32;
  c->wbuf = malloc(c->wcap);
  c->rbuf = malloc(c->rcap);
  c->nbr = nbr;
  c->on_read = conn_read_handshake;
  c->next = conn_head;
  conn_head = c;
  ++conn_count;
  /* send handshake */
  conn_write(c, magic, sizeof(magic));
  conn_write_uint32(c, self_id);
}

static void
conn_handle_write(struct conn *c)
{
  ssize_t len = send(c->sock, c->wbuf, c->wsize, 0);
  if (len < 0) {
    err_sys("send");
    conn_abort();
  }
  c->wsize -= len;
  if (c->wsize)
    memmove(c->wbuf, c->wbuf + len, c->wsize);
  else if (c->nbr && c->nbr->flags & NBR_SHUT)
    shutdown(c->sock, SHUT_WR);
}

static void
conn_handle_read(struct conn *c)
{
  if (c->rcap < c->rwant)
    c->rbuf = realloc(c->rbuf, (c->rcap = c->rwant));
  ssize_t len = recv(c->sock, c->rbuf + c->rsize, c->rcap - c->rsize, 0);
  if (len < 0) {
    if (errno == EAGAIN || errno == EWOULDBLOCK)
      return;
    err_sys("recv");
    conn_abort();
  }
  if (!len) {
    if (c->nbr && c->nbr->flags & NBR_SHUT)
      neighbor_destroy(c->nbr);
    else if (c->nbr)
      err("connection with %u closed unexpectedly", c->nbr->id);
    else
      err("connection closed unexpectedly");
    conn_abort();
  }
  c->rsize += len;
  char *next = c->rbuf, *cur;
  while (c->rsize >= c->rwant) {
    cur = next;
    next += c->rwant;
    c->rsize -= c->rwant;
    c->rwant = 0;
    c->on_read(c, cur);
    if (!c->rwant) {
      c->rwant = 5;
      c->on_read = conn_read_message;
    }
  }
  if (c->rsize)
    memmove(c->rbuf, next, c->rsize);
}

static void
neighbor_connect(struct neighbor *nbr)
{
  struct addrinfo *ai, *ai_head;
  struct addrinfo hints = { .ai_family = AF_UNSPEC, .ai_socktype = SOCK_STREAM };
  int sock, res;
  res = getaddrinfo(nbr->host, NULL, &hints, &ai_head);
  if (res)
    die("failed to resolve '%s': %s", nbr->host, gai_strerror(res));
  uint16_t port_be = htons(nbr->port);
  for (ai = ai_head; ai; ai = ai->ai_next) {
    if (ai->ai_family == AF_INET) {
      ((struct sockaddr_in *)ai->ai_addr)->sin_port = port_be;
    } else { /* AF_INET6 */
      ((struct sockaddr_in6 *)ai->ai_addr)->sin6_port = port_be;
    }
    sock = socket(ai->ai_family, SOCK_STREAM, 0);
    if (sock == -1)
      die_sys("socket");
    if (!connect(sock, ai->ai_addr, ai->ai_addrlen))
      goto success;
    err_sys("connect");
    close(sock);
  }
  exit(1);
success:
  freeaddrinfo(ai_head);
  shit("Peer %u makes a connection to Peer %u", self_id, nbr->id);
  conn_init(sock, nbr);
}

static int
conn_ratecmp(const void *a, const void *b)
{
  return (int)(*(struct conn **)a)->rrate - (*(struct conn **)b)->rrate;
}

static void
reselect_preferred_neighbors(void)
{
  unsigned int count = 0;
  struct conn *c;
  for (c = conn_head; c; c = c->next) {
    if (c->nbr && c->nbr->flags & NBR_WINT) {
      leaderboard[count++] = c;
    }
  }
  qsort(leaderboard, count, sizeof(*leaderboard), conn_ratecmp);
  for (unsigned int i = 0; i < count; ++i) {
    c = leaderboard[i];
    if (i < num_preferred_neighbors) {
      if (!(c->nbr->flags & NBR_WFLOW))
        conn_write_header(c, 0, P2P_UNCHOKE);
      c->nbr->flags |= NBR_WPREF;
    } else {
      if ((c->nbr->flags & NBR_WFLOW) == NBR_WPREF)
        conn_write_header(c, 0, P2P_CHOKE);
      c->nbr->flags &= ~NBR_WPREF;
    }
  }
  char buf[2048];
  int len = 0;
  strcpy(buf, "(none)");
  for (unsigned int i = 0; i < count && i < num_preferred_neighbors; ++i) {
    len += snprintf(buf + len, sizeof(buf) - len, i ? ", %u" : "%u", leaderboard[i]->nbr->id);
    if (len > sizeof(buf)) {
      len = sizeof(buf);
      break;
    }
  }
  shit("Peer %u has the preferred neighbors %s.", self_id, buf);
}

static void
optimistic_unchoke_neighbor(void)
{
  int count = 0;
  struct conn *c;
  for (c = conn_head; c; c = c->next) {
    if (c->nbr) {
      if (c->nbr->flags & NBR_WINT && !(c->nbr->flags & NBR_WFLOW)) {
        leaderboard[count++] = c;
      } else {
        if ((c->nbr->flags & NBR_WFLOW) == NBR_WOPT)
          conn_write_header(c, 0, P2P_CHOKE);
        c->nbr->flags &= ~NBR_WOPT;
      }
    }
  }
  if (!count)
    return;
  c = leaderboard[(unsigned int)mrand48() % count];
  conn_write_header(c, 0, P2P_UNCHOKE);
  c->nbr->flags |= NBR_WOPT;
  shit("Peer %u has the optimistically unchoked neighbor %u.", self_id, c->nbr->id);
}

static void
handle_accept(void)
{
  int sock = accept(self_sock, NULL, NULL);
  if (sock == -1) {
    if (errno == EAGAIN || errno == EWOULDBLOCK || errno == ECONNABORTED)
      return;
    die_sys("accept");
  }
  conn_init(sock, NULL);
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
    if (!value) {
      goto fail;
    }
    if (!strcmp(key, "NumberOfPreferredNeighbors")) {
      if (parse_pint(value, &num_preferred_neighbors))
        goto fail;
    } else if (!strcmp(key, "UnchokingInterval")) {
      if (parse_pint(value, &unchoking_interval))
        goto fail;
    } else if (!strcmp(key, "OptimisticUnchokingInterval")) {
      if (parse_pint(value, &optimistic_unchoking_interval))
        goto fail;
    } else if (!strcmp(key, "FileName")) {
      strcpy(the_file_name, value);
    } else if (!strcmp(key, "FileSize")) {
      if (parse_pint(value, &the_file_size))
        goto fail;
    } else if (!strcmp(key, "PieceSize")) {
      if (parse_pint(value, &piece_size))
        goto fail;
    } else {
      goto fail;
    }
  }
  fclose(f);
  unchoking_interval *= 1000;
  optimistic_unchoking_interval *= 1000;
  num_pieces = (the_file_size + piece_size - 1) / piece_size;
  bitfield_size = (num_pieces + 7) >> 3;
  bitfield_tailmask = ~(255 >> (num_pieces & 7));
  return;
fail:
  die("failed to parse '%s'", filename);
}

static void
peerinfo_load(const char *filename)
{
  FILE *f = fopen(filename, "r");
  if (!f)
    die_sys("could not open '%s'", filename);
  char line[1024];
  while (fgets(line, sizeof(line), f)) {
    char *fid, *fhst, *fprt, *fhas;
    unsigned int id, port;
    if (!(fid  = strtok(line, config_delim)) ||
        !(fhst = strtok(NULL, config_delim)) ||
        !(fprt = strtok(NULL, config_delim)) ||
        !(fhas = strtok(NULL, config_delim)) ||
        parse_pint(fid, &id) || !id ||
        parse_pint(fprt, &port) || !port || port > 0xffff) {
      goto abort;
    }
    if (id == self_id) {
      self_port = port;
      self_has_file = !strcmp(fhas, "1");
      continue;
    }
    struct neighbor *nbr, **slot;
    for (slot = &nbr_head; (nbr = *slot); slot = &nbr->next)
      if (nbr->id == id)
        goto abort;
    nbr = calloc(1, sizeof(*nbr));
    nbr->next = NULL;
    nbr->slot = slot;
    nbr->id = id;
    if (!self_port)
      nbr->flags |= NBR_DOCONN;
    size_t hostlen = strlen(fhst);
    memcpy(nbr->host, fhst, hostlen < sizeof(nbr->host) ? hostlen : sizeof(nbr->host) - 1);
    nbr->port = port;
    nbr->bitfield = calloc(1, bitfield_size);
    *slot = nbr;
    ++nbr_count;
  }
  fclose(f);
  if (!self_port)
    die("could not find self configuration");
  return;
abort:
  die("could not parse '%s'", filename);
}

int main(int argc, char **argv)
{
  static const char usage[] = "usage: peer <peer_id>\n";
  if (argc != 2 || parse_pint(argv[1], &self_id)) {
    write(2, usage, sizeof(usage) - 1);
    return 2;
  }

  config_load("Common.cfg");
  peerinfo_load("PeerInfo.cfg");

  self_bitfield = calloc(1, bitfield_size);
  if (self_has_file) {
    memset(self_bitfield, -1, bitfield_size);
    if (bitfield_tailmask)
      self_bitfield[bitfield_size - 1] &= bitfield_tailmask;
    self_num_pieces = num_pieces;
  }
  leaderboard = calloc(nbr_count, sizeof(*leaderboard));

  char filename[1024];
  struct sockaddr_in6 addr = {
    .sin6_family = AF_INET6,
    .sin6_port = htons(self_port),
    .sin6_addr = IN6ADDR_ANY_INIT
  };

  /* start server */
  self_sock = socket(AF_INET6, SOCK_STREAM, 0);
  if (self_sock == -1)
    die_sys("socket");
  setsockopt(self_sock, IPPROTO_IPV6, IPV6_V6ONLY, &cero, sizeof(cero));
  setsockopt(self_sock, SOL_SOCKET, SO_REUSEADDR, &uno, sizeof(uno));
  if (bind(self_sock, (const struct sockaddr *)&addr, sizeof(addr)))
    die_sys("could not bind to port %u", (unsigned int)self_port);
  if (listen(self_sock, 16))
    die_sys("listen");
  fcntl(self_sock, F_SETFL, O_NONBLOCK);

  /* open log file */
  snprintf(filename, sizeof(filename), "log_peer_%u.log", self_id);
  logfd = open(filename, O_WRONLY | O_CREAT | O_TRUNC, 0666);
  if (logfd == -1)
    die_sys("failed to open '%s'", filename);

  /* create the directory if necessary */
  snprintf(filename, sizeof(filename), "peer_%u", self_id);
  if (mkdir(filename, 0777) && errno != EEXIST)
    die_sys("failed to create directory '%s'", filename);

  /* open the file */
  snprintf(filename, sizeof(filename), "peer_%u/%s", self_id, the_file_name);
  int the_fd = open(filename, self_has_file ? O_RDONLY : (O_RDWR | O_CREAT), 0666);
  if (the_fd == -1) {
    err_sys("failed to open '%s'", filename);
    exit(1);
  }
  int prot = self_has_file ? PROT_READ : PROT_READ | PROT_WRITE;
  /* TODO */
  the_file = mmap(NULL, the_file_size, prot, MAP_PRIVATE | MAP_ANON, the_fd, 0);
  if (the_file == MAP_FAILED)
    die_sys("failed to mmap '%s'", filename);
  close(the_fd);

  /* connect neighbors */
  usleep(100000); /* TODO: currently sleeping for 100ms */
  for (struct neighbor *nbr = nbr_head; nbr; nbr = nbr->next) {
    if (nbr->flags & NBR_DOCONN) {
      neighbor_connect(nbr);
    }
  }

  /* event loop */
  struct timespec ts;
  unsigned int unchoke_at = 0;
  unsigned int optimistic_unchoke_at = 0;
  unsigned int now, timeout, timeout2;
  unsigned int pfds_max = 0;
  struct pollfd *pfds = NULL, *pfd;
  struct conn *c, **cslot;

  while (nbr_count) {
    clock_gettime(CLOCK_MONOTONIC, &ts);
    now = ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
    timeout = unchoke_at - now;
    if (timeout - 1 > unchoking_interval) {
      reselect_preferred_neighbors();
      unchoke_at = now + unchoking_interval;
      timeout = unchoking_interval;
    }
    timeout2 = optimistic_unchoke_at - now;
    if (timeout2 - 1 > optimistic_unchoking_interval) {
      optimistic_unchoke_neighbor();
      optimistic_unchoke_at = now + optimistic_unchoking_interval;
      timeout2 = optimistic_unchoking_interval;
    }
    if (timeout2 < timeout) {
      timeout = timeout2;
    }
    if (pfds_max <= conn_count) {
      free(pfds);
      pfds = malloc((pfds_max = conn_count + 1) * sizeof(*pfds));
    }
    pfd = pfds;
    pfd->fd = self_sock;
    pfd->events = POLLIN;
    for (c = conn_head; c; c = c->next) {
      ++pfd;
      pfd->fd = c->sock;
      pfd->events = c->wsize ? (POLLOUT | POLLIN) : POLLIN;
    }
    int count = poll(pfds, conn_count + 1, timeout);
    if (count < 0)
      die_sys("poll");
    pfd = pfds;
    if (pfd->revents) {
      --count;
      handle_accept();
    }
    for (cslot = &conn_head; (c = *cslot); ) {
      ++pfd;
      if (!pfd->revents) {
        cslot = &c->next;
        continue;
      }
      --count;
      if (setjmp(conn_abort_jmp_buf)) {
        *cslot = c->next;
        close(c->sock);
        free(c->wbuf);
        free(c->rbuf);
        free(c);
        --conn_count;
        continue;
      }
      if (pfd->revents & POLLOUT)
        conn_handle_write(c);
      if (pfd->revents & ~POLLOUT)
        conn_handle_read(c);
      cslot = &c->next;
    }
  }

  return 0;
}
