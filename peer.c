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
static unsigned int nbr_rcap;

static unsigned int self_id;
static uint16_t self_port;
static int self_has_file;
static int self_sock = -1;
static unsigned char *self_bitfield;
static unsigned char *self_reqfield;
static unsigned int self_rem_pieces;
static int logfd = -1;
static char *the_file = MAP_FAILED;

#define NEIGHBOR_CONN   1
#define NEIGHBOR_WFLOW  2
#define NEIGHBOR_WINT   4
#define NEIGHBOR_RFLOW  8

struct neighbor
{
  unsigned int id;
  int sock;
  char host[256];
  uint16_t port;
  unsigned char *bitfield;
  char *wbuf;
  char *rbuf;
  int flags;
  unsigned int num_want;
  unsigned int wcap;
  unsigned int wsize;
  unsigned int rsize;
  unsigned int rrate;
  int rtarget;
  void (*on_read)(struct neighbor *, char *);
  struct neighbor *prev;
  struct neighbor *next;
};

static unsigned int nbr_count;
static struct neighbor *nbr_head;
static struct neighbor **leaderboard;

/* Functions */

static void __attribute__((format(printf, 1, 2)))
err(const char *format, ...)
{
  va_list ap;
  char buf[2048], *p = buf;
  p += snprintf(buf, sizeof(buf), "[%u] ", self_id);
  va_start(ap, format);
  vsnprintf(p, buf + sizeof(buf) - p, format, ap);
  va_end(ap);
  for (; *p; ++p)
    if (*p < 0x20 || *p == 0x7f)
      *p = '?';
  *p = '\n';
  write(2, buf, ++p - buf);
}

#define err_sys_(format, ...) err(format ": %s", __VA_ARGS__)
#define err_sys(...) err_sys_(__VA_ARGS__, strerror(errno))

static void __attribute__((format(printf, 1, 2)))
shit(const char *format, ...)
{
  va_list ap;
  char buf[2048], *p = buf;
  time_t tim = time(NULL);
  struct tm *tm = localtime(&tim);
  p += strftime(buf, sizeof(buf), "%Y-%m-%d %I:%M:%S %p: ", tm);
  va_start(ap, format);
  vsnprintf(p, buf + sizeof(buf) - p, format, ap);
  va_end(ap);
  for (; *p; ++p)
    if (*p < 0x20 || *p == 0x7f)
      *p = '?';
  *p = '\n';
  write(logfd, buf, ++p - buf);
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

/* Configuration */

static void
config_load(const char *filename)
{
  FILE *f = fopen(filename, "r");
  if (!f) {
    err_sys("could not open '%s'", filename);
    exit(1);
  }
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
  /* doesn't matter what this is as long as any valid message will fit */
  nbr_rcap = bitfield_size + piece_size + 1024;
  return;
fail:
  err("failed to parse '%s'", filename);
  exit(1);
}

/* Event Loop */

typedef void (*ev_callback)(void *);

struct ev_handler
{
  ev_callback callback;
  void *userdata;
};

static unsigned int ev_cap;
static unsigned int ev_count;
static unsigned int ev_next;
static struct pollfd *ev_pollfds;
static struct ev_handler *ev_handlers;

static void
ev_register(int fd, int events, ev_callback callback, void *userdata)
{
  if (ev_count >= ev_cap) {
    ev_cap = ev_cap ? ev_cap * 2 : 64;
    ev_pollfds = realloc(ev_pollfds, ev_cap * sizeof(*ev_pollfds));
    ev_handlers = realloc(ev_handlers, ev_cap * sizeof(*ev_handlers));
  }
  ev_pollfds[ev_count].fd = fd;
  ev_pollfds[ev_count].events = events;
  ev_pollfds[ev_count].revents = 0;
  ev_handlers[ev_count].callback = callback;
  ev_handlers[ev_count].userdata = userdata;
  ++ev_count;
}

static void
ev_detach(ev_callback callback, void *userdata)
{
  for (unsigned int i = 0; i < ev_count; ++i) {
    if ((!callback || ev_handlers[i].callback == callback) && ev_handlers[i].userdata == userdata) {
      if (i != --ev_count) {
        memcpy(&ev_pollfds[i], &ev_pollfds[ev_count], sizeof(*ev_pollfds));
        memcpy(&ev_handlers[i], &ev_handlers[ev_count], sizeof(*ev_handlers));
        if (i < ev_next)
          ev_next = i;
        --i;
      }
    }
  }
}

/* Neighbor */

/*
static void
neighbor_destroy(struct neighbor *nbr)
{
  if (nbr->prev)
    nbr->prev->next = nbr->next;
  else
    nbr->me->nbr_head = nbr->next;
  if (nbr->next)
    nbr->next->prev = nbr->prev;
  else
    nbr->me->nbr_tail = nbr->prev;
  if (nbr->sock != -1)
    close(nbr->sock);
  free(nbr->bitfield);
  free(nbr->wbuf);
  free(nbr->rbuf);
  free(nbr);
}
*/

static void quarantine(struct neighbor *nbr, int sock);

static void
neighbor_connect(struct neighbor *nbr)
{
  struct addrinfo *ai, *ai_head, hints = {
    .ai_family = AF_UNSPEC,
    .ai_socktype = SOCK_STREAM
  };
  int res = getaddrinfo(nbr->host, NULL, &hints, &ai_head);
  if (res) {
    err("failed to resolve '%s': %s", nbr->host, gai_strerror(res));
    exit(1);
  }
  uint16_t port_be = htons(nbr->port);
  for (ai = ai_head; ai; ai = ai->ai_next) {
    if (ai->ai_family == AF_INET) {
      ((struct sockaddr_in *)ai->ai_addr)->sin_port = port_be;
    } else { /* AF_INET6 */
      ((struct sockaddr_in6 *)ai->ai_addr)->sin6_port = port_be;
    }
    nbr->sock = socket(ai->ai_family, SOCK_STREAM, 0);
    if (nbr->sock == -1) {
      err_sys("socket");
      exit(1);
    }
    if (!connect(nbr->sock, ai->ai_addr, ai->ai_addrlen))
      goto success;
    err_sys("connect");
    close(nbr->sock);
  }
  exit(1);
success:
  freeaddrinfo(ai_head);
  shit("Peer %u makes a connection to Peer %u", self_id, nbr->id);
  quarantine(nbr, nbr->sock);
}

static void
neighbor_write_callback(void *userdata)
{
  struct neighbor *nbr = userdata;
  ssize_t bytes_sent = send(nbr->sock, nbr->wbuf, nbr->wsize, 0);
  if (bytes_sent < 0) {
    err_sys("send");
    // TODO: abort connection
    nbr->wsize = 0;
    ev_detach(neighbor_write_callback, nbr);
  }
  nbr->wsize -= bytes_sent;
  if (!nbr->wsize)
    ev_detach(neighbor_write_callback, nbr);
  else
    memmove(nbr->wbuf, nbr->wbuf + bytes_sent, nbr->wsize);
}

static void
neighbor_write_alloc(struct neighbor *nbr, unsigned int size)
{
  nbr->wsize += size;
  if (nbr->wcap < nbr->wsize) {
    if (!nbr->wcap)
      nbr->wcap = 256;
    while (nbr->wcap < nbr->wsize)
      nbr->wcap *= 2;
    nbr->wbuf = realloc(nbr->wbuf, nbr->wcap);
  }
  if (nbr->wsize == size)
    ev_register(nbr->sock, POLLOUT, neighbor_write_callback, nbr);
}

static void
neighbor_write(struct neighbor *nbr, int type, const void *buf, unsigned int size)
{
  neighbor_write_alloc(nbr, 5 + size);
  char *start = nbr->wbuf + nbr->wsize - (5 + size);
  uint32_t msglen_be = htonl(1 + size);
  memcpy(start, &msglen_be, 4);
  start[4] = type;
  memcpy(start + 5, buf, size);
}

static void
neighbor_request(struct neighbor *nbr)
{
  /* TODO: randomize */
  /* if (nbr->num_want) */
  for (uint32_t i = 0; i < num_pieces; ++i) {
    unsigned int mask = 1 << (7 - (i & 7));
    if (nbr->bitfield[i >> 3] & ~self_reqfield[i >> 3] & mask) {
      self_reqfield[i >> 3] |= mask;
      i = htonl(i);
      neighbor_write(nbr, P2P_REQUEST, &i, 4);
      break;
    }
  }
}

static void
neighbor_read_have(struct neighbor *nbr, char *buf)
{
  uint32_t index;
  memcpy(&index, buf, 4);
  index = ntohl(index);
  if (index >= num_pieces) {
    // TODO: protocol error
    return;
  }
  shit("Peer %u received the 'have' message from %u for the piece %lu.", self_id, nbr->id, (unsigned long)index);
  unsigned int mask = 1 << (7 - (index & 7));
  if (!(nbr->bitfield[index >> 3] & mask)) {
    nbr->bitfield[index >> 3] |= mask;
    if (!(self_bitfield[index >> 3] & mask) && !nbr->num_want++) {
      neighbor_write(nbr, P2P_INT, NULL, 0);
    }
  }
  // TODO: maybe close connection
  // TODO: do we need to send NOINT?
}

static void
neighbor_read_bitfield(struct neighbor *nbr, char *buf)
{
  // TODO: handle padding
  memcpy(nbr->bitfield, buf, bitfield_size);
  // TODO: maybe close connection
  nbr->num_want = 0;
  for (unsigned int i = 0; i < num_pieces; ++i) {
    unsigned int mask = 1 << (7 - (i & 7));
    if ((nbr->bitfield[i >> 3] & ~self_bitfield[i >> 3] & mask) && !nbr->num_want++) {
      neighbor_write(nbr, P2P_INT, NULL, 0);
    }
  }
  if (!nbr->num_want)
    neighbor_write(nbr, P2P_NOINT, NULL, 0);
}

static void
neighbor_read_request(struct neighbor *nbr, char *buf)
{
  uint32_t i;
  memcpy(&i, buf, 4);
  i = ntohl(i);
  if (!(nbr->flags & NEIGHBOR_WFLOW))
    return; /* choked */
  if (i >= num_pieces)
    return; /* TODO: protocol error */
  unsigned int offset = i * piece_size;
  /* TODO: instrument read */
  neighbor_write(nbr, P2P_PIECE, the_file + offset,
      i == num_pieces - 1 ?
      the_file_size - offset : piece_size);
}

static void
neighbor_read_piece(struct neighbor *nbr, char *buf)
{
  uint32_t index;
  memcpy(&index, buf, 4);
  index = ntohl(index);
  if (index >= num_pieces)
    return; /* TODO: protocol error */
  unsigned int mask = 1 << (7 - (index & 7));
  if (self_bitfield[index >> 3] & mask)
    return; /* already have piece */
  unsigned int offset = index * piece_size;
  /* TODO: instrument write */
  memcpy(the_file + offset, buf + 4, index == num_pieces - 1 ?
      the_file_size - offset : piece_size);
  self_bitfield[index >> 3] |= mask;
  --self_rem_pieces;
  for (struct neighbor *oth = nbr_head; oth; oth = oth->next) {
    neighbor_write(oth, P2P_HAVE, buf, 4);
    if (oth->num_want && (oth->bitfield[index >> 3] & mask) && !--oth->num_want) {
      neighbor_write(oth, P2P_NOINT, NULL, 0);
    }
  }
  // TODO: request another if RFLOW
  if (nbr->flags & NEIGHBOR_RFLOW)
    neighbor_request(nbr);
}

static void
neighbor_read_header(struct neighbor *nbr, char *buf)
{
  uint32_t len;
  memcpy(&len, buf, 4);
  len = ntohl(len);
  err("message type %u len %u", (unsigned int)buf[4], len);
  switch (buf[4]) {
    case P2P_CHOKE:
      shit("Peer %u is choked by %u.", self_id, nbr->id);
      nbr->flags &= ~NEIGHBOR_RFLOW;
      break;
    case P2P_UNCHOKE:
      shit("Peer %u is unchoked by %u.", self_id, nbr->id);
      nbr->flags |= NEIGHBOR_RFLOW;
      neighbor_request(nbr);
      break;
    case P2P_INT:
      shit("Peer %u received the 'interested' message from %u.", self_id, nbr->id);
      nbr->flags |= NEIGHBOR_WINT;
      break;
    case P2P_NOINT:
      shit("Peer %u received the 'not interested' message from %u.", self_id, nbr->id);
      nbr->flags &= ~NEIGHBOR_WINT;
      break;
    case P2P_HAVE:
      nbr->rtarget = 4;
      nbr->on_read = neighbor_read_have;
      break;
    case P2P_BITFIELD:
      nbr->rtarget = bitfield_size;
      nbr->on_read = neighbor_read_bitfield;
      break;
    case P2P_REQUEST:
      nbr->rtarget = 4;
      nbr->on_read = neighbor_read_request;
      break;
    case P2P_PIECE:
      nbr->rtarget = 4 + piece_size;
      nbr->on_read = neighbor_read_piece;
      break;
    default:
      /* protocol error */
  }
}

static void
neighbor_read_callback(void *userdata)
{
  struct neighbor *nbr = userdata;
  ssize_t len = recv(nbr->sock, nbr->rbuf + nbr->rsize, nbr_rcap - nbr->rsize, 0);
  if (len < 0) {
    if (!(errno == EAGAIN || errno == EWOULDBLOCK)) {
      err_sys("recv");
      ev_detach(neighbor_read_callback, nbr);
    }
    return;
  }
  nbr->rsize += len;
  char *buf = nbr->rbuf;
  while (nbr->rsize >= nbr->rtarget) {
    unsigned int target = nbr->rtarget;
    nbr->rtarget = 0;
    nbr->on_read(nbr, buf);
    if (!nbr->rtarget) {
      nbr->rtarget = 5;
      nbr->on_read = neighbor_read_header;
    }
    buf += target;
    nbr->rsize -= target;
  }
  if (nbr->rsize)
    memmove(nbr->rbuf, buf, nbr->rsize);
}

static int
neighbor_ratecmp(const void *a, const void *b)
{
  const struct neighbor *const *ap = a, *const *bp = b;
  return (int)(*bp)->rrate - (*ap)->rrate;
}

static void
reselect_preferred_neighbors(void)
{
  unsigned int count = 0;
  struct neighbor *nbr;
  for (nbr = nbr_head; nbr; nbr = nbr->next) {
    if (nbr->flags & NEIGHBOR_WINT) {
      leaderboard[count++] = nbr;
    }
  }
  qsort(leaderboard, count, sizeof(*leaderboard), neighbor_ratecmp);
  for (unsigned int i = 0; i < count; ++i) {
    nbr = leaderboard[i];
    if (i < num_preferred_neighbors) {
      if (!(nbr->flags & NEIGHBOR_WFLOW)) {
        nbr->flags |= NEIGHBOR_WFLOW;
        neighbor_write(nbr, P2P_UNCHOKE, NULL, 0);
      }
    } else {
      if (nbr->flags & NEIGHBOR_WFLOW) {
        nbr->flags &= ~NEIGHBOR_WFLOW;
        neighbor_write(nbr, P2P_CHOKE, NULL, 0);
      }
    }
  }
  shit("Peer %u has the preferred neighbors X", self_id);
  // TODO
}

static void
optimistic_unchoke_neighbor(void)
{
  shit("Peer %u has the optimistically unchoked neighbor %u.", self_id, 69420);
}

struct quarantine
{
  struct neighbor *nbr;
  int sock;
  unsigned int wsize;
  unsigned int rsize;
  char wbuf[32];
  char rbuf[32];
};

static void
quarantine_write_callback(void *userdata)
{
  struct quarantine *q = userdata;
  ssize_t len = send(q->sock, q->wbuf + q->wsize, sizeof(q->wbuf) - q->wsize, 0);
  if (len < 0) {
    err_sys("send");
    ev_detach(quarantine_write_callback, q);
  } else if ((q->wsize += len) == sizeof(q->wbuf)) {
    ev_detach(quarantine_write_callback, q);
  }
}

static void
quarantine_read_callback(void *userdata)
{
  struct quarantine *q = userdata;
  ssize_t len = recv(q->sock, q->rbuf + q->rsize, sizeof(q->rbuf) - q->rsize, 0);
  err("%zd", len);
  if (len < 0) {
    err_sys("recv");
    goto destroy;
  }
  q->rsize += len;
  if (q->rsize < sizeof(q->rbuf))
    return; /* wait for more */

  /* check handshake */
  uint32_t id;
  memcpy(&id, q->rbuf + sizeof(magic), 4);
  id = ntohl(id);
  if (memcmp(magic, q->rbuf, sizeof(magic)) || (q->nbr && q->nbr->id != id)) {
    err("invalid handshake");
    goto destroy;
  }
  struct neighbor *nbr = q->nbr;
  if (!nbr) {
    for (nbr = nbr_head; nbr; nbr = nbr->next) {
      if (nbr->id == id) {
        if (nbr->sock != -1) {
          err("[~%u] already connected", id);
          goto destroy;
        }
        goto found;
      }
    }
    err("[~%u] unrecognized peer", id);
    goto destroy;
found:
    shit("Peer %u is connected from Peer %u", self_id, id);
    nbr->sock = q->sock;
  }
  if (q->wsize < sizeof(q->wbuf)) {
    neighbor_write_alloc(nbr, sizeof(q->wbuf) - q->wsize);
    memcpy(nbr->wbuf, q->wbuf + q->wsize, sizeof(q->wbuf) - q->wsize);
  }
  // TODO: initialize bitfield here?
  nbr->bitfield = calloc(1, bitfield_size);
  neighbor_write(nbr, P2P_BITFIELD, nbr->bitfield, bitfield_size);
  ev_register(nbr->sock, POLLIN, neighbor_read_callback, nbr);
destroy:
  ev_detach(NULL, q);
  free(q);
}

static void
quarantine(struct neighbor *nbr, int sock)
{
  fcntl(sock, F_SETFL, O_NONBLOCK);
  struct quarantine *q = calloc(1, sizeof(*q));
  /* send handshake */
  uint32_t my_id_be = htonl(self_id);
  q->nbr = nbr;
  q->sock = sock;
  memcpy(q->wbuf, magic, sizeof(magic));
  memcpy(q->wbuf + sizeof(magic), &my_id_be, 4);
  ev_register(q->sock, POLLOUT, quarantine_write_callback, q);
  ev_register(q->sock, POLLIN, quarantine_read_callback, q);
}

static void
accept_callback(void *_unused)
{
  int sock = accept(self_sock, NULL, NULL);
  if (sock != -1) {
    quarantine(NULL, sock);
  } else if (!(errno == EAGAIN || errno == EWOULDBLOCK || errno == ECONNABORTED)) {
    /* give up, stop accepting new connections */
    err_sys("accept");
    ev_detach(accept_callback, NULL);
  }
}

static void
peerinfo_load(const char *filename)
{
  FILE *f = fopen(filename, "r");
  if (!f) {
    err_sys("could not open '%s'", filename);
    exit(1);
  }
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
    struct neighbor *tail = NULL, *nbr;
    for (nbr = nbr_head; nbr; tail = nbr, nbr = nbr->next)
      if (nbr->id == id)
        goto abort;
    nbr = calloc(1, sizeof(*nbr));
    nbr->id = id;
    nbr->sock = -1;
    size_t hostlen = strlen(fhst);
    memcpy(nbr->host, fhst, hostlen < sizeof(nbr->host) ? hostlen : sizeof(nbr->host) - 1);
    nbr->port = port;
    if (!self_port)
      nbr->flags |= NEIGHBOR_CONN;
    nbr->rbuf = malloc(nbr_rcap);
    nbr->rtarget = 5;
    nbr->on_read = neighbor_read_header;
    nbr->prev = tail;
    nbr->next = NULL;
    if (tail)
      tail->next = nbr;
    else
      nbr_head = nbr;
    ++nbr_count;
  }
  fclose(f);
  if (!self_port) {
    err("could not find self configuration");
    exit(1);
  }
  return;
abort:
  err("could not parse '%s'", filename);
  exit(1);
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
  self_reqfield = calloc(1, bitfield_size);
  if (self_has_file) {
    /* TODO: handle padding */
    memset(self_bitfield, -1, bitfield_size);
    memset(self_reqfield, -1, bitfield_size);
  } else {
    self_rem_pieces = num_pieces;
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
  if (self_sock == -1) {
    err_sys("socket");
    exit(1);
  }
  setsockopt(self_sock, IPPROTO_IPV6, IPV6_V6ONLY, &cero, sizeof(cero));
  setsockopt(self_sock, SOL_SOCKET, SO_REUSEADDR, &uno, sizeof(uno));
  if (bind(self_sock, (const struct sockaddr *)&addr, sizeof(addr))) {
    err_sys("could not bind to port %u", (unsigned int)self_port);
    exit(1);
  }
  if (listen(self_sock, 16)) {
    err_sys("listen");
    exit(1);
  }
  fcntl(self_sock, F_SETFL, O_NONBLOCK);
  ev_register(self_sock, POLLIN, accept_callback, NULL);

  /* open log file */
  snprintf(filename, sizeof(filename), "log_peer_%u.log", self_id);
  logfd = 2; //open(filename, O_WRONLY | O_CREAT | O_TRUNC, 0666);
  if (logfd == -1) {
    err_sys("failed to open '%s'", filename);
    exit(1);
  }

  /* create the directory if necessary */
  snprintf(filename, sizeof(filename), "peer_%u", self_id);
  if (mkdir(filename, 0777) && errno != EEXIST) {
    err_sys("failed to create directory '%s'", filename);
    exit(1);
  }

  /* open the file */
  snprintf(filename, sizeof(filename), "peer_%u/%s", self_id, the_file_name);
  int the_fd = open(filename, self_has_file ? O_RDONLY : (O_RDWR | O_CREAT), 0666);
  if (the_fd == -1) {
    err_sys("failed to open '%s'", filename);
    exit(1);
  }
  the_file = mmap(NULL, the_file_size, self_has_file ?
      PROT_READ : PROT_READ | PROT_WRITE, MAP_SHARED, the_fd, 0);
  if (the_file == MAP_FAILED) {
    err_sys("failed to mmap '%s'", filename);
    exit(1);
  }
  close(the_fd);

  /* connect neighbors */
  usleep(100000); /* TODO: currently sleeping for 100ms */
  for (struct neighbor *nbr = nbr_head; nbr; nbr = nbr->next) {
    if (nbr->flags & NEIGHBOR_CONN) {
      neighbor_connect(nbr);
    }
  }

  /* event loop */
  struct timespec ts;
  unsigned int unchoke_at = 0;
  unsigned int optimistic_unchoke_at = 0;
  unsigned int now, timeout, timeout2;
  while (ev_count) {
    clock_gettime(CLOCK_MONOTONIC, &ts);
    now = ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
    timeout = unchoke_at - now;
    if (timeout > unchoking_interval) {
      reselect_preferred_neighbors();
      unchoke_at = now + unchoking_interval;
      timeout = unchoking_interval;
    }
    timeout2 = optimistic_unchoke_at - now;
    if (timeout2 > optimistic_unchoking_interval) {
      optimistic_unchoke_neighbor();
      optimistic_unchoke_at = now + optimistic_unchoking_interval;
      timeout2 = optimistic_unchoking_interval;
    }
    if (timeout2 < timeout)
      timeout = timeout2;
    int count = poll(ev_pollfds, ev_count, timeout);
    if (count < 0) {
      err_sys("poll");
      break;
    }
    ev_next = 0;
    while (ev_next < ev_count) {
      unsigned int i = ev_next++;
      int revents = ev_pollfds[i].revents;
      if (!revents)
        continue;
      //err("%d %d %d %d", ev_pollfds[i].fd, ev_pollfds[i].events, ev_pollfds[i].revents, ev_handlers[i].callback == neighbor_read_callback);
      ev_pollfds[i].revents = 0;
      ev_handlers[i].callback(ev_handlers[i].userdata);
    }
  }

  return 0;
}
