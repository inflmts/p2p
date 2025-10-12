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
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#define P2P_CHOKE     0
#define P2P_UNCHOKE   1
#define P2P_WANT      2
#define P2P_NOWANT    3
#define P2P_HAVE      4
#define P2P_BITFIELD  5
#define P2P_REQUEST   6
#define P2P_PIECE     7

static const char magic[28] = "P2PFILESHARINGPROJ\0\0\0\0\0\0\0\0\0\0";
static const int uno = 1;

static void __attribute__((format(printf, 1, 2)))
err(const char *format, ...)
{
  va_list ap;
  fputs("error: ", stderr);
  va_start(ap, format);
  vfprintf(stderr, format, ap);
  va_end(ap);
  fputc('\n', stderr);
}

#define err_sys(format, ...) err(format ": %s", ##__VA_ARGS__, strerror(errno))

/* Configuration */

struct config
{
  unsigned int num_preferred_neighbors;
  unsigned int unchoking_interval;
  unsigned int optimistic_unchoking_interval;
  char file_name[1024];
  unsigned int file_size;
  unsigned int piece_size;
  unsigned int num_pieces;
  unsigned int bitfield_size;
};

static int
config_parse_uint(const char *s, unsigned int *dest)
{
  const char *t = s;
  unsigned int value = 0;
  do {
    if (*t < '0' || *t > '9')
      return -1;
    value = value * 10 + (*t - '0');
  } while (*(++t));
  *dest = value;
  return 0;
}

static int
config_load(struct config *cfg, const char *filename)
{
  FILE *f = fopen(filename, "r");
  if (!f) {
    err_sys("could not open '%s'", filename);
    return -1;
  }
  cfg->num_preferred_neighbors = 3;
  cfg->unchoking_interval = 5;
  cfg->optimistic_unchoking_interval = 10;
  strcpy(cfg->file_name, "thefile");
  cfg->file_size = 0;
  cfg->piece_size = 16384;
  static const char *delim = "\r\n ";
  char line[1024];
  while (fgets(line, sizeof(line), f)) {
    char *key = strtok(line, delim);
    if (!key || key[0] == '#') {
      /* empty line */
      continue;
    }
    char *value = strtok(NULL, delim);
    if (!value) {
      goto fail;
    }
    if (!strcmp(key, "NumberOfPreferredNeighbors")) {
      if (config_parse_uint(value, &cfg->num_preferred_neighbors))
        goto fail;
    } else if (!strcmp(key, "UnchokingInterval")) {
      if (config_parse_uint(value, &cfg->unchoking_interval))
        goto fail;
    } else if (!strcmp(key, "OptimisticUnchokingInterval")) {
      if (config_parse_uint(value, &cfg->optimistic_unchoking_interval))
        goto fail;
    } else if (!strcmp(key, "FileName")) {
      strcpy(cfg->file_name, value);
    } else if (!strcmp(key, "FileSize")) {
      if (config_parse_uint(value, &cfg->file_size))
        goto fail;
    } else if (!strcmp(key, "PieceSize")) {
      if (config_parse_uint(value, &cfg->piece_size))
        goto fail;
    } else {
      goto fail;
    }
  }
  fclose(f);
  cfg->num_pieces = (cfg->file_size + cfg->piece_size - 1) / cfg->piece_size;
  cfg->bitfield_size = (cfg->num_pieces + 7) >> 3;
  return 0;
fail:
  fclose(f);
  err("failed to parse '%s'", filename);
  return -1;
}

/* Peer Info */

struct peerinfo
{
  unsigned int id;
  uint16_t port;
  char host[240];
  struct peerinfo *next;
};

static void
peerinfo_free(struct peerinfo *pi)
{
  while (pi) {
    struct peerinfo *next = pi->next;
    free(pi);
    pi = next;
  }
}

static struct peerinfo *
peerinfo_load(const char *filename, unsigned int my_id, uint16_t *my_port,
    int *i_have_file)
{
  FILE *f = fopen(filename, "r");
  if (!f) {
    err_sys("could not open '%s'", filename);
    return NULL;
  }
  struct peerinfo *pi = NULL;
  struct peerinfo **lastpi = &pi;
  char line[240];
  while (fgets(line, sizeof(line), f)) {
    char *fields[4];
    unsigned int id, port, has_file;
    if (!(fields[0] = strtok(line, "\r\n ")) ||
        !(fields[1] = strtok(NULL, "\r\n ")) ||
        !(fields[2] = strtok(NULL, "\r\n ")) ||
        !(fields[3] = strtok(NULL, "\r\n ")) ||
        config_parse_uint(fields[0], &id) ||
        config_parse_uint(fields[2], &port)) {
      goto abort;
    }
    if (!strcmp(fields[3], "0")) {
      has_file = 0;
    } else if (!strcmp(fields[3], "1")) {
      has_file = 1;
    } else {
      goto abort;
    }
    if (id == my_id) {
      *my_port = port;
      *i_have_file = has_file;
      return pi;
    }
    *lastpi = malloc(sizeof(struct peerinfo));
    (*lastpi)->id = id;
    (*lastpi)->port = port;
    strcpy((*lastpi)->host, fields[1]);
    lastpi = &(*lastpi)->next;
  }
  err("could not find peer ID %u", my_id);
abort:
  peerinfo_free(pi);
  return NULL;
}

/* Event Loop */

typedef int (*ev_callback)(void *);

struct ev_handler
{
  ev_callback callback;
  void *userdata;
};

static unsigned int ev_cap;
static unsigned int ev_count;
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
}

/* Neighbor */

#define NEIGHBOR_WRITING  1
#define NEIGHBOR_WFLOW    2
#define NEIGHBOR_WWANT    4
#define NEIGHBOR_RFLOW    8
#define NEIGHBOR_RWANT    16

struct self
{
  unsigned int id;
  int sock;
  unsigned char *bitfield;
  int log;
  char *file;
  struct config *cfg;
  struct neighbor *nbr_head;
  struct neighbor *nbr_tail;
};

struct neighbor
{
  unsigned int id;
  int sock;
  unsigned char *bitfield;
  char *wbuf;
  char *rbuf;
  int flags;
  unsigned int wcap;
  unsigned int wsize;
  unsigned int rcap;
  unsigned int rsize;
  unsigned int rtarget;
  void (*on_read)(struct neighbor *, char *);
  struct self *me;
  struct config *cfg;
  struct neighbor *prev;
  struct neighbor *next;
};

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

static void neighbor_read_handshake(struct neighbor *nbr, char *buf);
static void neighbor_write_alloc(struct neighbor *nbr, unsigned int size);
static void neighbor_flush(struct neighbor *nbr);
static int neighbor_read_callback(void *userdata);

static struct neighbor *
neighbor_create(struct self *me, unsigned int id, int sock)
{
  fcntl(sock, F_SETFL, O_NONBLOCK);

  struct neighbor *nbr = malloc(sizeof(struct neighbor));
  nbr->id = id;
  nbr->sock = sock;
  nbr->bitfield = NULL;
  nbr->wbuf = NULL;
  nbr->rbuf = NULL;
  nbr->flags = 0;
  nbr->wcap = 0;
  nbr->wsize = 0;
  nbr->rcap = 0;
  nbr->rsize = 0;
  nbr->rtarget = 32;
  nbr->on_read = neighbor_read_handshake;
  nbr->me = me;
  nbr->cfg = me->cfg;
  if ((nbr->prev = me->nbr_tail))
    nbr->prev->next = nbr;
  else
    me->nbr_head = nbr;
  me->nbr_tail = nbr;
  nbr->next = NULL;

  /* send handshake */
  neighbor_write_alloc(nbr, 32);
  uint32_t my_id_be = htonl(me->id);
  memcpy(nbr->wbuf, magic, 28);
  memcpy(nbr->wbuf + 28, &my_id_be, 4);
  neighbor_flush(nbr);

  /* start reading */
  ev_register(nbr->sock, POLLIN, neighbor_read_callback, nbr);
  return nbr;
}

static int
neighbor_connect(const char *host, uint16_t port)
{
  struct addrinfo *ai, *ai_head;
  int res = getaddrinfo(host, NULL, NULL, &ai_head);
  if (!res) {
    err("failed to resolve '%s': %s", host, gai_strerror(res));
    return -1;
  }
  uint16_t port_be = htons(port);
  for (ai = ai_head; ai; ai = ai->ai_next) {
    if (ai->ai_family == AF_INET) {
      ((struct sockaddr_in *)ai->ai_addr)->sin_port = port_be;
    } else { /* AF_INET6 */
      ((struct sockaddr_in6 *)ai->ai_addr)->sin6_port = port_be;
    }
    int sock = socket(ai->ai_family, SOCK_STREAM, 0);
    if (sock == -1) {
      err_sys("socket");
      goto abort;
    }
    if (!connect(sock, ai->ai_addr, ai->ai_addrlen)) {
      freeaddrinfo(ai_head);
      return sock;
    }
    close(sock);
  }
abort:
  freeaddrinfo(ai_head);
  return -1;
}

static int
neighbor_write_callback(void *userdata)
{
  struct neighbor *nbr = userdata;
  ssize_t bytes_sent = send(nbr->sock, nbr->wbuf, nbr->wsize, 0);
  if (bytes_sent < 0) {
    err_sys("send");
    nbr->flags &= ~NEIGHBOR_WRITING;
    return 0;
  }
  nbr->wsize -= bytes_sent;
  if (!nbr->wsize) {
    nbr->flags &= ~NEIGHBOR_WRITING;
    return 0;
  }
  memmove(nbr->wbuf, nbr->wbuf + bytes_sent, nbr->wsize);
  return 1;
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
neighbor_flush(struct neighbor *nbr)
{
  if (!(nbr->flags & NEIGHBOR_WRITING)) {
    ev_register(nbr->sock, POLLOUT, neighbor_write_callback, nbr);
    nbr->flags |= NEIGHBOR_WRITING;
  }
}

static int
neighbor_read_callback(void *userdata)
{
  struct neighbor *nbr = userdata;
  if (nbr->rtarget > nbr->rcap) {
    nbr->rcap = nbr->rtarget;
    nbr->rbuf = realloc(nbr->rbuf, nbr->rcap);
  }
  ssize_t len = recv(nbr->sock, nbr->rbuf + nbr->rsize, nbr->rcap - nbr->rsize, 0);
  if (len < 0) {
    if (errno == EAGAIN || errno == EWOULDBLOCK)
      return 1;
    err_sys("recv");
    return 0;
  }
  nbr->rsize += len;
  char *buf = nbr->rbuf;
  while (nbr->rsize >= nbr->rtarget) {
    unsigned int target = nbr->rtarget;
    nbr->on_read(nbr, buf);
    buf += target;
    nbr->rsize -= target;
  }
  if (nbr->rsize)
    memmove(nbr->rbuf, buf, nbr->rsize);
  return 1;
}

static void neighbor_read_message(struct neighbor *nbr, char *buf);

static void
neighbor_read_handshake(struct neighbor *nbr, char *buf)
{
  uint32_t id;
  memcpy(&id, buf + sizeof(magic), 4);
  id = ntohl(id);
  if (memcmp(magic, buf, sizeof(magic)) || (nbr->id && nbr->id != id)) {
    /* invalid */
    return;
  }
  if (!nbr->id) {
    nbr->id = id;
    self_log(me, "Peer %u is connected from Peer %u", nbr->self->id, id);
  }
  nbr->rtarget = 5;
  nbr->on_read = neighbor_read_message;
}

static void
neighbor_read_have(struct neighbor *nbr, char *buf)
{
  uint32_t index;
  memcpy(&index, buf, 4);
  index = ntohl(index);
  // TODO: update bitmap
  nbr->rtarget = 5;
  nbr->on_read = neighbor_read_message;
}

static void
neighbor_read_bitfield(struct neighbor *nbr, char *buf)
{
  memcpy(nbr->bitfield, buf, nbr->cfg->bitfield_size);
  // TODO: send WANT/NOWANT
  nbr->rtarget = 5;
  nbr->on_read = neighbor_read_message;
}

static void
neighbor_read_request(struct neighbor *nbr, char *buf)
{
  uint32_t index;
  memcpy(&index, buf, 4);
  index = ntohl(index);
  if (nbr->flags & NEIGHBOR_WFLOW) {
    // TODO: validate
    unsigned int offset = index * nbr->cfg->piece_size;
    neighbor_write(nbr, P2P_PIECE, nbr->self->file + offset,
        index == nbr->cfg->num_pieces - 1 ?
        nbr->cfg->file_size - offset : nbr->cfg->piece_size);
  }
  nbr->rtarget = 5;
  nbr->on_read = neighbor_read_message;
}

static void
neighbor_read_piece(struct neighbor *nbr, char *buf)
{
  // TODO: store the piece
  // TODO: update bitmap
  // TODO: send HAVE to all
  // TODO: send NOWANT
  nbr->rtarget = 5;
  nbr->on_read = neighbor_read_message;
}

static void
neighbor_read_message(struct neighbor *nbr, char *buf)
{
  uint32_t len;
  memcpy(&len, buf, 4);
  len = ntohl(len);
  switch (buf[4]) {
    case P2P_CHOKE:
      nbr->flags &= ~NEIGHBOR_RFLOW;
      break;
    case P2P_UNCHOKE:
      nbr->flags |= NEIGHBOR_RFLOW;
      break;
    case P2P_WANT:
      nbr->flags |= NEIGHBOR_WWANT;
      break;
    case P2P_NOWANT:
      nbr->flags &= ~NEIGHBOR_WWANT;
      break;
    case P2P_HAVE:
      nbr->rtarget = 4;
      nbr->on_read = neighbor_read_have;
      break;
    case P2P_BITFIELD:
      nbr->rtarget = nbr->cfg->bitfield_size;
      nbr->on_read = neighbor_read_bitfield;
      break;
    case P2P_REQUEST:
      nbr->rtarget = 4;
      nbr->on_read = neighbor_read_request;
      break;
    case P2P_PIECE:
      nbr->rtarget = 0;
      nbr->on_read = neighbor_read_piece;
      break;
    default:
      /* protocol error */
  }
}

/* Self */

static void
self_free(struct self *me)
{
  if (me->sock != -1)
    close(me->sock);
  free(me->bitfield);
  if (me->log != -1)
    close(me->log);
  if (me->file != MAP_FAILED)
    munmap(me->file, me->cfg->file_size);
}

static int self_accept_callback(void *userdata);

static int
self_init(struct self *me, struct config *cfg, unsigned int id, uint16_t port,
    int has_file)
{
  char filename[1056];
  me->id = id;
  me->sock = -1;
  me->bitfield = NULL;
  me->log = -1;
  me->file = MAP_FAILED;
  me->cfg = cfg;
  me->nbr_head = NULL;
  me->nbr_tail = NULL;

  /* open log file */
  snprintf(filename, sizeof(filename), "log_peer_%u.log", id);
  me->log = open(filename, O_WRONLY | O_CREAT | O_TRUNC, 0666);
  if (me->log == -1) {
    err_sys("failed to open '%s'", filename);
    goto abort;
  }

  /* open the file */
  snprintf(filename, sizeof(filename), "peer_%u/%s", id, cfg->file_name);
  int fd = open(filename, O_RDWR | O_CREAT, 0666);
  if (fd == -1) {
    err_sys("failed to open '%s'", filename);
    goto abort;
  }
  me->file = mmap(NULL, cfg->file_size, PROT_READ, MAP_SHARED, fd, 0);
  if (me->file == MAP_FAILED) {
    err_sys("failed to mmap '%s'", filename);
    close(fd);
    goto abort;
  }
  close(fd);

  /* start server */
  me->sock = socket(AF_INET, SOCK_STREAM, 0);
  if (me->sock == -1) {
    err_sys("socket");
    goto abort;
  }
  setsockopt(me->sock, SOL_SOCKET, SO_REUSEADDR, &uno, sizeof(uno));
  struct sockaddr_in addr;
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  addr.sin_addr.s_addr = 0;
  if (bind(me->sock, (const struct sockaddr *)&addr, sizeof(addr))) {
    err_sys("bind");
    goto abort;
  }
  if (listen(me->sock, 16)) {
    err_sys("listen");
    goto abort;
  }
  fcntl(me->sock, F_SETFL, O_NONBLOCK);
  ev_register(me->sock, POLLIN, self_accept_callback, me);
  return 0;

abort:
  self_free(me);
  return -1;
}

static void __attribute__((format(printf, 2, 3)))
self_log(struct self *me, const char *format, ...)
{
  char buf[2048];
  time_t tim = time(NULL);
  struct tm *tm = localtime(&tim);
  int len = strftime(buf, sizeof(buf), "%y-%m-%d %I:%M:%S %p: ", tm);
  va_list ap;
  va_start(ap, format);
  len += vsnprintf(buf + len, sizeof(buf) - len, format, ap) + 1;
  va_end(ap);
  if (len > sizeof(buf))
    len = sizeof(buf);
  buf[len - 1] = '\n';
  write(me->log, buf, len);
}

static int
self_accept_callback(void *userdata)
{
  struct self *me = userdata;
  int sock = accept(me->sock, NULL, NULL);
  if (sock == -1) {
    if (errno == EAGAIN || errno == EWOULDBLOCK)
      return 1;
    err_sys("accept");
    return 0;
  }
  neighbor_create(me, 0, sock);
  return 1;
}

/* Main */

int main(int argc, char **argv)
{
  if (argc != 2) {
    fprintf(stderr, "usage: %s <peer_id>\n", argv[0]);
    return 2;
  }

  unsigned int self_id;
  uint16_t self_port;
  int self_has_file;
  struct config cfg;
  struct peerinfo *pi_head, *pi;
  struct self me;

  if (config_parse_uint(argv[1], &self_id))
    return 2;

  if (config_load(&cfg, "Common.cfg"))
    return 1;

  pi_head = peerinfo_load("PeerInfo.cfg", self_id, &self_port, &self_has_file);
  if (!pi_head)
    return 1;

  if (self_init(&me, &cfg, self_id, self_port, self_has_file))
    return 1;

  for (pi = pi_head; pi; pi = pi->next) {
    int sock = neighbor_connect(pi->host, pi->port);
    if (sock == -1)
      return -1;
    self_log(&me, "Peer %u makes a connection to Peer %u", me.id, pi->id);
    neighbor_create(&me, pi->id, sock);
  }

  while (ev_count) {
    int count = poll(ev_pollfds, ev_count, -1);
    if (count < 0) {
      err_sys("poll");
      break;
    }
    int i = 0;
    while (count) {
      int revents = ev_pollfds[i].revents;
      if (!revents || ev_handlers[i].callback(ev_handlers[i].userdata)) {
        ++i;
        continue;
      }
      --ev_count;
      --count;
      if (i != ev_count) {
        memcpy(&ev_pollfds[i], &ev_pollfds[ev_count], sizeof(*ev_pollfds));
        memcpy(&ev_handlers[i], &ev_handlers[ev_count], sizeof(*ev_handlers));
      }
    }
  }

  free(ev_pollfds);
  free(ev_handlers);
  return 0;
}
