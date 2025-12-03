#include <errno.h>
#include <fcntl.h>
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
# include <sys/mman.h>
# include <sys/socket.h>
# include <unistd.h>
#else
# include <io.h>
# include <winsock2.h>
# include <ws2tcpip.h>
# define mkdir(path, mode) mkdir(path)
# define fcntl(...) ((void)0)
# define ftruncate(...) chsize(__VA_ARGS__)
# define poll(...) WSAPoll(__VA_ARGS__)
# define MAP_FAILED NULL
# define PROT_READ 1
# define PROT_WRITE 2
# define mmap(...) NULL
# define mrand48() rand()
# define srand48(seed) srand(seed)
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

static unsigned int self_id;
static int self_sock = -1;
static unsigned char *self_have, *self_pend;
static unsigned int self_num_pieces;
static int logfd = -1;
static char *the_file = MAP_FAILED;

//globals for checking full list
static unsigned int *expected_peer_ids = NULL;
static int expected_peer_count = 0;


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
    return ntohl(value);   /* change off of htonl() for windows compatibility*/
}

//helper functions for windows
#ifdef _WIN32
static void make_socket_nonblocking(int sock) {
    u_long mode = 1;
    if (ioctlsocket(sock, FIONBIO, &mode) != 0) {
        die("ioctlsocket(FIONBIO) failed");
    }
}
#else
static void make_socket_nonblocking(int sock) {
    int flags = fcntl(sock, F_GETFL, 0);
    fcntl(sock, F_SETFL, flags | O_NONBLOCK);
}
#endif
//helper functions for windows


#define CONN_WINT   4
#define CONN_RFLOW  8
#define CONN_WPREF  32
#define CONN_WOPT   64
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
static int board_cap = 0; //capacity of board

static void
conn_close(struct conn *c)
{
    if (!c) return;
    msg("closing connection to peer %u (fd %d)", c->id, c->sock);

#ifdef _WIN32
    closesocket(c->sock);
#else
    close(c->sock);
#endif

    /* remove from linked list */
    if (conn_head == c) {
        conn_head = c->next;
    } else {
        struct conn *p = conn_head;
        while (p && p->next != c) p = p->next;
        if (p) p->next = c->next;
    }

    /* free resources */
    free(c->have);
    free(c->wbuf);
    free(c->rbuf);

    free(c);

    --conn_count;
}

static void
conn_write(struct conn *c, const void *buf, unsigned int size)
{
    //debug
    //msg("conn_write called for %u: size=%u (wsize before=%u)", c->id, size, c->wsize);
//data being queued

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


//sender helpers


static void
send_have(struct conn *c, uint32_t index)
{
    conn_write_header(c, 4, P2P_HAVE);
    conn_write_uint32(c, index);
    msg("queued 'have' for piece %u to %u (wsize=%u).", index, c->id, c->wsize);
}

static void
send_piece_msg(struct conn *c, uint32_t index, const char *data, unsigned int len)
{
    conn_write_header(c, 4 + len, P2P_PIECE);
    conn_write_uint32(c, index);
    conn_write(c, data, len);
    msg("queued 'piece' %u (%u bytes) to %u (wsize=%u).", index, len, c->id, c->wsize);
}
//end of new helper functions



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
    if (!(self_pend[index >> 3] & mask)) {
        c->num_want = 1;        // mark the peer has something we want
        c->flags |= CONN_WINT;  // mark we are interested
    }
}

static void
conn_read_bitfield(struct conn *c, char *buf)
{
    /*TODO*/
    memcpy(c->have, buf, bitfield_size);

    /* compute counts and whether we want anything from this peer */
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

    msg("received the 'bitfield' message from %u. peer has %u pieces; we want %u pieces.",
        c->id, c->num_pieces, c->num_want);

    if (c->num_want) {
        conn_write_header(c, 0, P2P_INT);
        msg("sent 'interested' to %u.", c->id);
        conn_request(c);
    } else {
        conn_write_header(c, 0, P2P_NOINT);
        msg("sent 'not interested' to %u.", c->id);
    }
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
        for (c = conn_head; c; c = c->next)
          if ((c->have[c->req >> 3] & mask) && !c->num_want++)
            conn_write_header(c, 0, P2P_INT);
        for (c = conn_head; c; c = c->next)
          conn_request(c);
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
conn_read_handshake(struct conn *c, char *buf) //new read handshake
{
    // string validation
    if (memcmp(buf, magic, 18) != 0) {
        die("Invalid handshake magic from fd %d", c->sock);
    }

    for (int i = 18; i < 28; ++i) {
        if (buf[i] != 0) {
            die("Invalid handshake zero bytes from fd %d", c->sock);
        }
    }

    uint32_t remote_id = read_uint32(buf + 28);

    // assign id if unknown
    if (c->id == 0) {
        c->id = remote_id;
        msg("Peer %u is connected from Peer %u.", self_id, c->id);
    }
    else if (remote_id != c->id) {
        die("Handshake remote ID mismatch: expected %u, got %u",
            c->id, remote_id);
    }

    msg("HANDSHAKE RECEIVED: Peer %u <-> Peer %u", self_id, c->id);

    if (self_num_pieces > 0) {
        conn_write_header(c, bitfield_size, P2P_BITFIELD);
        conn_write(c, self_have, bitfield_size);
        msg("queued bitfield to %u (bitcount=%u).",
            c->id, self_num_pieces);
    }
    else {
        msg("Skipping bitfield to %u (have no pieces).", c->id);
    }
}


static void conn_handle_write(struct conn *c) {
    if (c->wsize == 0) {
        //msg("conn_handle_write called for %u but wsize=0", c->id);
        return;
    }

    //msg("conn_handle_write: attempting to send %u bytes to %u", c->wsize, c->id);
    ssize_t len = send(c->sock, c->wbuf, c->wsize, 0);
    if (len < 0) {
#ifdef _WIN32
        int err = WSAGetLastError();
        msg("send() returned error %d for %u", err, c->id);
        if (err == WSAEWOULDBLOCK) return;
#else
        msg("send() returned errno=%d (%s) for %u", errno, strerror(errno), c->id);
        if (errno == EAGAIN || errno == EWOULDBLOCK) return;
#endif
        die_sys("cannot send to %u", c->id);
    }
    //msg("conn_handle_write: actually sent %zd bytes to %u", len, c->id);
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

#ifdef _WIN32
    int rc = recv(c->sock, c->rbuf + c->rsize, c->rcap - c->rsize, 0);
    if (rc == SOCKET_ERROR) {
        int wsaerr = WSAGetLastError();
        msg("recv() would block/error %d on %u (rsize=%u, rwant=%u).", wsaerr, c->id, c->rsize, c->rwant);
        if (wsaerr == WSAEWOULDBLOCK) return;
        msg("recv error on %u: WSA error %d -- closing connection INSTEAD OF DYING", c->id, wsaerr);
        conn_close(c);
        return;
    }
    ssize_t len = rc;
#else
    ssize_t len = recv(c->sock, c->rbuf + c->rsize, c->rcap - c->rsize, 0);
    if (len < 0) {
        msg("recv() returned errno=%d (%s) on %u (rsize=%u, rwant=%u).", errno, strerror(errno), c->id, c->rsize, c->rwant);
        if (errno == EAGAIN || errno == EWOULDBLOCK) return;
        die_sys("recv error on %u", c->id);
    }
#endif

    //msg("conn_handle_read: recv returned %zd bytes for %u (rsize before=%u rwant=%u)", len, c->id, c->rsize, c->rwant);

    if (len == 0) {
        /* orderly shutdown from peer */
        msg("peer %u performed orderly shutdown.", c->id);
        if (self_num_pieces != num_pieces)
            die("was disconnected by %u without the complete file", c->id);
        exit(0);
    }

    //dont zero out rsize
    c->rsize += len;
    //msg("conn_handle_read: rsize now %u for %u (rcap=%u).", c->rsize, c->id, c->rcap);


    //parsing loop
    char *next = c->rbuf, *cur;
    while (c->rsize >= c->rwant) {
        void (*on_read)(struct conn *c, char *buf) = c->on_read;
        //msg("conn_handle_read: about to call on_read (%p) for %u with %u bytes", (void*)on_read, c->id, c->rwant);
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

//conn add changes
static struct conn *
conn_add(int sock, unsigned int id)
{
    struct conn *c = calloc(1, sizeof(struct conn));
    if (!c) die("calloc failed in conn_add");
    c->have = calloc(1, bitfield_size);
    if (!c->have) die("calloc failed in conn_add->have");
    c->wbuf = malloc(32);
    c->rbuf = malloc(32);
    c->sock = sock;
    c->id = id;
    c->wcap = 32;
    c->rcap = 32;
    c->wsize = 0;
    c->rsize = 0;



    c->rwant = 32;               //handshake first I think
    c->on_read = conn_read_handshake;
    c->next = conn_head;
    conn_head = c;
    ++conn_count;

    //testing
    if (conn_count > board_cap) {
        int newcap = conn_count;
        struct conn **tmp = realloc(board, newcap * sizeof(*board));
        if (!tmp) die("realloc failed for board");
        /* zero the newly allocated slots so the array is in a known state */
        if (newcap > board_cap)
            memset(tmp + board_cap, 0, (newcap - board_cap) * sizeof(*board));
        board = tmp;
        board_cap = newcap;
    }


    msg("queued handshake to %u (will be sent when socket writable).", c->id);

    make_socket_nonblocking(sock);

    //sending handshake logic
    conn_write(c, magic, sizeof(magic));
    conn_write_uint32(c, self_id);

    msg("HANDSHAKE SENT to %u: magic='%.*s' peer_id=%u",
        c->id, 28, c->wbuf + c->wsize - 32,
        read_uint32(c->wbuf + c->wsize - 4));

    conn_handle_write(c); // try to flush handshake to socket

    return c;
}


static void
conn_bind(uint16_t port, int has_file)
{
    struct sockaddr_in6 addr = {0};
    addr.sin6_family = AF_INET6;
    addr.sin6_port = htons(port);
    addr.sin6_addr = in6addr_any;

    self_sock = socket(AF_INET6, SOCK_STREAM, 0);
    make_socket_nonblocking(self_sock); //maybe take out

    //debugging
    //msg("DEBUG: About to bind on port %u", port);

    if (self_sock == -1)
        die_sys("cannot create server socket");

    int opt = 1;
    setsockopt(self_sock, SOL_SOCKET, SO_REUSEADDR, (const char *)&opt, sizeof(opt));
#ifdef IPV6_V6ONLY
    setsockopt(self_sock, IPPROTO_IPV6, IPV6_V6ONLY, (const void *)&cero, sizeof(cero));
#endif


    if (bind(self_sock, (struct sockaddr *)&addr, sizeof(addr)))
        die_sys("cannot bind to port %u", port);

    if (listen(self_sock, 16))
        die_sys("cannot listen");

    msg("Peer %u listening on port %u", self_id, port);

    // now handle the file
    char filename[1024];
    snprintf(filename, sizeof(filename), "peer_%u/%s", self_id, the_file_name);

//#ifdef _WIN32
    // Just windows
    int fd = open(filename, has_file ? O_RDONLY : (O_RDWR | O_CREAT), 0666);
    if (fd == -1) {
        die_sys("failed to open '%s'", filename);
    }

    if (!has_file) {
        // Resize file to correct size
        if (_chsize(fd, the_file_size) != 0) {
            die_sys("failed to resize '%s'", filename);
        }
    } else {
        struct _stat st;
        if (_fstat(fd, &st) || st.st_size != the_file_size) {
            die("expected '%s' to be %u bytes, got %lld",
                filename, the_file_size, (long long)st.st_size);
        }
    }
//mapping so you can actually see the file fixing hte hex 0 error
    DWORD mapProt = has_file ? PAGE_READONLY : PAGE_READWRITE;
    HANDLE fm = CreateFileMapping((HANDLE)_get_osfhandle(fd),
                                  NULL,
                                  mapProt,
                                  0,
                                  the_file_size,
                                  NULL);
    if (!fm) die_sys("CreateFileMapping failed");

    DWORD viewProt = has_file ? FILE_MAP_READ : (FILE_MAP_READ | FILE_MAP_WRITE);
    the_file = MapViewOfFile(fm, viewProt, 0, 0, the_file_size);
    if (!the_file) die_sys("MapViewOfFile failed");

    CloseHandle(fm);
    close(fd);

    self_have = calloc(1, bitfield_size*2);
    if (!self_have) {
        die("calloc failed");
    }
    self_pend = self_have + bitfield_size;
    if (has_file) {
        memset(self_have, 0xFF, bitfield_size);

        if (num_pieces & 7) { //check
            unsigned int valid_bits = num_pieces & 7;
            unsigned char mask = (unsigned char)(0xFF << (8 - valid_bits));
            self_have[bitfield_size - 1] = mask;
        }

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
    if (sock == -1){
      die_sys("cannot create socket");
    }

    //new connection not sure if it will work with windows
      if (!connect(sock, ai->ai_addr, ai->ai_addrlen))
          goto success;
      close(sock);
  }
    err("failed to connect to %s port %u: %s", host, (unsigned int)port, strerror(errno));
    freeaddrinfo(ai_head);
    return;
    success:
    freeaddrinfo(ai_head);
    msg("makes a connection to Peer %u.", id);

    make_socket_nonblocking(sock); /* mark non-blocking from now on */

    conn_add(sock, id);
}

static struct conn *
conn_accept(void)
{
    int sock = accept(self_sock, NULL, NULL);
    if (sock == -1)
        die_sys("cannot accept");
    make_socket_nonblocking(sock);

    return conn_add(sock, 0); // remote id will be set by handshake
}



static void
conn_init(const char *filename)
{
    expected_peer_count = 0;
    expected_peer_ids = NULL;

    char line[1024];
    FILE *f = fopen(filename, "r");
    if (!f) die_sys("could not open '%s'", filename);

    //new loop for multiple connections
    while (fgets(line, sizeof(line), f)) {
        char *fid, *host, *fprt, *fhas;
        unsigned int id, port;
        fid = strtok(line, config_delim);
        if (!fid || fid[0] == '#') continue;
        if (!(host = strtok(NULL, config_delim)) ||
            !(fprt = strtok(NULL, config_delim)) ||
            !(fhas = strtok(NULL, config_delim)) ||
            parse_pint(fid, &id) || !id ||
            parse_pint(fprt, &port) || !port || port > 0xffff) {
            die("could not parse '%s'", filename);
        }
        //not sure
        unsigned int *tmp = realloc(expected_peer_ids, (expected_peer_count + 1) * sizeof(*expected_peer_ids));
        if (!tmp) die("realloc failed for expected_peer_ids");
        expected_peer_ids = tmp;
        expected_peer_ids[expected_peer_count++] = id;

        if (id == self_id) {
            conn_bind(port, !strcmp(fhas, "1")); // bind/listen for self
        } else if (id < self_id) {
            conn_connect(host, port, id);
        } else {
            //dont attempt all peers at once
        }
    }


    fclose(f);
    if (self_sock == -1)
        die("could not find self configuration");

    //board = calloc(conn_count, sizeof(*board));
    //testing:works
    if (conn_count > 0) {
        board = calloc(conn_count, sizeof(*board));
        if (!board) die("calloc failed for board");
        board_cap = conn_count;
    } else {
        board = NULL;
        board_cap = 0;
    }
}

static int
conn_ratecmp(const void *a, const void *b)
{
  return (int)(*(struct conn **)b)->rrate - (*(struct conn **)a)->rrate;
}
static int
all_peers_complete(void)
{
    if (self_num_pieces != num_pieces) return 0;

    for (int i = 0; i < expected_peer_count; ++i) {
        unsigned int id = expected_peer_ids[i];
        if (id == self_id) continue;

        struct conn *c;
        for (c = conn_head; c; c = c->next) {
            if (c->id == (int)id) break;
        }
        if (!c) {
            return 0;
        }
        if (c->num_pieces != num_pieces) {
            return 0;
        }
    }

    return 1;
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
  //deubg
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

#ifdef _WIN32
    WSADATA wsaData;
int r = WSAStartup(MAKEWORD(2,2), &wsaData);
if (r != 0) {
    die("WSAStartup failed with code %d", r);
}
#endif


    //msg("DEBUG startup: num_pieces=%u bitfield_size=%u self_num_pieces=%u",num_pieces, bitfield_size, self_num_pieces);


    static const char usage[] = "usage: peer <peer_id>\n";
  if (argc != 2 || parse_pint(argv[1], &self_id)) {
    write(2, usage, sizeof(usage) - 1);
    return 2;
  }


  char filename[1024];

  //log file opening
  snprintf(filename, sizeof(filename), "log_peer_%u.log", self_id);
  logfd = open(filename, O_WRONLY | O_CREAT | O_TRUNC, 0666);
  if (logfd == -1)
    die_sys("failed to open '%s'", filename);



  //directory creation
  snprintf(filename, sizeof(filename), "peer_%u", self_id);
  if (mkdir(filename, 0777) && errno != EEXIST)
    die_sys("failed to create directory '%s'", filename);




  config_load("Common.cfg");
  conn_init("PeerInfo.cfg");


    //event loop
    struct timespec ts;
    unsigned int prev_time, cur_time, t1 = -1, t2 = -1;
    //struct pollfd *pfds, *pfd;
    struct conn *c;

    clock_gettime(CLOCK_MONOTONIC, &ts);
    prev_time = ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
    srand48(prev_time + self_id);

// allocate space for listener and connections
    struct pollfd *pfds = NULL, *pfd;
    pfds = calloc(conn_count + 1, sizeof(*pfds));


    //accept connections before we are in loop
    //still need to do something for leaving at the end
    struct timespec start, now;
    clock_gettime(CLOCK_MONOTONIC, &start);

    struct pollfd p;
    p.fd = self_sock;
    p.events = POLLIN;

    while (1) {
        clock_gettime(CLOCK_MONOTONIC, &now);
        long elapsed =
                (now.tv_sec - start.tv_sec) * 1000 +
                (now.tv_nsec - start.tv_nsec) / 1000000;

        if (elapsed > 1000)  // wait 1 second max
            break;

        int r = poll(&p, 1, 100);  // 100ms mini-poll

        if (r > 0 && (p.revents & POLLIN)) {
            conn_accept();
        } else if (r < 0) {
            break;
        }
    }
//accept connections before loop





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

    //new done statement
    int done = all_peers_complete();
    if (done) {
        msg("all peers have the complete file; exiting.");
        return 0;
    }

// Not sure about this
    size_t needed = (size_t)conn_count + 1;
    struct pollfd *tmp = realloc(pfds, needed * sizeof(*pfds));
    if (!tmp) die("realloc failed for pfds");
    pfds = tmp;




    // polling list
    int nfds = 0;

    // listener socket at index 0
    pfds[nfds].fd = self_sock;
    pfds[nfds].events = POLLIN;
    nfds++;

    // connection sock
    for (c = conn_head; c; c = c->next) {
        pfds[nfds].fd = c->sock;
        pfds[nfds].events = c->wsize ? (POLLOUT | POLLIN) : POLLIN;
        nfds++;
    }

    // poll
    int timeout = (t1 < t2 ? t1 : t2) + 1;
    int n = poll(pfds, nfds, timeout);

    //debug
    //msg("poll returned %d (polling %d fds, max wait %d ms)", n, nfds, timeout);

    if (n < 0) goto loop;

    if (pfds[0].revents & POLLIN) {
        // listener socket READY, accepting inbound connection
        conn_accept();
    }

    // Handle connection sockets
    pfd = &pfds[1];  // skip listener
    for (c = conn_head; c; c = c->next, ++pfd) {
        if (!pfd->revents)
            continue;

        //msg("poll detected events %d on fd %d (peer %u)",pfd->revents, pfd->fd, c->id);


        if (pfd->revents & POLLOUT)
            conn_handle_write(c);

        if (pfd->revents & (POLLIN | POLLERR | POLLHUP))
            conn_handle_read(c);

    }

    goto loop;


}





