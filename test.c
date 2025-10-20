#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#define MAX_PEERS 32

extern char **environ;

static void __attribute__((format(printf, 1, 2)))
msg(const char *format, ...)
{
  va_list ap;
  char buf[2048], *s;
  va_start(ap, format);
  vsnprintf(buf, sizeof(buf), format, ap);
  va_end(ap);
  for (s = buf; *s; ++s)
    if (*s < 0x20 || *s == 0x7f)
      *s = '?';
  *(s++) = '\n';
  write(2, buf, s - buf);
}

#define fatal_(format, ...) msg("fatal: " format ": %s", __VA_ARGS__)
#define fatal(...) fatal_(__VA_ARGS__, strerror(errno)), _exit(-1)

int main(int argc, char **argv)
{
  int id, ids[MAX_PEERS];
  pid_t pid, pids[MAX_PEERS];

  /* parse peerinfo file */
  const char *const filename = "PeerInfo.cfg";
  char line[1024];
  int num_peers = 0;
  FILE *f = fopen(filename, "r");
  if (!f) {
    fatal("could not open '%s'", filename);
  }
  while (fgets(line, sizeof(line), f)) {
    if (num_peers == MAX_PEERS) {
      fatal("only %d peers supported", MAX_PEERS);
    }
    char *token = strtok(line, "\r\n ");
    if (token && (id = atoi(token))) {
      ids[num_peers++] = id;
    }
  }
  fclose(f);

  /* launch peer processes */
  char arg[32];
  char *args[3] = { "peer.exe", arg, NULL };
  for (int i = 0; i < num_peers; ++i) {
    pid = fork();
    if (pid < 0) {
      fatal("fork");
      break;
    }
    if (pid == 0) {
      /* sleep for i x 200ms */
      struct timespec ts = { i / 5, (i % 5) * 200000000 };
      nanosleep(&ts, NULL);
      sprintf(arg, "%d", ids[i]);
      msg("[starting peer %d]", ids[i]);
      execve("peer.exe", args, environ);
      fatal("failed to execute peer process %d", ids[i]);
    }
    pids[i] = pid;
  }

  /* wait for them all to exit */
  int ret = 0;
  int wstatus;
  while ((pid = wait(&wstatus)) != -1) {
    for (int i = 0; i < num_peers; ++i) {
      if (pids[i] == pid) {
        id = ids[i];
        if (WIFSIGNALED(wstatus)) {
          msg("[peer %d signaled: %s]", id, strsignal(WTERMSIG(wstatus)));
        } else {
          msg("[peer %d exited %d]", id, WEXITSTATUS(wstatus));
        }
        if (WIFSIGNALED(wstatus) || WEXITSTATUS(wstatus)) {
          ret = 1;
        }
        break;
      }
    }
  }
  return ret;
}
