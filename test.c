#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>

static const char *err_prefix = "error: ";

static void __attribute__((format(printf, 2, 3)))
msg(const char *prefix, const char *format, ...)
{
  va_list ap;
  char buf[2048], *p = buf;
  if (prefix) {
    size_t prefix_len = strlen(prefix);
    memcpy(buf, prefix, prefix_len);
    p += prefix_len;
  }
  va_start(ap, format);
  vsnprintf(p, buf + sizeof(buf) - p, format, ap);
  va_end(ap);
  for (; *p; ++p)
    if (*p < 0x20 || *p == 0x7f)
      *p = '?';
  *p = '\n';
  write(2, buf, ++p - buf);
}

#define err(...) msg(err_prefix, __VA_ARGS__)
#define err_sys_(format, ...) err(format ": %s", __VA_ARGS__)
#define err_sys(...) err_sys_(__VA_ARGS__, strerror(errno))

#define BASE_ID 1001
#define NUM_PEERS 9

static void
handle_termination_signal(int signum)
{
  signal(SIGTERM, SIG_DFL);
  kill(0, SIGTERM);
}

extern char **environ;

int main(int argc, char **argv)
{
  if (setpgid(0, 0)) {
    err_sys("setpgid");
    return 1;
  }
  struct sigaction sa = { .sa_handler = handle_termination_signal };
  sigaction(SIGHUP, &sa, NULL);
  sigaction(SIGINT, &sa, NULL);
  sigaction(SIGTERM, &sa, NULL);
  char *args[3];
  char arg[32];
  args[0] = "peer";
  args[1] = arg;
  args[2] = NULL;
  for (int i = 0; i < NUM_PEERS; ++i) {
    unsigned int id = BASE_ID + i;
    snprintf(arg, sizeof(arg), "%u", id);
    pid_t pid = fork();
    if (pid < 0) {
      err_sys("fork");
      return 1;
    }
    if (pid == 0) {
      /* child */
      execve("peer", args, environ);
      err_sys("failed to execute peer process");
      exit(-1);
    }
  }
  int ret = 0;
  int wstatus;
  while (wait(&wstatus) != -1) {
    if (WEXITSTATUS(wstatus)) {
      ret = 1;
    }
  }
  return ret;
}
