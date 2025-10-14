#include <errno.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>

#define BASE_ID 101
#define NUM_PEERS 9

extern char **environ;

int main(int argc, char **argv)
{
  char *args[3];
  char arg[32];
  args[0] = "peer";
  args[1] = arg;
  args[2] = NULL;
  signal(SIGINT, SIG_IGN);
  signal(SIGTERM, SIG_IGN);
  pid_t pids[NUM_PEERS];
  pid_t pid;
  for (int i = 0; i < NUM_PEERS; ++i) {
    unsigned int id = BASE_ID + i;
    snprintf(arg, sizeof(arg), "%u", id);
    pid = fork();
    if (pid < 0) {
      fprintf(stderr, "fatal: fork: %s\n", strerror(errno));
      break;
    }
    if (pid == 0) {
      /* child */
    signal(SIGINT, SIG_DFL);
    signal(SIGTERM, SIG_DFL);
      execve("peer", args, environ);
      fprintf(stderr, "fatal: failed to execute peer process: %s\n", strerror(errno));
      exit(-1);
    }
    pids[i] = pid;
  }
  int ret = 0;
  int wstatus;
  while ((pid = wait(&wstatus)) != -1) {
    for (int i = 0; i < NUM_PEERS; ++i) {
      if (pids[i] == pid) {
        if (WIFSIGNALED(wstatus)) {
          fprintf(stderr, "[%u] %s\n", i + BASE_ID, strsignal(WTERMSIG(wstatus)));
          ret = 1;
        } else if (WEXITSTATUS(wstatus)) {
          fprintf(stderr, "[%u] exited %d\n", i + BASE_ID, WEXITSTATUS(wstatus));
          ret = 1;
        }
      }
    }
  }
  return ret;
}
