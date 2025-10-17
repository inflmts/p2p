#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>

#define BASE_ID 101
#define NUM_PEERS 5

extern char **environ;
static char env[64];

int main(int argc, char **argv)
{
  char *args[3];
  char arg[32];
  int pipefd[2];
  unsigned int id;
  int ret, wstatus;
  pid_t pids[NUM_PEERS];
  pid_t pid;
  if (pipe(pipefd)) {
    fprintf(stderr, "fatal: pipe: %s\n", strerror(errno));
    exit(-1);
  }
  sprintf(env, "P2P_NOTIFY_FD=%d", pipefd[1]);
  putenv(env);
  args[0] = "peer";
  args[1] = arg;
  args[2] = NULL;
  for (int i = 0; i < NUM_PEERS; ++i) {
    id = BASE_ID + i;
    sprintf(arg, "%u", id);
    pid = fork();
    if (pid < 0) {
      fprintf(stderr, "fatal: fork: %s\n", strerror(errno));
      break;
    }
    if (pid == 0) {
      /* child */
      execve("peer", args, environ);
      fprintf(stderr, "fatal: failed to execute peer process: %s\n", strerror(errno));
      exit(-1);
    }
    pids[i] = pid;
    read(pipefd[0], &arg, 1);
  }
  ret = 0;
  while ((pid = wait(&wstatus)) != -1) {
    for (int i = 0; i < NUM_PEERS; ++i) {
      if (pids[i] == pid) {
        id = i + BASE_ID;
        if (WIFSIGNALED(wstatus)) {
          fprintf(stderr, "[peer %u: %s]\n", id, strsignal(WTERMSIG(wstatus)));
          ret = 1;
        } else {
          fprintf(stderr, "[peer %u exited %d]\n", id, WEXITSTATUS(wstatus));
          if (WEXITSTATUS(wstatus)) {
            ret = 1;
          }
        }
      }
    }
  }
  return ret;
}
