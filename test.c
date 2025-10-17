#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>

#define MAX_PEERS 32

extern char **environ;
static char env[64];
static const char *filename = "PeerInfo.cfg";

int main(int argc, char **argv)
{
  char arg[32];
  char *args[3] = { "peer", arg, NULL };
  int pipefd[2];
  int id, ret, wstatus;
  int num_peers = 0;
  int ids[MAX_PEERS];
  pid_t pids[MAX_PEERS];
  pid_t pid;
  if (pipe(pipefd)) {
    fprintf(stderr, "fatal: pipe: %s\n", strerror(errno));
    exit(-1);
  }
  sprintf(env, "P2P_NOTIFY_FD=%d", pipefd[1]);
  putenv(env);
  FILE *f = fopen(filename, "r");
  if (!f) {
    fprintf(stderr, "fatal: could not open '%s': %s\n", filename, strerror(errno));
    exit(-1);
  }
  char line[1024];
  while (fgets(line, sizeof(line), f) && num_peers < MAX_PEERS) {
    id = atoi(strtok(line, "\r\n "));
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
      _exit(-1);
    }
    ids[num_peers] = id;
    pids[num_peers] = pid;
    read(pipefd[0], &arg, 1);
    ++num_peers;
  }
  fclose(f);
  ret = 0;
  while ((pid = wait(&wstatus)) != -1) {
    for (int i = 0; i < num_peers; ++i) {
      if (pids[i] == pid) {
        id = ids[i];
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
