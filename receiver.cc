// I cannot believe there is no existing tool does this.
// Basically listen and receive multiple connections and dump them
// into files.

#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <string>
#include <cstring>
#include <fcntl.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <arpa/inet.h>

static const size_t kBufferSize = 1 << 20;

static void DoDumpNetwork(int nfds, int fds[], bool nullfile, size_t buffer_size = (512 << 20))
{
  FILE **fout = new FILE*[nfds];
  char **fbuf = new char*[nfds];
  char *buffer = new char[kBufferSize];

  if (!nullfile) {
    for (int i = 0; i < nfds; i++) {
      std::string filename = std::string("dolly-net.") + std::to_string(i) + ".dump";
      fout[i] = fopen(filename.c_str(), "w");
      if (buffer_size == 0) {
	fprintf(stderr, "IO buffer size is 0\n");
	continue;
      }
      fbuf[i] = (char *) malloc(buffer_size);
      setvbuf(fout[i], fbuf[i], _IOFBF, buffer_size);
    }
  }

  if (!nullfile) {
    fprintf(stderr, "Dumping the network packets into dolly-net.*.dump\n");
  } else {
    fprintf(stderr, "Start receiving packets\n");
  }

  int max_fd = -1;
  fd_set fset;
  fd_set eof_fset;
  FD_ZERO(&fset);
  FD_ZERO(&eof_fset);

  // set sockets to async and watch for reading
  for (int i = 0; i < nfds; i++) {
    int opt = fcntl(fds[i], F_GETFL);
    fcntl(fds[i], F_SETFL, opt | O_NONBLOCK);
    FD_SET(fds[i], &fset);
    max_fd = std::max(max_fd, fds[i] + 1);
  }
  int eof_count = 0;
  while (true) {
    int res = select(max_fd, &fset, NULL, NULL, NULL);
    if (errno == EINTR || res == 0) continue;

    if (res < 0) {
      perror("select");
      std::abort();
    }
    max_fd = -1;
    for (int i = 0; i < nfds; i++) {
      int fd = fds[i];
      if (!FD_ISSET(fd, &fset)) {
	if (!FD_ISSET(fd, &eof_fset)) {
	  FD_SET(fd, &fset);
	  max_fd = std::max(max_fd, fd + 1);
	}
	continue;
      }
      int rsize = 0;
      int rcount = 0;
      int tot_rsize = 0;
      while (++rcount < 160 && (rsize = read(fd, buffer, kBufferSize)) > 0) {
	if (!nullfile)
	  fwrite(buffer, rsize, 1, fout[i]);
	tot_rsize += rsize;
      }
      if (rsize < 0 && errno != EAGAIN && errno != EWOULDBLOCK) {
	perror("read");
	std::abort();
      }
      if (rsize == 0) {
	fprintf(stderr, "connection %d EOF\n", i);
	FD_CLR(fd, &fset);
	FD_SET(fd, &eof_fset);
	eof_count++;
      } else {
	// fprintf(stderr, "connection %d recv %u bytes\n", i, tot_rsize);
	max_fd = std::max(max_fd, fd + 1);
      }
    }
    if (eof_count == nfds)
      break;
  }
  delete [] buffer;
  if (!nullfile) {
    for (int i = 0; i < nfds; i++) {
      fclose(fout[i]);
      free(fbuf[i]);
    }
  }
  delete [] fout;
}

static int SetupServer(const char *addr, int port)
{
  struct sockaddr_in serv;
  int fd = socket(AF_INET, SOCK_STREAM, 0);
  memset(&serv, 0, sizeof(struct sockaddr_in));

  serv.sin_family = AF_INET;
  serv.sin_addr.s_addr = inet_addr(addr);
  serv.sin_port = htons(port);

  if (bind(fd, (struct sockaddr *) &serv, sizeof(struct sockaddr)) < 0) {
    perror("bind");
    goto error;
  }
  if (listen(fd, 1) < 0) {
    perror("listen");
    goto error;
  }
  fprintf(stderr, "Listening on port %u\n", port);
  return fd;
error:
  close(fd);
  return -1;
}

static void WaitForAllClient(int server_fd, int nfds, int fds[])
{
  for (int i = 0; i < nfds; i++) {
    socklen_t addrlen = sizeof(struct sockaddr_in);
    struct sockaddr_in addr;
    fds[i] = accept(server_fd, (struct sockaddr *) &addr, &addrlen);
    if (fds[i] < 0) {
      fprintf(stderr, "Error when accepting fd %d\n", i);
      perror("accept");
      std::abort();
    }
  }
}

static void ShowUsage(const char *prog)
{
  printf("%s -c <nr_clients> -h <host> -p <port>\n", prog);
  std::exit(-1);
}

int main(int argc, char *argv[])
{
  int opt;
  int nfds = 0;
  bool nullfile = false;
  std::string host;
  int port = 0;
  size_t buffer_size = (512 << 20);
  while ((opt = getopt(argc, argv, "c:h:p:n:b:")) != -1) {
    switch (opt) {
    case 'c':
      nfds = atoi(optarg);
      break;
    case 'h':
      host = std::string(optarg);
      break;
    case 'p':
      port = atoi(optarg);
      break;
    case 'n':
      nullfile = true;
      break;
    case 'b':
      buffer_size = strtol(optarg, nullptr, 10);
      break;
    default:
      ShowUsage(argv[0]);
      break;
    }
  }

  if (nfds == 0) {
    fprintf(stderr, "Number of clients is 0\n");
    std::abort();
  }

  if (port == 0) {
    fprintf(stderr, "Invalid port %d\n", port);
    std::abort();
  }

  if (host == "") {
    fprintf(stderr, "Empty host\n");
    std::abort();
  }

  int server_fd = SetupServer(host.c_str(), port);
  int fds[nfds];
  WaitForAllClient(server_fd, nfds, fds);
  printf("Start receiving...\n");
  DoDumpNetwork(nfds, fds, nullfile, buffer_size);
  printf("Done\n");

  return 0;
}
