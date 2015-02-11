#include "config.h"
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <fcntl.h>
#include <sys/ioctl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <stdio.h>
#include <dirent.h>
#include <sys/dir.h>
#include <error.h>
#include <stddef.h>
#include <stdint.h>
#include <sys/syscall.h>	/* for __NR_* definitions */
#include <linux/aio_abi.h>	/* for AIO types and constants */

#include "savedir.h"
#include "filenamecat.h"
#include "xalloc.h"

inline int io_setup(unsigned nr, aio_context_t *ctxp)
{
  return syscall(__NR_io_setup, nr, ctxp);
}
  
inline int io_destroy(aio_context_t ctx) 
{
  return syscall(__NR_io_destroy, ctx);
}
  
inline int io_submit(aio_context_t ctx, long nr,  struct iocb **iocbpp) 
{
  return syscall(__NR_io_submit, ctx, nr, iocbpp);
}
  
inline int io_getevents(aio_context_t ctx, long min_nr, long max_nr,
                struct io_event *events, struct timespec *timeout)
{
  return syscall(__NR_io_getevents, ctx, min_nr, max_nr, events, timeout);
}

int log_next;

typedef enum {IDLE, READ, TRANSIT, WRITE} op_state_t;
typedef enum {INVALID, DATA, END} state_t;

typedef struct {
  op_state_t state;
  char *buf;
  size_t nbytes;
  off_t offset;
  int eof; // EOF code
} async_op_t;

typedef async_op_t *async_op_ptr;

typedef struct {
  int srcfd;
  int dstfd;
  struct stat sb; // stat block for src
  async_op_t cop; // current assigned op
  // global state: ALWAYS SEQUENTIAL READ/WRITE
  state_t state;
  off_t pos;
  void *buf;
} log_entry_t;

typedef log_entry_t *log_entry_ptr;

#define NAMELEN (128)

typedef struct {
  char srcName[NAMELEN + 1];
  char dstName[NAMELEN + 1];
  pthread_t thread;
  int valid;
  int marked;
} dir_entry_t;

typedef dir_entry_t *dir_entry_ptr;

// global vars
#define NTHREADS (16) 
#define NDIRS (1024 * 4)
#define PAGE_SIZE (4096 * 4)

dir_entry_t fifo_dirs[NDIRS]; // a straight line queue buffer, never circular
int enqP, deqP, markP, cnt;

void fifo_init() {
  enqP = deqP = markP = cnt = 0;
}

int fifo_canWalk() {
  return enqP > markP;
}

int fifo_canDeq() {
  return markP > deqP;
}

void fifo_enq(char *src, char *dst) {

  if (enqP >= NDIRS) {
    perror("Linear Directory Queue too small!\n");
    abort();
  }

  if (strlen(src) > NAMELEN || strlen(dst) > NAMELEN) {
    perror("fifo_enq() too long name\n");
    abort();
  }

  dir_entry_ptr p = fifo_dirs + enqP;
  strcpy(p->srcName, src);
  strcpy(p->dstName, dst);
  p->valid = 1;
  p->marked = 0;
  enqP++;
}

dir_entry_ptr fifo_walk() {

  dir_entry_ptr p = fifo_dirs + markP;

  if (markP >= enqP || !p->valid) {
    perror("walk an inValid Node!\n");
    abort();
  }

  if (!p->marked)
    p->marked = 1;
  else {
    printf("Walk into a marked node!\n");
    abort();
  }

  markP++;
  return p;
}

dir_entry_ptr fifo_first() {

  dir_entry_ptr p = fifo_dirs + deqP;

  if (deqP >= markP || !p->marked) {
    perror("First() an unmarked Node!\n");
    abort();
  }

  return p;
}

void fifo_deq() {

  dir_entry_ptr p = fifo_dirs + deqP;

  if (p->valid)
    p->valid = 0;
  else  {
    perror("Deq() an inValid Node!\n");
    abort();
  }

  deqP++;
}

void visit_dir (char *src_name_in, char *dst_name_in) {
  char *name_space;
  char *namep;
  struct stat src_sb;

  printf("Visiting %s\n", src_name_in);

  lstat(src_name_in, &src_sb);
  mkdir(dst_name_in, src_sb.st_mode);

  name_space = savedir (src_name_in, SAVEDIR_SORT_FASTREAD);
  if (name_space == NULL) {
      /* This diagnostic is a bit vague because savedir can fail in
         several different ways.  */
      perror("read directory failed.");
      abort();
  }

  namep = name_space;
  while (*namep != '\0') {
      char *src_name = file_name_concat (src_name_in, namep, NULL);
      char *dst_name = file_name_concat (dst_name_in, namep, NULL);
      struct stat src_sb;
      mode_t src_mode;

      lstat (src_name, &src_sb);
      src_mode = src_sb.st_mode;

      if (S_ISDIR(src_mode)) {
          fifo_enq(src_name, dst_name);
      }

      free (src_name);
      free (dst_name);
      namep += strlen (namep) + 1;
  }

  free (name_space);
  return;
}

void directory_walking (char *src, char *dst) {
  struct stat src_sb;
  mode_t src_mode;

  lstat (src, &src_sb);
  src_mode = src_sb.st_mode;

  printf("Walking %s and %s\n", src, dst);

  if (!S_ISDIR(src_mode)) {
    perror("walking: src is not dir!");
    abort();
  }

  fifo_enq(src, dst);
  while (fifo_canWalk()) {
    dir_entry_ptr p = fifo_walk();
    visit_dir (p->srcName, p->dstName);
  }

}

int main(int argc, char **argv) {
  // int ret, i;

  if (argc < 3) {
    printf("bag input.\n");
    return 0;
  }

  char *src = argv[1];
  char *dst = argv[2];

  directory_walking (src, dst);
  printf("%d %d %d\n", enqP, markP, deqP);

  return 0;
}
