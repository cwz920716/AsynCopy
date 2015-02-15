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
#include <pthread.h>
#include <errno.h>
#include <time.h>

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
#define NASYN (16) 
#define NLOGS (NASYN)

typedef struct {
  char srcName[NAMELEN + 1];
  char dstName[NAMELEN + 1];
  int valid;
  int marked;
  log_entry_t logs[NLOGS];
  int log_next;
  aio_context_t ctx;
  struct iocb cbs[NASYN];
  void **bufs[NASYN];
} dir_entry_t;

typedef dir_entry_t *dir_entry_ptr;

// global vars
#define NTHREADS (16) 
#define NDIRS (1024 * 4)
#define PAGE_SIZE (4096 * 4)
pthread_mutex_t lock;
extern int errno;

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

void fifo_swap(int i, int j) {

  dir_entry_t iE = fifo_dirs[i];
  dir_entry_t jE = fifo_dirs[j];

  dir_entry_t temp;
  temp = iE;
  iE = jE;
  jE = temp;

}

void fifo_shuffle() {
  int i = enqP - 1, j;
  srand(time(NULL));

  for (i = enqP - 1; i > 1; i--) {
    j = rand() % (i + 1);
    if (i != j) 
      fifo_swap(i, j);
  }
}

void visit_dir (char *src_name_in, char *dst_name_in) {
  char *name_space;
  char *namep;
  struct stat src_sb;

  // printf("Visiting %s\n", src_name_in);

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

static void log_flush(dir_entry_ptr p) {
  int i, j, submit = 0, ret, finished = 0, nr = 0, nor = 0, cur = 0;
  while (1) {
    finished = 1;
    nor = 0;
    struct iocb *cbsubmit[NASYN];
    for (;cur < p->log_next;cur++) {
      // printf("cur = %d\n", cur);
      i = cur;
      if (nor > 128)
        break;
      async_op_ptr opp = &p->logs[i].cop;

      submit = 0;
      if (p->logs[i].state == END || p->logs[i].state == INVALID)
        continue;
      
      finished = 0;
      if (opp->state == READ || opp->state == WRITE)
        continue;

      memset(&p->cbs[i], 0, sizeof(p->cbs[i]));
      if (opp->state == IDLE) {
        // submit a read request
        p->cbs[i].aio_fildes = p->logs[i].srcfd;
        p->cbs[i].aio_lio_opcode = IOCB_CMD_PREAD;
        p->cbs[i].aio_buf = (uint64_t) p->bufs[i];
        p->cbs[i].aio_offset = p->logs[i].pos;
        p->cbs[i].aio_nbytes = PAGE_SIZE;
        submit = 1;
      } else if (opp->state == TRANSIT && !opp->eof) {
        // submit a transit request
        p->cbs[i].aio_fildes = p->logs[i].dstfd;
        p->cbs[i].aio_lio_opcode = IOCB_CMD_PWRITE;
        p->cbs[i].aio_buf = (uint64_t) p->bufs[i];
        p->cbs[i].aio_offset = p->logs[i].pos;
        p->cbs[i].aio_nbytes = opp->nbytes;
        submit = 1;
      } else if (opp->state == TRANSIT && opp->eof) {
        p->logs[i].state = END;
        p->logs[i].cop.state = IDLE;
      }

      if (submit) {
        cbsubmit[nor] = &p->cbs[i];
        nor++;
        nr++;
        if (opp->state == IDLE) 
          opp->state = READ;
        else
          opp->state = WRITE;
        }

      }
    
      if (cur >= p->log_next)
        cur = 0;

      if (nor > 0) {
        // printf("subm nor = %d srcfd = %d dstfd = %d\n", nor, p->logs[i].srcfd, p->logs[i].dstfd);
        // printf("submit r?%d, %llu %llu\n", (p->cbs[i].aio_lio_opcode == IOCB_CMD_PREAD), p->cbs[i].aio_offset, p->cbs[i].aio_nbytes);
        ret = io_submit(p->ctx, nor, cbsubmit);
        if (ret < nor) { 
          printf ("ret=%d nor=%d", ret, nor);
          perror ("io_submit retVal < nor.\n");
          abort ();
        }
      } 
    // io_getevents
    if (nr > 0) {
      struct io_event events[NASYN];
      struct timespec to;
      to.tv_nsec = 1000;
      to.tv_sec = 0;
      ret = io_getevents(p->ctx, 1, 64, events, &to);
      if (ret < 0) { 
        perror ("io_getevents retVal < 0.\n");
        abort ();
      }

      if (ret > 0) {
        // printf("gete ret = %d\n", ret);
        nr -= ret;
        for (i = 0; i < ret; i++) {
          struct io_event ev = events[i];

          for (j = 0; j < NASYN; j++)
            if (p->cbs + j == (struct iocb *) ev.obj) {
 
              async_op_ptr opp = &p->logs[j].cop;
              if (opp->state == READ) {
                if (ev.res < 0) {
                  printf ("read %lld return < 0.\n", ev.res);
                  abort ();
                }

                opp->nbytes = ev.res;
                opp->state = TRANSIT;
                opp->eof = (ev.res == 0);

              } else {
                if (opp->nbytes != ev.res) {
                  // perror ("write early return.\n");
                  abort ();
                }

                opp->state = IDLE;
                p->logs[j].pos += opp->nbytes;
              }
            }
        }
      }
    }

    // can I exit now ?
    if (finished) {
      break;
    }
  }

   for (i = 0; i < p->log_next; i++) {
     p->logs[i].state = INVALID;
// pthread_mutex_lock(&lock);
     close(p->logs[i].srcfd);
     close(p->logs[i].dstfd);
     // printf("close src %d dst %d name %s\n", p->logs[i].srcfd, p->logs[i].dstfd, p->srcName);
// pthread_mutex_unlock(&lock);
   }
   p->log_next = 0;
}

void log_copy(int srcfd, int dstfd, struct stat sb, dir_entry_ptr p) {
  // printf("log_copy %d\n", log_next);

  if (NLOGS == 0) {
    perror ("NLOGS Never Goto 0.");
    abort ();
  }

  // NLOGS > 0

  if (p->log_next < NLOGS) {
    memset(&p->cbs[p->log_next], 0, sizeof(p->cbs[p->log_next]));
    p->logs[p->log_next].srcfd = srcfd;
    p->logs[p->log_next].dstfd = dstfd;
    p->logs[p->log_next].sb = sb;
    p->logs[p->log_next].state = DATA;
    p->logs[p->log_next].cop.state = IDLE;
    p->logs[p->log_next].cop.eof = 0;
    p->logs[p->log_next].pos = SEEK_SET;
    p->log_next++;
    return;
  } else {
    log_flush(p);
    log_copy (srcfd, dstfd, sb, p);
  }
}

void flat_reg (char *src_name, char *dst_name, dir_entry_ptr p) {
  // struct stat src_sb;
  // printf("read reg(): src_name: %s \n", src_name);
  struct stat sb;
// pthread_mutex_lock(&lock);
  int srcfd = open(src_name, O_RDONLY);
  int dstfd = open(dst_name, O_WRONLY | O_CREAT | O_TRUNC, 0777);
  fstat(srcfd, &sb);
// pthread_mutex_unlock(&lock);

  // printf("open src %d dst %d name %s name %s\n", srcfd, dstfd, src_name, dst_name);
  if (srcfd < 0 || dstfd < 0) {
    perror("open: srcfd < 0 OR dstfd < 0.");
    abort();
  }

  log_copy(srcfd, dstfd, sb, p);
}

void flat_copy (char *src_name_in, char *dst_name_in, dir_entry_ptr p) {
  char *name_space;
  char *namep;
  struct stat src_sb;
  mode_t src_mode;

// pthread_mutex_lock(&lock);
  lstat (src_name_in, &src_sb);
// pthread_mutex_unlock(&lock);
  src_mode = src_sb.st_mode;

  if (S_ISREG(src_mode)) {
    flat_reg(src_name_in, dst_name_in, p);
    return;
  }

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
      
// pthread_mutex_lock(&lock);
      lstat (src_name, &src_sb);
// pthread_mutex_unlock(&lock);
      src_mode = src_sb.st_mode;

      if (S_ISREG(src_mode)) {
        flat_reg (src_name, dst_name, p);
      }
      free (src_name);
      free (dst_name);
      namep += strlen (namep) + 1;
  }

  free (name_space);
  return;
}

void * threaded_copy(void *arg) {
  int ret, i;

  dir_entry_ptr p = (dir_entry_ptr) arg;
  p->log_next = 0;
  p->ctx = 0;
  ret = io_setup(NASYN, &p->ctx);
  if (ret < 0) {
    perror ("io_setup failed.");
    abort ();
  }

  for (i = 0; i < NASYN; i++) {
    p->bufs[i] = xmalloc(PAGE_SIZE);
  }

  // printf("thread x: %s\n", p->srcName);
  flat_copy (p->srcName, p->dstName, p);
  log_flush(p);

  io_destroy(p->ctx);
  pthread_exit(NULL);
}

int main(int argc, char **argv) {
  pthread_t threads[NTHREADS];
  int rc, i;

  if (argc < 3) {
    printf("bag input.\n");
    return 0;
  }

  char *src = argv[1];
  char *dst = argv[2];

  directory_walking (src, dst);
  fifo_shuffle();
  pthread_mutex_init(&lock, NULL);

  int nextThread = 0;
  while (1) {
    if (fifo_canDeq() && nextThread < NTHREADS) {
      dir_entry_ptr p = fifo_first();
      rc = pthread_create( &threads[nextThread], NULL, &threaded_copy, p);
      if (rc != 0) {
        perror("create thread failed.\n");
        abort();
      }
  // printf("fifo %d %d %d\n", enqP, markP, deqP);
      nextThread++;
      fifo_deq();
    }
    else if (!fifo_canDeq() && nextThread == 0) {
      break;
    } 
    else {
      for (i = 0; i < nextThread; i++)
        pthread_join(threads[i], NULL);
      nextThread = 0;
    }
  }

  printf("%d %d %d\n", enqP, markP, deqP);

  return 0;
}
