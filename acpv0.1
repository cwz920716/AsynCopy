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
} log_entry_t;

typedef log_entry_t *log_entry_ptr;

// global vars
#define NASYN (500) 
#define NLOGS (NASYN)
log_entry_t logs[NLOGS];
int log_next;
aio_context_t ctx = 0;
#define PAGE_SIZE (4096 * 4)
struct iocb cbs[NASYN];
void **bufs[NASYN];

void mfc_reg (char *src_name, char *dst_name);
void meta_first_copy (char *src, char *dst);

void mfc_dir (char *src_name_in, char *dst_name_in, struct stat *src_sb) {
  char *name_space;
  char *namep;

  mkdir(dst_name_in, src_sb->st_mode);

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

      meta_first_copy (src_name, dst_name);
      free (src_name);
      free (dst_name);
      namep += strlen (namep) + 1;
  }

  free (name_space);
  return;
}

static void log_flush() {
  int i, j, submit = 0, ret, finished = 0, nr = 0, nor = 0, cur = 0;
  while (1) {
    finished = 1;
    nor = 0;
    struct iocb *cbsubmit[NASYN];
    for (;cur < NLOGS;cur++) {
      // printf("cur = %d\n", cur);
      i = cur;
      if (nor > 128)
        break;
      async_op_ptr opp = &logs[i].cop;

      submit = 0;
      if (logs[i].state == END || logs[i].state == INVALID)
        continue;
      
      finished = 0;
      if (opp->state == READ || opp->state == WRITE)
        continue;

      memset(&cbs[i], 0, sizeof(cbs[i]));
      if (opp->state == IDLE) {
        // submit a read request
        cbs[i].aio_fildes =logs[i].srcfd;
        cbs[i].aio_lio_opcode = IOCB_CMD_PREAD;
        cbs[i].aio_buf = (uint64_t) bufs[i];
        cbs[i].aio_offset = logs[i].pos;
        cbs[i].aio_nbytes = PAGE_SIZE;
        submit = 1;
      } else if (opp->state == TRANSIT && !opp->eof) {
        // submit a transit request
        cbs[i].aio_fildes =logs[i].dstfd;
        cbs[i].aio_lio_opcode = IOCB_CMD_PWRITE;
        cbs[i].aio_buf = (uint64_t) bufs[i];
        cbs[i].aio_offset = logs[i].pos;
        cbs[i].aio_nbytes = opp->nbytes;
        submit = 1;
      } else if (opp->state == TRANSIT && opp->eof) {
        logs[i].state = END;
        logs[i].cop.state = IDLE;
      }

      if (submit) {
        cbsubmit[nor] = &cbs[i];
        nor++;
        nr++;
        if (opp->state == IDLE) 
          opp->state = READ;
        else
          opp->state = WRITE;
        }

      }
    
      if (cur >= NLOGS)
        cur = 0;

      if (nor > 0) {
        // printf("subm nor = %d\n", nor);
        // printf("submit r?%d, %llu %llu\n", (cbs[i].aio_lio_opcode == IOCB_CMD_PREAD), cbs[i].aio_offset, cbs[i].aio_nbytes);
        ret = io_submit(ctx, nor, cbsubmit);
        if (ret < nor) { 
          perror ("io_submit retVal < nor.\n");
          abort ();
        }
      } 
    // io_getevents
    if (nr > 0) {
      struct io_event events[NASYN];
      struct timespec to;
      to.tv_nsec = 0;
      to.tv_sec = 0;
      ret = io_getevents(ctx, 1, 64, events, &to);
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
            if (cbs + j == (struct iocb *) ev.obj) {
 
              async_op_ptr opp = &logs[j].cop;
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
                logs[j].pos += opp->nbytes;
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

   log_next = 0;
   for (i = 0; i < NLOGS; i++) {
     logs[i].state = INVALID;
     close(logs[i].srcfd);
     close(logs[i].dstfd);
   }
}

void log_copy(int srcfd, int dstfd) {
  // printf("log_copy %d\n", log_next);
  struct stat sb;
  fstat(srcfd, &sb);

  if (NLOGS == 0) {
    perror ("NLOGS Never Goto 0.");
    abort ();
  }

  // NLOGS > 0

  if (log_next < NLOGS) {
    memset(&cbs[log_next], 0, sizeof(cbs[log_next]));
    logs[log_next].srcfd = srcfd;
    logs[log_next].dstfd = dstfd;
    logs[log_next].sb = sb;
    logs[log_next].state = DATA;
    logs[log_next].cop.state = IDLE;
    logs[log_next].cop.eof = 0;
    logs[log_next].pos = SEEK_SET;
    log_next++;
    return;
  } else {
    log_flush();
    log_copy (srcfd, dstfd);
  }
}

void mfc_reg (char *src_name, char *dst_name) {
  // struct stat src_sb;
  // printf("read reg(): src_name: %s \n", src_name);

  int srcfd = open(src_name, O_RDONLY);

  int dstfd = open(dst_name, O_WRONLY | O_CREAT | O_TRUNC, 0777);

  if (srcfd < 0 || dstfd < 0) {
    perror("open: srcfd < 0 OR dstfd < 0.");
    abort();
  }

  log_copy(srcfd, dstfd);
}

void meta_first_copy (char *src, char *dst) {
  struct stat src_sb;
  mode_t src_mode;

  lstat (src, &src_sb);
  src_mode = src_sb.st_mode;

  if (S_ISDIR(src_mode)) {
    mfc_dir(src, dst, &src_sb);
  }

  if (S_ISREG(src_mode)) {
    mfc_reg(src, dst);
  }
}

int main(int argc, char **argv) {
  int ret, i;

  if (argc < 3) {
    printf("bag input.\n");
    return 0;
  }

  char *src = argv[1];
  char *dst = argv[2];

  log_next = 0;
  ctx = 0;
  ret = io_setup(NASYN, &ctx);
  if (ret < 0) {
    perror ("io_setup failed.");
    abort ();
  }
  for (i = 0; i < NASYN; i++) {
    bufs[i] = xmalloc(PAGE_SIZE);
  }

  meta_first_copy (src, dst);
  log_flush();

  return 0;
}
