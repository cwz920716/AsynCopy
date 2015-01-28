#include <config.h>
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


#define NAMLEN(dirent) strlen((dirent)->d_name)

#define NLOGS (500)

int log_next;

typedef struct {
  int srcfd;
  int dstfd;
  ino_t src_ino;
  char *buf;
  off_t offset;
  int started;
  int finished;
} logentry_t;

logentry_t logs[NLOGS];


static int logentry_cmp_inode (void const *a, void const *b)
{
  logentry_t const *dea = a;
  logentry_t const *deb = b;

  return dea->src_ino < deb->src_ino ? -1 : dea->src_ino > deb->src_ino;
}

void metaRead (char *src, char *dst, int fd);

void meta_readdir (char *src_name_in, char *dst_name_in, struct stat *src_sb, int fd) {
  char *name_space;
  char *namep;

  // printf("read dir(): src_name: %s \n", src_name_in);

  mkdir(dst_name_in, src_sb->st_mode);

  name_space = savedir (src_name_in, SAVEDIR_SORT_FASTREAD);
  if (name_space == NULL) {
      /* This diagnostic is a bit vague because savedir can fail in
         several different ways.  */
      return;
  }

  namep = name_space;
  while (*namep != '\0') {
      int fn_length = strlen (namep) + 1;
      char *src_name = malloc (strlen (src_name_in) + fn_length + 1);
      stpcpy (stpcpy (stpcpy (src_name, src_name_in), "/"), namep);
      // char *src_name = file_name_concat (src_name_in, namep, NULL);
      char *dst_name = malloc (strlen (dst_name_in) + fn_length + 1);
      stpcpy (stpcpy (stpcpy (dst_name, dst_name_in), "/"), namep);

      metaRead (src_name, dst_name, fd);
      namep += strlen (namep) + 1;
  }

  return;
}

void log_copy(int srcfd, int dstfd, ino_t src_ino) {

  // printf("log_copy %d\n", log_next);

  if (NLOGS == 0) {
    copy_reg (srcfd, dstfd, "src", "dst", -1);
    close(srcfd);
    close(dstfd);
    return;
  }

  // NLOGS > 0

  if (log_next < NLOGS) {
    logs[log_next].srcfd = srcfd;
    logs[log_next].dstfd = dstfd;
    logs[log_next].src_ino = src_ino;
    log_next++;
    return;
  } else {
    log_flush();
    log_copy(srcfd, dstfd, src_ino);
  }
}

void meta_readreg (char *src_name, char *dst_name) {
  // struct stat src_sb;
  // printf("read reg(): src_name: %s \n", src_name);

  int srcfd = open(src_name, O_RDONLY | O_BINARY);

  int dstfd = open(dst_name, O_WRONLY | O_CREAT | O_TRUNC | O_BINARY, 0777);

  if (srcfd < 0 || dstfd < 0) {
    printf("open: src_name: %s dst_name %s srcfd %d dstfd %d log_next %d \n", src_name, dst_name, srcfd, dstfd, log_next);
    exit(EXIT_FAILURE);
  }

  struct stat sb;
  fstat(srcfd, &sb);

  log_copy(srcfd, dstfd, sb.st_ino);

  // close(srcfd);
  // close(dstfd);

}

void metaRead (char *src, char *dst, int fd) {
  struct stat src_sb;
  mode_t src_mode;
  // printf("read meta(): src: %s dst: %s \n", src, dst);

  // char log[1024] = "";

  lstat (src, &src_sb);
  src_mode = src_sb.st_mode;

  if (S_ISDIR(src_mode)) {
    meta_readdir(src, dst, &src_sb, fd);
    // sprintf(log, "%s D %s\n", src, dst);
  }

  if (S_ISREG(src_mode)) {
    meta_readreg(src, dst);
    // sprintf(log, "%s %s R\n", src, dst);
  }

  // write(fd, log, strlen(log));

}

int main(int argc, char **argv) {

  if (argc < 3) {
    printf("bag input.\n");
    return 0;
  }

  int fd = open (DIR_CONF, O_WRONLY | O_CREAT | O_TRUNC, 0777);

  char *src = argv[1];
  char *dst = argv[2];

  log_next = 0;
  metaFirstCopy (src, dst, fd);
  log_flush();

  close(fd);

  return 0;
}
