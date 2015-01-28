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

/***********************************************************************/

#include <config.h>
#include <stdio.h>
#include <assert.h>
#include <sys/ioctl.h>
#include <sys/types.h>
#include <selinux/selinux.h>

#if HAVE_HURD_H
# include <hurd.h>
#endif
#if HAVE_PRIV_H
# include <priv.h>
#endif

#include "system.h"
#include "acl.h"
#include "backupfile.h"
#include "buffer-lcm.h"
#include "canonicalize.h"
#include "copy.h"
#include "cp-hash.h"
#include "extent-scan.h"
#include "error.h"
#include "fadvise.h"
#include "fcntl--.h"
#include "fiemap.h"
#include "file-set.h"
#include "filemode.h"
#include "filenamecat.h"
#include "full-write.h"
#include "hash.h"
#include "hash-triple.h"
#include "ignore-value.h"
#include "ioblksize.h"
#include "quote.h"
#include "root-uid.h"
#include "same.h"
#include "savedir.h"
#include "stat-size.h"
#include "stat-time.h"
#include "utimecmp.h"
#include "utimens.h"
#include "write-any-file.h"
#include "areadlink.h"
#include "yesno.h"
#include "selinux.h"
#include "xalloc.h"

#if USE_XATTR
# include <attr/error_context.h>
# include <attr/libattr.h>
# include <stdarg.h>
# include "verror.h"
#endif

#ifndef HAVE_FCHOWN
# define HAVE_FCHOWN false
# define fchown(fd, uid, gid) (-1)
#endif

#ifndef HAVE_LCHOWN
# define HAVE_LCHOWN false
# define lchown(name, uid, gid) chown (name, uid, gid)
#endif

#ifndef HAVE_MKFIFO
static int
rpl_mkfifo (char const *file, mode_t mode)
{
  errno = ENOTSUP;
  return -1;
}
# define mkfifo rpl_mkfifo
#endif

#ifndef USE_ACL
# define USE_ACL 0
#endif

#define SAME_OWNER(A, B) ((A).st_uid == (B).st_uid)
#define SAME_GROUP(A, B) ((A).st_gid == (B).st_gid)
#define SAME_OWNER_AND_GROUP(A, B) (SAME_OWNER (A, B) && SAME_GROUP (A, B))

/* LINK_FOLLOWS_SYMLINKS is tri-state; if it is -1, we don't know
   how link() behaves, so assume we can't hardlink symlinks in that case.  */
#if defined HAVE_LINKAT || ! LINK_FOLLOWS_SYMLINKS
# define CAN_HARDLINK_SYMLINKS 1
#else
# define CAN_HARDLINK_SYMLINKS 0
#endif

static bool
is_probably_sparse (struct stat const *sb)
{
  return (HAVE_STRUCT_STAT_ST_BLOCKS
          && S_ISREG (sb->st_mode)
          && ST_NBLOCKS (*sb) < sb->st_size / ST_NBLOCKSIZE);
}


/* Copy the regular file open on SRC_FD/SRC_NAME to DST_FD/DST_NAME,
   honoring the MAKE_HOLES setting and using the BUF_SIZE-byte buffer
   BUF for temporary storage.  Copy no more than MAX_N_READ bytes.
   Return true upon successful completion;
   print a diagnostic and return false upon error.
   Note that for best results, BUF should be "well"-aligned.
   BUF must have sizeof(uintptr_t)-1 bytes of additional space
   beyond BUF[BUF_SIZE-1].
   Set *LAST_WRITE_MADE_HOLE to true if the final operation on
   DEST_FD introduced a hole.  Set *TOTAL_N_READ to the number of
   bytes read.  */
static bool
sparse_copy (int src_fd, int dest_fd, char *buf, size_t buf_size,
             bool make_holes,
             char const *src_name, char const *dst_name,
             uintmax_t max_n_read, off_t *total_n_read,
             bool *last_write_made_hole)
{
  // printf("sparse_cp\n");
  *last_write_made_hole = false;
  *total_n_read = 0;

  while (max_n_read)
    {
      bool make_hole = false;

      ssize_t n_read = read (src_fd, buf, MIN (max_n_read, buf_size));
      if (n_read < 0)
        {
          if (errno == EINTR)
            continue;
          error (0, errno, _("error reading %s"), quote (src_name));
          return false;
        }
      if (n_read == 0)
        break;
      max_n_read -= n_read;
      *total_n_read += n_read;

      if (make_holes)
        {
          /* Sentinel required by is_nul().  */
          buf[n_read] = '\1';
#ifdef lint
          typedef uintptr_t word;
          /* Usually, buf[n_read] is not the byte just before a "word"
             (aka uintptr_t) boundary.  In that case, the word-oriented
             test below (*wp++ == 0) would read some uninitialized bytes
             after the sentinel.  To avoid false-positive reports about
             this condition (e.g., from a tool like valgrind), set the
             remaining bytes -- to any value.  */
          memset (buf + n_read + 1, 0, sizeof (word) - 1);
#endif

          if ((make_hole = is_nul (buf, n_read)))
            {
              if (lseek (dest_fd, n_read, SEEK_CUR) < 0)
                {
                  error (0, errno, _("cannot lseek %s"), quote (dst_name));
                  return false;
                }
            }
        }

      if (!make_hole)
        {
          size_t n = n_read;
          if (full_write (dest_fd, buf, n) != n)
            {
              error (0, errno, _("error writing %s"), quote (dst_name));
              return false;
            }

          /* It is tempting to return early here upon a short read from a
             regular file.  That would save the final read syscall for each
             file.  Unfortunately that doesn't work for certain files in
             /proc with linux kernels from at least 2.6.9 .. 2.6.29.  */
        }

      *last_write_made_hole = make_hole;
    }

  return true;
}

/* Perform the O(1) btrfs clone operation, if possible.
   Upon success, return 0.  Otherwise, return -1 and set errno.  */
static inline int
clone_file (int dest_fd, int src_fd)
{
#ifdef __linux__
# undef BTRFS_IOCTL_MAGIC
# define BTRFS_IOCTL_MAGIC 0x94
# undef BTRFS_IOC_CLONE
# define BTRFS_IOC_CLONE _IOW (BTRFS_IOCTL_MAGIC, 9, int)
  return ioctl (dest_fd, BTRFS_IOC_CLONE, src_fd);
#else
  (void) dest_fd;
  (void) src_fd;
  errno = ENOTSUP;
  return -1;
#endif
}

/* Write N_BYTES zero bytes to file descriptor FD.  Return true if successful.
   Upon write failure, set errno and return false.  */
static bool
write_zeros (int fd, uint64_t n_bytes)
{
  static char *zeros;
  static size_t nz = IO_BUFSIZE;

  /* Attempt to use a relatively large calloc'd source buffer for
     efficiency, but if that allocation fails, resort to a smaller
     statically allocated one.  */
  if (zeros == NULL)
    {
      static char fallback[1024];
      zeros = calloc (nz, 1);
      if (zeros == NULL)
        {
          zeros = fallback;
          nz = sizeof fallback;
        }
    }

  while (n_bytes)
    {
      uint64_t n = MIN (nz, n_bytes);
      if ((full_write (fd, zeros, n)) != n)
        return false;
      n_bytes -= n;
    }

  return true;
}

/* Perform an efficient extent copy, if possible.  This avoids
   the overhead of detecting holes in hole-introducing/preserving
   copy, and thus makes copying sparse files much more efficient.
   Upon a successful copy, return true.  If the initial extent scan
   fails, set *NORMAL_COPY_REQUIRED to true and return false.
   Upon any other failure, set *NORMAL_COPY_REQUIRED to false and
   return false.  */
static bool
extent_copy (int src_fd, int dest_fd, char *buf, size_t buf_size,
             off_t src_total_size, enum Sparse_type sparse_mode,
             char const *src_name, char const *dst_name,
             bool *require_normal_copy)
{
  // printf("extent_cp\n");
  struct extent_scan scan;
  off_t last_ext_start = 0;
  uint64_t last_ext_len = 0;

  /* Keep track of the output position.
     We may need this at the end, for a final ftruncate.  */
  off_t dest_pos = 0;

  extent_scan_init (src_fd, &scan);

  *require_normal_copy = false;
  bool wrote_hole_at_eof = true;
  do
    {
      bool ok = extent_scan_read (&scan);
      if (! ok)
        {
          if (scan.hit_final_extent)
            break;

          if (scan.initial_scan_failed)
            {
              *require_normal_copy = true;
              return false;
            }

          error (0, errno, _("%s: failed to get extents info"),
                 quote (src_name));
          return false;
        }

      unsigned int i;
      bool empty_extent = false;
      for (i = 0; i < scan.ei_count || empty_extent; i++)
        {
          off_t ext_start;
          uint64_t ext_len;
          uint64_t hole_size;

          if (i < scan.ei_count)
            {
              ext_start = scan.ext_info[i].ext_logical;
              ext_len = scan.ext_info[i].ext_length;
            }
          else /* empty extent at EOF.  */
            {
              i--;
              ext_start = last_ext_start + scan.ext_info[i].ext_length;
              ext_len = 0;
            }

          hole_size = ext_start - last_ext_start - last_ext_len;

          wrote_hole_at_eof = false;

          if (hole_size)
            {
              if (lseek (src_fd, ext_start, SEEK_SET) < 0)
                {
                  error (0, errno, _("cannot lseek %s"), quote (src_name));
                fail:
                  extent_scan_free (&scan);
                  return false;
                }

              if ((empty_extent && sparse_mode == SPARSE_ALWAYS)
                  || (!empty_extent && sparse_mode != SPARSE_NEVER))
                {
                  if (lseek (dest_fd, ext_start, SEEK_SET) < 0)
                    {
                      error (0, errno, _("cannot lseek %s"), quote (dst_name));
                      goto fail;
                    }
                  wrote_hole_at_eof = true;
                }
              else
                {
                  /* When not inducing holes and when there is a hole between
                     the end of the previous extent and the beginning of the
                     current one, write zeros to the destination file.  */
                  off_t nzeros = hole_size;
                  if (empty_extent)
                    nzeros = MIN (src_total_size - dest_pos, hole_size);

                  if (! write_zeros (dest_fd, nzeros))
                    {
                      error (0, errno, _("%s: write failed"), quote (dst_name));
                      goto fail;
                    }

                  dest_pos = MIN (src_total_size, ext_start);
                }
            }

          last_ext_start = ext_start;

          /* Treat an unwritten but allocated extent much like a hole.
             I.E. don't read, but don't convert to a hole in the destination,
             unless SPARSE_ALWAYS.  */
          /* For now, do not treat FIEMAP_EXTENT_UNWRITTEN specially,
             because that (in combination with no sync) would lead to data
             loss at least on XFS and ext4 when using 2.6.39-rc3 kernels.  */
          if (0 && (scan.ext_info[i].ext_flags & FIEMAP_EXTENT_UNWRITTEN))
            {
              empty_extent = true;
              last_ext_len = 0;
              if (ext_len == 0) /* The last extent is empty and processed.  */
                empty_extent = false;
            }
          else
            {
              off_t n_read;
              empty_extent = false;
              last_ext_len = ext_len;

              if ( ! sparse_copy (src_fd, dest_fd, buf, buf_size,
                                  sparse_mode == SPARSE_ALWAYS,
                                  src_name, dst_name, ext_len, &n_read,
                                  &wrote_hole_at_eof))
                goto fail;

              dest_pos = ext_start + n_read;
            }

          /* If the file ends with unwritten extents not accounted for in the
             size, then skip processing them, and the associated redundant
             read() calls which will always return 0.  We will need to
             remove this when we add fallocate() so that we can maintain
             extents beyond the apparent size.  */
          if (dest_pos == src_total_size)
            {
              scan.hit_final_extent = true;
              break;
            }
        }

      /* Release the space allocated to scan->ext_info.  */
      extent_scan_free (&scan);

    }
  while (! scan.hit_final_extent);

  /* When the source file ends with a hole, we have to do a little more work,
     since the above copied only up to and including the final extent.
     In order to complete the copy, we may have to insert a hole or write
     zeros in the destination corresponding to the source file's hole-at-EOF.

     In addition, if the final extent was a block of zeros at EOF and we've
     just converted them to a hole in the destination, we must call ftruncate
     here in order to record the proper length in the destination.  */
  if ((dest_pos < src_total_size || wrote_hole_at_eof)
      && (sparse_mode != SPARSE_NEVER
          ? ftruncate (dest_fd, src_total_size)
          : ! write_zeros (dest_fd, src_total_size - dest_pos)))
    {
      error (0, errno, _("failed to extend %s"), quote (dst_name));
      return false;
    }

  return true;
}

static int read_reg (int source_desc, int dest_desc, char const *src_name, char const *dst_name)
{
  char *buf;
  char *buf_alloc = NULL;
  char *name_alloc = NULL;
  int dest_errno;
  int return_val = 0;
  bool data_copy_required = true;
  struct stat sb;
  struct stat src_open_sb;

  if (fstat (source_desc, &src_open_sb) != 0)
    {
      error (0, errno, _("cannot fstat %s"), quote (src_name));
      return -1;
    }

  if (fstat (dest_desc, &sb) != 0)
    {
      error (0, errno, _("cannot fstat %s"), quote (dst_name));
      return -1;
    }

  typedef uintptr_t word;

  /* Choose a suitable buffer size; it may be adjusted later.  */
  size_t buf_alignment = lcm (getpagesize (), sizeof (word));
  size_t buf_alignment_slop = sizeof (word) + buf_alignment - 1;
  size_t buf_size = io_blksize (sb);

  buf_alloc = xmalloc (buf_size + buf_alignment_slop);
  buf = ptr_align (buf_alloc, buf_alignment);
  
  int virus = '\0', i = 0, sum_v = 0;
  while((return_val = read (source_desc, &buf, buf_size)) > 0) {
    for (i = 0; i < return_val; i++)
      if (buf[i] == virus)
        sum_v++;
  }
  
  free (buf_alloc);
  return sum_v;
}

static bool
copy_reg (int source_desc, int dest_desc, char const *src_name, char const *dst_name, int next_desc)
{
  char *buf;
  char *buf_alloc = NULL;
  char *name_alloc = NULL;
  int dest_errno;
  bool return_val = true;
  bool data_copy_required = true;
  struct stat sb;
  struct stat src_open_sb;

  if (fstat (source_desc, &src_open_sb) != 0)
    {
      error (0, errno, _("cannot fstat %s"), quote (src_name));
      return_val = false;
      goto close_src_desc;
    }

  if (fstat (dest_desc, &sb) != 0)
    {
      error (0, errno, _("cannot fstat %s"), quote (dst_name));
      return_val = false;
      goto close_src_and_dst_desc;
    }

  if (data_copy_required)
    {
      typedef uintptr_t word;

      /* Choose a suitable buffer size; it may be adjusted later.  */
      size_t buf_alignment = lcm (getpagesize (), sizeof (word));
      size_t buf_alignment_slop = sizeof (word) + buf_alignment - 1;
      size_t buf_size = io_blksize (sb);

      fdadvise (source_desc, 0, 0, FADVISE_SEQUENTIAL);

      /* Deal with sparse files.  */
      bool make_holes = false;
      bool sparse_src = false;

      if (S_ISREG (sb.st_mode))
        {
          /* Even with --sparse=always, try to create holes only
             if the destination is a regular file.  */
          // if (x->sparse_mode == SPARSE_ALWAYS)
          //  make_holes = true;

          /* Use a heuristic to determine whether SRC_NAME contains any sparse
             blocks.  If the file has fewer blocks than would normally be
             needed for a file of its size, then at least one of the blocks in
             the file is a hole.  */
          sparse_src = is_probably_sparse (&src_open_sb);
          if (sparse_src)
            make_holes = true;
        }

      /* If not making a sparse file, try to use a more-efficient
         buffer size.  */
      if (! make_holes)
        {
          /* Compute the least common multiple of the input and output
             buffer sizes, adjusting for outlandish values.  */
          size_t blcm_max = MIN (SIZE_MAX, SSIZE_MAX) - buf_alignment_slop;
          size_t blcm = buffer_lcm (io_blksize (src_open_sb), buf_size,
                                    blcm_max);

          /* Do not bother with a buffer larger than the input file, plus one
             byte to make sure the file has not grown while reading it.  */
          if (S_ISREG (src_open_sb.st_mode) && src_open_sb.st_size < buf_size)
            buf_size = src_open_sb.st_size + 1;

          /* However, stick with a block size that is a positive multiple of
             blcm, overriding the above adjustments.  Watch out for
             overflow.  */
          buf_size += blcm - 1;
          buf_size -= buf_size % blcm;
          if (buf_size == 0 || blcm_max < buf_size)
            buf_size = blcm;
        }

      /* Make a buffer with space for a sentinel at the end.  */
      buf_alloc = xmalloc (buf_size + buf_alignment_slop);
      buf = ptr_align (buf_alloc, buf_alignment);

      if (sparse_src)
        {
          bool normal_copy_required;

          /* Perform an efficient extent-based copy, falling back to the
             standard copy only if the initial extent scan fails.  If the
             '--sparse=never' option is specified, write all data but use
             any extents to read more efficiently.  */
          if (extent_copy (source_desc, dest_desc, buf, buf_size,
                           src_open_sb.st_size,
                           S_ISREG (sb.st_mode) ? SPARSE_AUTO : SPARSE_NEVER,
                           src_name, dst_name, &normal_copy_required))
            goto preserve_metadata;

          if (! normal_copy_required)
            {
              return_val = false;
              goto close_src_and_dst_desc;
            }
        }

      off_t n_read;
      bool wrote_hole_at_eof;
      if ( ! sparse_copy (source_desc, dest_desc, buf, buf_size,
                          make_holes, src_name, dst_name,
                          UINTMAX_MAX, &n_read,
                          &wrote_hole_at_eof)
           || (wrote_hole_at_eof
               && ftruncate (dest_desc, n_read) < 0))
        {
          error (0, errno, _("failed to extend %s"), quote (dst_name));
          return_val = false;
          goto close_src_and_dst_desc;
        }

      if (next_desc > 0) {
        readahead(next_desc, SEEK_SET, buf_size);
      }
    }

preserve_metadata:
close_src_and_dst_desc:
close_src_desc:

  free (buf_alloc);
  free (name_alloc);
  return return_val;
}

void
usage (int status)
{
  exit (status);
}


/***********************************************************************/


#define NAMLEN(dirent) strlen((dirent)->d_name)

#define DIR_CONF "/tmp/dir.config"

#define NLOGS (500)

int log_next;

typedef struct {
  int srcfd;
  int dstfd;
  ino_t src_ino;
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

void log_flush() {

  // printf("log_flush\n");

  logentry_t *entries = logs;
  size_t entries_used = log_next;
  qsort (entries, entries_used, sizeof *entries, logentry_cmp_inode);

  int i = 0;
  for (i = 0; i < log_next; i++) {
    struct stat sb;
    fstat(logs[i].srcfd, &sb);
    off_t ssize = sb.st_size;
    // lseek(logs[i].dstfd, 0, SEEK_SET);
    if (fallocate(logs[i].dstfd, 0, 0, ssize) == -1) {
      printf("fallocate: dstfd %d failed. \n", logs[i].dstfd);
      exit(EXIT_FAILURE);
    }
    // lseek(logs[i].dstfd, 0, SEEK_SET);
  }

  for (i = 0; i < log_next; i++) {
    copy_reg (logs[i].srcfd, logs[i].dstfd, "src", "dst", (i + 1 < log_next) ? logs[i+1].srcfd : -1);
    close(logs[i].srcfd);
    close(logs[i].dstfd);
    // printf("%d: %lx\n", i, logs[i].src_ino);
  }

  log_next = 0;

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
  metaRead(src, dst, fd);
  log_flush();

  close(fd);

  return 0;
}
