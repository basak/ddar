/*
   Copyright 2010-2011 True Blue Logic Ltd
   
   This program is free software: you can redistribute it and/or modify
   it under the terms of version 3 of the GNU General Public License as
   published by the Free Software Foundation.
   
   This program is distributed in the hope that it will be useful, but
   WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
   General Public License for more details.
   
   You should have received a copy of the GNU General Public License
   along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

#ifdef HAVE_FADVISE
#define _XOPEN_SOURCE 600
#endif
#define _FILE_OFFSET_BITS 64
#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include <errno.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <inttypes.h>
#ifdef HAVE_AIO
#include <aio.h>
#endif
#include <string.h>
#include <setjmp.h>

#include "rabin.h"

#include "scan.h"

#define likely(x) __builtin_expect(x, 1)
#define unlikely(x) __builtin_expect(x, 0)

struct scan_ctx {
    /* The main read buffer itself */
    unsigned char *buffer[3];
    unsigned char *buffer_end;
    int buffer_size;

    /* Reading the source file */
    int fd;
    unsigned long long source_offset;

    unsigned char *p;
    int bytes_left;

    int eof; /* if EOF has been read */

    struct rabin_ctx *rabin_ctx;

    int window_size;
    int target_chunk_size;
    int minimum_chunk_size;
    int maximum_chunk_size;

#ifdef HAVE_AIO
    struct aiocb aiocb;
#endif
    unsigned char *io_destination;

    void (*start_io)(struct scan_ctx *, unsigned char *);
    void (*finish_io)(struct scan_ctx *);

    jmp_buf jmp_env;
};

static int retry_read(struct scan_ctx *scan, unsigned char *p,
		      int bytes_to_read) {
    ssize_t result;
    unsigned char *start, *end;

    start = p;
    end = p + bytes_to_read;
    while (p < end) {
	do {
	    result = read(scan->fd, p, end - p);
	} while (result < 0 && errno == EINTR);
	if (result < 0) {
	    longjmp(scan->jmp_env, 1);
	} else if (!result) {
	    scan->eof = 1;
	    break;
	}
	p += result;
    }
    return p - start;
}

static void start_sync_io(struct scan_ctx *scan, unsigned char *buffer) {
    scan->io_destination = buffer;
}

static void finish_sync_io(struct scan_ctx *scan) {
    int bytes_read;

    bytes_read = retry_read(scan, scan->io_destination, scan->buffer_size / 3);
#ifdef HAVE_FADVISE
    posix_fadvise(scan->fd, scan->source_offset, bytes_read, POSIX_FADV_DONTNEED);
#endif
    scan->source_offset += bytes_read;
    scan->bytes_left += bytes_read;
}

#ifdef HAVE_AIO

static void start_aio(struct scan_ctx *scan, unsigned char *buffer) {
    memset(&scan->aiocb, 0, sizeof(scan->aiocb));
    scan->aiocb.aio_buf = scan->io_destination = buffer;
    scan->aiocb.aio_nbytes = scan->buffer_size / 3;
    scan->aiocb.aio_fildes = scan->fd;
    scan->aiocb.aio_offset = scan->source_offset;
    if (aio_read(&scan->aiocb)) {
	longjmp(scan->jmp_env, 1);
    }
}

static void finish_aio(struct scan_ctx *scan) {
    int bytes_read;
    off_t required_offset, lseek_result;
    int result;

    const struct aiocb *cblist[1] = { &scan->aiocb };
    do {
	result = aio_suspend(cblist, 1, 0);
    } while (result < 0 && errno == EINTR);
    if (result) {
	longjmp(scan->jmp_env, 1);
    }
    if (aio_error(&scan->aiocb)) {
	longjmp(scan->jmp_env, 1);
    }
    bytes_read = aio_return(&scan->aiocb);
    if (!bytes_read) {
	scan->eof = 1;
	return;
    } else if (bytes_read != scan->buffer_size / 3) {
	required_offset = scan->source_offset + bytes_read;
	/* If we have been reading from a regular file, then the current
	   offset will not match the data we have read in using aio_read,
	   since that doesn't alter the offset, so we need to seek to the
	   correct offset. However, we could have been reading from a pipe, in
	   which case this lseek will fail because of ESPIPE, which is fine
	   since there is no "offset" to be misaligned and we should
	   still fill the buffer. Any other failure is an error */
	lseek_result = lseek(scan->fd, required_offset, SEEK_SET);
	if ((lseek_result == required_offset) ||
		(lseek_result < 0 && errno == ESPIPE))
	    bytes_read += retry_read(scan,
				     scan->io_destination + bytes_read,
				     scan->buffer_size / 3 - bytes_read);
	else
	    longjmp(scan->jmp_env, 1);
    }

#ifdef HAVE_FADVISE
    posix_fadvise(scan->fd, scan->source_offset, bytes_read, POSIX_FADV_DONTNEED);
#endif
    scan->source_offset += bytes_read;
    scan->bytes_left += bytes_read;
}

#endif /* #ifdef HAVE_AIO */

static inline void read_more_data(struct scan_ctx *scan) {
    unsigned char *head = scan->p + scan->bytes_left;
    unsigned char *readahead_buffer;

    if (scan->eof)
	return;

    assert(head == scan->buffer[0] || head == scan->buffer[1] ||
	    head == scan->buffer[2] ||
	    head == scan->buffer_end);

    if (head == scan->buffer_end)
	head = scan->buffer[0];

    if (head == scan->buffer[0])
	readahead_buffer = scan->buffer[1];
    else if (head == scan->buffer[1])
	readahead_buffer = scan->buffer[2];
    else if (head == scan->buffer[2])
	readahead_buffer = scan->buffer[0];

    scan->finish_io(scan);

    if (!scan->eof)
	scan->start_io(scan, readahead_buffer);
}

static inline void boundary_hit(struct scan_ctx *scan, unsigned char *boundary,
	int chunk_size, struct scan_chunk_data *chunk_data) {
    if (scan->buffer[0] + chunk_size > boundary) {
	/* chunk is wrapped */
	chunk_data[0].buf = boundary + scan->buffer_size - chunk_size;
	chunk_data[0].size = chunk_size - (boundary - scan->buffer[0]);
	chunk_data[1].buf = scan->buffer[0];
	chunk_data[1].size = boundary - scan->buffer[0];
    } else {
	/* chunk is not wrapped */
	chunk_data[0].buf = boundary - chunk_size;
	chunk_data[0].size =	chunk_size;
	chunk_data[1].buf = 0;
	chunk_data[1].size = 0;
    }
}

int scan_read_chunk(struct scan_ctx *scan,
		    struct scan_chunk_data *chunk_data) {
    uint32_t hash;
    int current_chunk_size;
    int temp;
    unsigned char *old;

    if (setjmp(scan->jmp_env))
	return 0;

    if (unlikely(scan->bytes_left <= scan->minimum_chunk_size)) {
	if (unlikely(scan->eof)) {
	    boundary_hit(scan, scan->p + scan->bytes_left, scan->bytes_left,
		         chunk_data);
	    return SCAN_CHUNK_FOUND | SCAN_CHUNK_LAST;
	} else {
	    read_more_data(scan);
	    /* Check again in case of EOF */
	    if (unlikely(scan->bytes_left <= scan->minimum_chunk_size)) {
		assert(scan->eof);
		boundary_hit(scan, scan->p + scan->bytes_left,
			scan->bytes_left, chunk_data);
		return SCAN_CHUNK_FOUND | SCAN_CHUNK_LAST;
	    }
	}
    }
    assert(scan->bytes_left >= scan->minimum_chunk_size);

    /* Move forward by the minimum_chunk_size */
    scan->p += scan->minimum_chunk_size;
    scan->bytes_left -= scan->minimum_chunk_size;
    if (unlikely(scan->p >= scan->buffer_end))
	scan->p -= scan->buffer_size;
    current_chunk_size = scan->minimum_chunk_size;

    /* Calculate the first hash (aligned to the end of the minimum chunk size)
     * */
    if(unlikely(scan->p < scan->buffer[0] + scan->window_size)) {
	/* scan->p has wrapped; hash calculation must take place in two
	 * sections */
	temp = scan->window_size - (scan->p - scan->buffer[0]);
	/* temp is the amount before the wrap */
	old = scan->buffer_end - temp;
	hash = rabin_hash_split(scan->rabin_ctx, old, temp, scan->buffer[0]);
    } else {
	old = scan->p - scan->window_size;
	hash = rabin_hash(scan->rabin_ctx, old);
    }

    while (1) {
	if (unlikely((((scan->target_chunk_size-1) & hash) ==
		    scan->target_chunk_size-1)
		|| (current_chunk_size >= scan->maximum_chunk_size))) {
	    /* Hash has matched to a boundary, or we have got to the maximum
	     * chunk size */
	    boundary_hit(scan, scan->p, current_chunk_size,
			 chunk_data);
	    return SCAN_CHUNK_FOUND;
	} else {
	    /* Move up and calculate the next hash */
	    if (unlikely(!scan->bytes_left)) {
		if (unlikely(scan->eof)) {
		    boundary_hit(scan, scan->p, current_chunk_size,
				 chunk_data);
		    return SCAN_CHUNK_FOUND | SCAN_CHUNK_LAST;
		}
		read_more_data(scan);
		if (unlikely(!scan->bytes_left)) {
		    assert(scan->eof);
		    boundary_hit(scan, scan->p, current_chunk_size,
				 chunk_data);
		    return SCAN_CHUNK_FOUND | SCAN_CHUNK_LAST;
		}
	    }
	    hash = rabin_hash_next(scan->rabin_ctx, hash, *old, *scan->p);
	    scan->p += 1;
	    if (unlikely(scan->p >= scan->buffer_end))
		scan->p -= scan->buffer_size;
	    old += 1;
	    if (unlikely(old >= scan->buffer_end))
		old -= scan->buffer_size;
	    scan->bytes_left -= 1;
	    current_chunk_size += 1;
	}
    }
}

struct scan_ctx *scan_init(void) {
    struct scan_ctx *scan;

    scan = malloc(sizeof(struct scan_ctx));
    if (!scan)
	goto unwind0;

    scan->buffer_size = 3 * (1<<24);
    scan->buffer[0] = malloc(scan->buffer_size);
    if (!scan->buffer[0])
	goto unwind1;

    scan->buffer[1] = scan->buffer[0] + scan->buffer_size/3;
    scan->buffer[2] = scan->buffer[1] + scan->buffer_size/3;
    scan->buffer_end = scan->buffer[0] + scan->buffer_size;
    scan->p = scan->buffer[0];
    scan->eof = 0;
    scan->bytes_left = 0;
    scan->source_offset = 0;

    scan->window_size = 48;
    scan->target_chunk_size = 1 << 18;
    scan->minimum_chunk_size = 1 << 16;
    scan->maximum_chunk_size = 1 << 24;
    scan->rabin_ctx = rabin_init(1103515245, scan->window_size);
    if (!scan->rabin_ctx)
	goto unwind2;

    scan->start_io = start_sync_io;
    scan->finish_io = finish_sync_io;

    return scan;

unwind2:
    rabin_free(scan->rabin_ctx);
unwind1:
    free(scan->buffer[0]);
unwind0:
    return 0;
}

void scan_free(struct scan_ctx *scan) {
    rabin_free(scan->rabin_ctx);
    free(scan->buffer[0]);
    free(scan);
}

void scan_set_fd(struct scan_ctx *scan, int fd) {
    scan->fd = fd;
}

#ifdef HAVE_AIO

void scan_set_aio(struct scan_ctx *scan) {
    scan->start_io = start_aio;
    scan->finish_io = finish_aio;
}

#else

void scan_set_aio(struct scan_ctx *scan) {}

#endif

int scan_begin(struct scan_ctx *scan) {
    if (setjmp(scan->jmp_env))
	return 0;

    scan->start_io(scan, scan->buffer[0]);
    scan->finish_io(scan);
    if (!scan->eof)
	scan->start_io(scan, scan->buffer[1]);

    return 1;
}

/* vim: set ts=8 sw=4 sts=4 cindent : */
