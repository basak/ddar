#define _GNU_SOURCE
#define _FILE_OFFSET_BITS 64
#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include <errno.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <byteswap.h>
#include <inttypes.h>

#include "sqlite3.h"

#include "rabin.h"
#include "sha2.h"

#define likely(x) __builtin_expect(x, 1)
#define unlikely(x) __builtin_expect(x, 0)

struct scan_t {
    /* The main read buffer itself */
    unsigned char *buffer;
    int buffer_size;

    unsigned char *buffer2; /* half-way through the buffer for double-buffering */
    unsigned char *buffer3; /* one past the end of the buffer for double-buffering */

    /* Reading the read buffer */
    int fd;

    unsigned char *p;
    int bytes_left;

    int eof; /* if EOF has been read */

    struct rabin_t *rabin_ctx;

    int window_size;
    int target_chunk_size;
    int minimum_chunk_size;
    int maximum_chunk_size;

    sqlite3 *db;
    sqlite3_stmt *db_insert_stmt;

    int64_t offset;
};

static void print_sqlite3_error(sqlite3 *db) {
    fputs("sqlite3: ", stderr);
    fputs(sqlite3_errmsg(db), stderr);
    fputc('\n', stderr);
}

static inline void fill_buffer(struct scan_t *scan, unsigned char *head) {
    int bytes_to_fill = scan->buffer_size / 2;
    int result;

    while (bytes_to_fill) {
	result = TEMP_FAILURE_RETRY(read(scan->fd, head, bytes_to_fill));
	if (result < 0) {
	    perror("read");
	    exit(1);
	} else if (!result) {
	    scan->eof = 1;
	    return;
	}
	bytes_to_fill -= result;
	head += result;
	scan->bytes_left += result;
    }
}

static inline void read_more_data(struct scan_t *scan) {
    unsigned char *head = scan->p + scan->bytes_left;

    assert(head == scan->buffer || head == scan->buffer2 ||
	    head == scan->buffer3);

    if (head == scan->buffer3 || head == scan->buffer) {
	/* Read into first buffer */
	fill_buffer(scan, scan->buffer);
    } else {
	/* Read into second buffer */
	fill_buffer(scan, scan->buffer2);
    }
}

/* boundary points to where the next chunk would be; going backwards to find
 * the start point may involve wrapping around the circular buffer */
static inline void boundary_hit(struct scan_t *scan, unsigned char *boundary,
	int chunk_size) {
    sha256_ctx sha256;
    unsigned char sha256_digest[32];
    char sha256_digest_string[65];

    sha256_init(&sha256);
    if (scan->buffer + chunk_size > boundary) {
	/* chunk is wrapped */
	sha256_update(&sha256, boundary + scan->buffer_size - chunk_size,
		chunk_size - (boundary - scan->buffer));
	sha256_update(&sha256, scan->buffer, boundary - scan->buffer);
    } else {
	sha256_update(&sha256, boundary-chunk_size, chunk_size);
    }
    sha256_final(&sha256, sha256_digest);
    sprintf(sha256_digest_string, "%016" PRIx64
	                          "%016" PRIx64
	                          "%016" PRIx64
	                          "%016" PRIx64,
	    bswap_64(*(uint64_t *)&sha256_digest[0]),
	    bswap_64(*(uint64_t *)&sha256_digest[8]),
	    bswap_64(*(uint64_t *)&sha256_digest[16]),
	    bswap_64(*(uint64_t *)&sha256_digest[24]));
    if (sqlite3_bind_text(scan->db_insert_stmt, 1, sha256_digest_string, -1,
		SQLITE_STATIC) != SQLITE_OK) {
	print_sqlite3_error(scan->db);
	abort();
    }
    if (sqlite3_bind_int64(scan->db_insert_stmt, 3, scan->offset) != SQLITE_OK)
    {
	print_sqlite3_error(scan->db);
	abort();
    }
    if (sqlite3_bind_int(scan->db_insert_stmt, 4, chunk_size) != SQLITE_OK) {
	print_sqlite3_error(scan->db);
	abort();
    }
    if (sqlite3_step(scan->db_insert_stmt) != SQLITE_DONE) {
	print_sqlite3_error(scan->db);
	abort();
    }
    if (sqlite3_reset(scan->db_insert_stmt) != SQLITE_OK) {
	print_sqlite3_error(scan->db);
	abort();
    }
#if 0
    printf("%016lx%016lx%016lx%016lx %d\n",
	    bswap64(*(uint64_t *)&sha256_digest[0]),
	    bswap64(*(uint64_t *)&sha256_digest[8]),
	    bswap64(*(uint64_t *)&sha256_digest[16]),
	    bswap64(*(uint64_t *)&sha256_digest[24]),
	    chunk_size);
#endif

    scan->offset += chunk_size;
}

/* Returns 1 for go-again, 0 for EOF reached */
static inline int find_chunk_boundary(struct scan_t *scan) {
    uint32_t hash;
    int current_chunk_size;
    int temp;
    unsigned char *old;

    if (unlikely(scan->bytes_left <= scan->minimum_chunk_size)) {
	if (unlikely(scan->eof)) {
	    boundary_hit(scan, scan->p + scan->bytes_left, scan->bytes_left);
	    return 0;
	} else {
	    read_more_data(scan);
	    /* Check again in case of EOF */
	    if (unlikely(scan->bytes_left <= scan->minimum_chunk_size)) {
		assert(scan->eof);
		boundary_hit(scan, scan->p + scan->bytes_left,
			scan->bytes_left);
		return 0;
	    }
	}
    }
    assert(scan->bytes_left >= scan->minimum_chunk_size);

    /* Move forward by the minimum_chunk_size */
    scan->p += scan->minimum_chunk_size;
    scan->bytes_left -= scan->minimum_chunk_size;
    if (unlikely(scan->p >= scan->buffer3))
	scan->p -= scan->buffer_size;
    current_chunk_size = scan->minimum_chunk_size;

    /* Calculate the first hash (aligned to the end of the minimum chunk size)
     * */
    if(unlikely(scan->p < scan->buffer + scan->window_size)) {
	/* scan->p has wrapped; hash calculation must take place in two
	 * sections */
	temp = scan->window_size - (scan->p - scan->buffer);
	/* temp is the amount before the wrap */
	old = scan->buffer3 - temp;
	hash = rabin_hash_split(scan->rabin_ctx, old, temp, scan->buffer);
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
	    boundary_hit(scan, scan->p, current_chunk_size);
	    return 1;
	} else {
	    /* Move up and calculate the next hash */
	    if (unlikely(!scan->bytes_left)) {
		if (unlikely(scan->eof)) {
		    boundary_hit(scan, scan->p, current_chunk_size);
		    return 0;
		}
		read_more_data(scan);
		if (unlikely(!scan->bytes_left)) {
		    assert(scan->eof);
		    boundary_hit(scan, scan->p, current_chunk_size);
		    return 0;
		}
	    }
	    hash = rabin_hash_next(scan->rabin_ctx, hash, *old, *scan->p);
	    scan->p += 1;
	    if (unlikely(scan->p >= scan->buffer3))
		scan->p -= scan->buffer_size;
	    old += 1;
	    if (unlikely(old >= scan->buffer3))
		old -= scan->buffer_size;
	    scan->bytes_left -= 1;
	    current_chunk_size += 1;
	}
    }
}

int main(int argc, char **argv) {
    struct scan_t scan;

    scan.fd = open(argv[1], O_RDONLY);
    if (scan.fd < 0) {
	perror("open");
	return EXIT_FAILURE;
    }
    if (sqlite3_open(argv[2], &scan.db) != SQLITE_OK) {
	print_sqlite3_error(scan.db);
	return EXIT_FAILURE;
    }

    if (sqlite3_exec(scan.db, "BEGIN", NULL, NULL, NULL) != SQLITE_OK) {
	print_sqlite3_error(scan.db);
	return EXIT_FAILURE;
    }

    if (sqlite3_prepare(scan.db, "INSERT INTO chunk (sha256, tag, offset, length) VALUES (?, ?, ?, ?)", -1, &scan.db_insert_stmt, NULL) != SQLITE_OK) {
	print_sqlite3_error(scan.db);
	return EXIT_FAILURE;
    }

    if (sqlite3_bind_text(scan.db_insert_stmt, 2, argv[1], -1, SQLITE_STATIC)
	    != SQLITE_OK) {
	print_sqlite3_error(scan.db);
	return EXIT_FAILURE;
    }

    scan.buffer_size = 1<<24;
    scan.buffer = malloc(scan.buffer_size);
    if (!scan.buffer) {
	perror("malloc");
	return EXIT_FAILURE;
    }
    scan.buffer2 = scan.buffer + scan.buffer_size/2;
    scan.buffer3 = scan.buffer + scan.buffer_size;
    scan.p = scan.buffer;
    scan.bytes_left = 0;
    scan.eof = 0;

    scan.window_size = 48;
    scan.target_chunk_size = 1 << 16;
    scan.minimum_chunk_size = 1 << 14;
    scan.maximum_chunk_size = 1 << 18;
    scan.rabin_ctx = rabin_init(1103515245, scan.window_size);
    if (!scan.rabin_ctx) {
	perror("rabin_init");
	return EXIT_FAILURE;
    }

    scan.offset = 0;

    while (find_chunk_boundary(&scan));

    sqlite3_finalize(scan.db_insert_stmt);

    if (sqlite3_exec(scan.db, "COMMIT", NULL, NULL, NULL) != SQLITE_OK) {
	print_sqlite3_error(scan.db);
	return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}

/* vim: set ts=8 sw=4 sts=4 cindent : */
