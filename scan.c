#define _GNU_SOURCE
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
#include <byteswap.h>
#include <inttypes.h>

#include "sqlite3.h"

#include "rabin.h"
#include "sha2.h"

#define likely(x) __builtin_expect(x, 1)
#define unlikely(x) __builtin_expect(x, 0)

struct scan_t {
    /* The main read buffer itself */
    unsigned char *buffer[2];
    unsigned char *buffer_end;
    int buffer_size;

    /* Reading the source file */
    int fd;
    unsigned long long source_bytes_left;
    unsigned long long source_offset;
    int page_size;

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

static inline void read_more_data(struct scan_t *scan) {
    unsigned char *head = scan->p + scan->bytes_left;
    unsigned char *buffer;
    int bytes_read;

    assert(head == scan->buffer[0] || head == scan->buffer[1] ||
	    head == scan->buffer_end);

    if (scan->source_bytes_left <= 0) {
	scan->eof = 1;
	return;
    }

    if (head == scan->buffer_end || head == scan->buffer[0]) {
	/* Read into first buffer */
	buffer = scan->buffer[0];
    } else {
	/* Read into second buffer */
	buffer = scan->buffer[1];
    }
    if (remap_file_pages(buffer, scan->buffer_size / 2, 0, scan->source_offset / scan->page_size, 0)) {
	perror("remap_file_pages");
	abort();
    }
    madvise(buffer, scan->buffer_size / 2, MADV_WILLNEED);

    if (scan->source_bytes_left >= scan->buffer_size / 2)
	bytes_read = scan->buffer_size / 2;
    else
	bytes_read = scan->source_bytes_left;

    scan->source_bytes_left -= bytes_read;
    scan->source_offset += bytes_read;
    scan->bytes_left += bytes_read;
}

/* boundary points to where the next chunk would be; going backwards to find
 * the start point may involve wrapping around the circular buffer */
static inline void boundary_hit(struct scan_t *scan, unsigned char *boundary,
	int chunk_size) {
    sha256_ctx sha256;
    unsigned char sha256_digest[32];
    char sha256_digest_string[65];

    sha256_init(&sha256);
    if (scan->buffer[0] + chunk_size > boundary) {
	/* chunk is wrapped */
	sha256_update(&sha256, boundary + scan->buffer_size - chunk_size,
		chunk_size - (boundary - scan->buffer[0]));
	sha256_update(&sha256, scan->buffer[0], boundary - scan->buffer[0]);
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

int main(int argc, char **argv) {
    struct scan_t scan;
    struct stat st;

    scan.page_size = sysconf(_SC_PAGESIZE);
    if (scan.page_size < 0) {
	perror("sysconf");
	return EXIT_FAILURE;
    }

    scan.fd = open(argv[1], O_RDWR);
    if (scan.fd < 0) {
	perror("open");
	return EXIT_FAILURE;
    }
    if (fstat(scan.fd, &st) < 0) {
	perror("fstat");
	return EXIT_FAILURE;
    }
    scan.source_bytes_left = st.st_size;
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
    scan.buffer[0] = mmap(0, scan.buffer_size, PROT_READ, MAP_SHARED, scan.fd, 0);
    if (scan.buffer[0] == MAP_FAILED) {
	perror("mmap");
	return EXIT_FAILURE;
    }
    madvise(scan.buffer[0], scan.buffer_size / 2, MADV_WILLNEED);
    scan.buffer[1] = scan.buffer[0] + scan.buffer_size/2;
    scan.buffer_end = scan.buffer[0] + scan.buffer_size;
    scan.p = scan.buffer[0];
    scan.bytes_left = 0;
    scan.eof = 0;

    scan.bytes_left = scan.source_bytes_left > scan.buffer_size ?
			    scan.buffer_size : scan.source_bytes_left;
    scan.source_bytes_left -= scan.bytes_left;
    scan.source_offset = scan.bytes_left;

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
