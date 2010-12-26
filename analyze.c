#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <inttypes.h>
#include <byteswap.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "sqlite3.h"
#include "sha2.h"
#include "scan.h"

struct analyze_ctx {
    sqlite3 *db;
    sqlite3_stmt *db_insert_stmt;
    int64_t offset;
};

static void print_sqlite3_error(sqlite3 *db) {
    fputs("sqlite3: ", stderr);
    fputs(sqlite3_errmsg(db), stderr);
    fputc('\n', stderr);
}

static void store_chunk(struct analyze_ctx *analyze,
			const struct scan_chunk_data *chunk_data) {
    int i;
    sha256_ctx sha256;
    unsigned char sha256_digest[32];
    char sha256_digest_string[65];
    int chunk_size = 0;

    sha256_init(&sha256);
    for (i=0; i<2; i++) {
	chunk_size +=  chunk_data[i].size;
	if (chunk_data[i].buf && chunk_data[i].size)
	    sha256_update(&sha256, chunk_data[i].buf, chunk_data[i].size);
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
    if (sqlite3_bind_text(analyze->db_insert_stmt, 1, sha256_digest_string, -1,
		SQLITE_STATIC) != SQLITE_OK) {
	print_sqlite3_error(analyze->db);
	abort();
    }
    if (sqlite3_bind_int64(analyze->db_insert_stmt, 3, analyze->offset) !=
	    SQLITE_OK) {
	print_sqlite3_error(analyze->db);
	abort();
    }
    if (sqlite3_bind_int(analyze->db_insert_stmt, 4, chunk_size) != SQLITE_OK) {
	print_sqlite3_error(analyze->db);
	abort();
    }
    if (sqlite3_step(analyze->db_insert_stmt) != SQLITE_DONE) {
	print_sqlite3_error(analyze->db);
	abort();
    }
    if (sqlite3_reset(analyze->db_insert_stmt) != SQLITE_OK) {
	print_sqlite3_error(analyze->db);
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

    analyze->offset += chunk_size;
}

struct analyze_ctx *analyze_open(const char *db_filename, const char *tag) {
    struct analyze_ctx *analyze;

    analyze = malloc(sizeof(struct analyze_ctx));
    if (!analyze)
	return 0;

    analyze->offset = 0;

    if (sqlite3_open(db_filename, &analyze->db) != SQLITE_OK) {
	print_sqlite3_error(analyze->db);
	abort();
    }

    if (sqlite3_exec(analyze->db, "BEGIN", NULL, NULL, NULL) != SQLITE_OK) {
	print_sqlite3_error(analyze->db);
	abort();
    }

    if (sqlite3_prepare(analyze->db, "INSERT INTO chunk (sha256, tag, offset, length) VALUES (?, ?, ?, ?)", -1, &analyze->db_insert_stmt, NULL) != SQLITE_OK) {
	print_sqlite3_error(analyze->db);
	abort();
    }

    if (sqlite3_bind_text(analyze->db_insert_stmt, 2, tag, -1,
			  SQLITE_STATIC) != SQLITE_OK) {
	print_sqlite3_error(analyze->db);
	abort();
    }

    return analyze;
}

void analyze_close(struct analyze_ctx *analyze) {
    sqlite3_finalize(analyze->db_insert_stmt);

    if (sqlite3_exec(analyze->db, "COMMIT", NULL, NULL, NULL) != SQLITE_OK) {
	print_sqlite3_error(analyze->db);
	abort();
    }
}

int main(int argc, char **argv) {
    struct scan_ctx *scan;
    struct scan_chunk_data chunk_data[2];
    struct analyze_ctx *analyze;
    int fd, result;

    analyze = analyze_open(argv[2], argv[1]);
    if (!analyze) {
	perror("analyze_init");
	return EXIT_FAILURE;
    }

    scan = scan_init();
    if (!scan) {
	perror("scan_init");
	return EXIT_FAILURE;
    }

    fd = open(argv[1], O_RDWR);
    if (fd < 0) {
	perror("open");
	return EXIT_FAILURE;
    }
    scan_set_fd(scan, fd);

    if (!scan_begin(scan))
	return EXIT_FAILURE;
    do {
	result = scan_read_chunk(scan, chunk_data);
	if (result & SCAN_CHUNK_FOUND)
	    store_chunk(analyze, chunk_data);
	else {
	    fputs("Scan error\n", stderr);
	    return EXIT_FAILURE;
	}
    } while (!(result & SCAN_CHUNK_LAST));
    analyze_close(analyze);
    scan_free(scan);

    return EXIT_SUCCESS;
}

/* vim: set ts=8 sts=4 sw=4 cindent : */
