#ifndef SCAN_H
#define SCAN_H

struct scan_ctx;

struct scan_chunk_data {
    unsigned char *buf;
    int size;
};

#define SCAN_CHUNK_FOUND 1
#define SCAN_CHUNK_LAST  2

struct scan_ctx *scan_init(void);
void scan_free(struct scan_ctx *);
void scan_set_fd(struct scan_ctx *, int);
int scan_begin(struct scan_ctx *);
int scan_read_chunk(struct scan_ctx *, struct scan_chunk_data *);

#endif

/* vim: set ts=8 sw=4 sts=4 cindent : */
