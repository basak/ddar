#ifndef SCAN_H
#define SCAN_H

struct scan_ctx;
typedef void scan_boundary_cb_fn_t (unsigned char *, int, unsigned char *, int,
			       void *);

struct scan_ctx *scan_init(void);
void scan_free(struct scan_ctx *);
void scan_set_fd(struct scan_ctx *, int);
void scan_begin(struct scan_ctx *);
int scan_find_chunk_boundary(struct scan_ctx *,
			     scan_boundary_cb_fn_t *, void *);

#endif

/* vim: set ts=8 sts=4 cindent : */
