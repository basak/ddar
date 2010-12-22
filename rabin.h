#ifndef RABIN_H
#define RABIN_H

#define _GNU_SOURCE
#include <stdlib.h>
#include <stdint.h>

struct rabin_ctx;

struct rabin_ctx *rabin_init(uint32_t a, int k);
void rabin_free(struct rabin_ctx *ctx);
uint32_t rabin_hash(const struct rabin_ctx *ctx, const unsigned char *p);
uint32_t rabin_hash_next(const struct rabin_ctx *ctx, uint32_t hash, unsigned char old,
	unsigned char new);
uint32_t rabin_hash_split(const struct rabin_ctx *ctx, const unsigned char *p,
	int size, const unsigned char *p2);

#endif

/* vim: set ts=8 sts=4 sw=4 cindent : */
