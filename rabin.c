#include "rabin.h"

struct rabin_ctx {
    uint32_t *a_exp;
    int k;
};

struct rabin_ctx *rabin_init(uint32_t a, int k) {
    int i;
    uint32_t *a_exp;
    uint32_t acc;
    struct rabin_ctx *ctx;

    ctx = (struct rabin_ctx *)malloc(sizeof(struct rabin_ctx));
    if (!ctx)
	return 0;
    a_exp = (uint32_t *)malloc(sizeof(uint32_t) * k);
    if (!a_exp) {
	free(ctx);
	return 0;
    }

    acc = 1;
    a_exp[0] = 1;
    for (i=1; i<k; i++) {
	acc *= a;
	a_exp[i] = acc;
    }

    ctx->k = k;
    ctx->a_exp = a_exp;

    return ctx;
}

void rabin_free(struct rabin_ctx *ctx) {
    free(ctx->a_exp);
    free(ctx);
}

uint32_t rabin_hash(const struct rabin_ctx *ctx, const unsigned char *p) {
    int i;
    uint32_t acc=0;
    for (i=ctx->k-1; i>=0; i--, p++) {
	acc += ctx->a_exp[i] * *p;
    }
    return acc;
}

uint32_t rabin_hash_split(const struct rabin_ctx *ctx, const unsigned char *p,
	int size, const unsigned char *p2) {
    int i;
    uint32_t acc=0;
    for (i=ctx->k-1; i>=0; i--, p++) {
	acc += ctx->a_exp[i] * *p;
	if (!--size)
	    p = p2;
    }
    return acc;
}

uint32_t rabin_hash_next(const struct rabin_ctx *ctx, uint32_t hash, unsigned
	char old, unsigned char new) {
    hash -= ctx->a_exp[ctx->k-1] * old;
    hash *= ctx->a_exp[1];
    hash += new;
    return hash;
}

/* vim: set ts=8 sts=4 sw=4 cindent : */
