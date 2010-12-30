#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <assert.h>

#include "mt19937ar.c"

#define MAX_SEED 2496
#define BUFSIZE (1<<18) /* in 32-bit integers */

int read_seed(char *seed) {
    size_t seed_length;

    seed_length = fread(seed, 1, MAX_SEED, stdin);
    if (ferror(stdin)) {
	perror("fread");
	return 0;
    }

    if (seed_length < 4) {
	fputs("seed too short (minimum length 4 bytes)", stderr);
	return 0;
    }

    return seed_length;
}

int main(int argc, char **argv) {
    /* Usage: random length < seed */
    assert(argc >= 2);
    long bytes_needed = atol(argv[1]);
    char seed[MAX_SEED];
    uint32_t random;
    uint32_t *buffer;
    char *p;
    int words_needed, i, bytes_to_write;

    buffer = (uint32_t *)malloc(BUFSIZE * sizeof(uint32_t));
    assert(buffer);

    long seed_length = read_seed(seed);
    if (!seed_length)
	return 1;

    init_by_array((uint32_t *)seed, seed_length / 4);

    while (bytes_needed) {
	words_needed = (bytes_needed+3) / 4;
	for(i=0; i<(words_needed > BUFSIZE ? BUFSIZE : words_needed); i++)
	    buffer[i] = genrand_int32();

	bytes_to_write = words_needed > BUFSIZE ? BUFSIZE*4 :
	    bytes_needed;
	if (fwrite(buffer, 1, bytes_to_write, stdout) != bytes_to_write) {
	    perror("fwrite");
	    return 1;
	}
	bytes_needed -= bytes_to_write;
    }

    return 0;
}

/* vim: set ts=8 sts=4 sw=4 cindent : */
