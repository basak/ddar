CFLAGS = -O3

.PHONY: tests test1 test2 corpus
tests: corpus test1 test2

corpus: corpus1
	md5sum -c MD5SUMS

corpus0: corpus1
	dd if=corpus1 of=corpus0 bs=1k count=2097

corpus1: seed1 random
	./random 268435466 < seed1 > corpus1

test1: corpus1
	rm -f db
	sqlite3 db < ../analyze.schema
	../analyze corpus1 db
	sqlite3 -separator , db 'select sha256,offset,length from chunk order by offset' > result.1
	cmp result.1 expected.1
	
test2: corpus1
	LD_LIBRARY_PATH=.. PYTHONPATH=.. python dds_test.py > result.1b
	cmp result.1b expected.1
	echo Test passed

random: random.c mt19937ar.c
	$(CC) $(CPPFLAGS) $(CFLAGS) -o random random.c
