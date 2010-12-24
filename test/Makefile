CFLAGS = -O3
test: random seed1
	./random 268435466 < seed1 > corpus1
	rm -f db
	sqlite3 db < ../analyze.schema
	../analyze corpus1 db
	sqlite3 -separator , db 'select sha256,offset,length from chunk order by offset' > result.1
	cmp result.1 expected.1
	echo Test passed

random: random.c mt19937ar.c
	$(CC) -o random random.c
