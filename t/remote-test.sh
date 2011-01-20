before() {
	top=`mktemp --tmpdir -d ddar-test.XXXXXXXXXX`
	export REMOTE_TOP=`mktemp --tmpdir -d ddar-test.XXXXXXXXXX`
	export DDAR_SRC=`pwd`
	PATH="$DDAR_SRC:$PATH"
	cd $top
}

after() {
	rm -Rf $top $REMOTE_TOP
}

ddar() {
	"$DDAR_SRC/ddar" --rsh "$DDAR_SRC/t/rsh" $@
}

fsck() {
	ddar --fsck "$1"
}

it_stores_and_extracts_foo() {
	echo foo|ddar -cf localhost:archive
	result=`ddar -xf $REMOTE_TOP/archive`
	test "$result" = "foo"
	fsck $REMOTE_TOP/archive
}

it_stores_and_extracts_corpus0() {
	ddar -cf localhost:archive < "$DDAR_SRC/test/corpus0"
	ddar -xf $REMOTE_TOP/archive|cmp - "$DDAR_SRC/test/corpus0"
	fsck $REMOTE_TOP/archive
}

it_stores_corpus0_and_dedups_a_second_time() {
	ddar -cf localhost:archive < "$DDAR_SRC/test/corpus0"
	ddar -cf localhost:archive < "$DDAR_SRC/test/corpus0"
	ddar -xf $REMOTE_TOP/archive|cmp - "$DDAR_SRC/test/corpus0"
}
