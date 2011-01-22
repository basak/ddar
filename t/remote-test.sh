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

it_will_not_store_from_two_files() {
	echo 1 > foo
	echo 2 > bar
	ddar -cf localhost:archive foo bar && false
	[ $? -eq 2 ]
}

it_stores_from_a_pipe() {
	echo foo|ddar -cf localhost:archive
	[ `ddar -xf $REMOTE_TOP/archive` = foo ]
}

it_stores_from_a_pipe_with_a_name() {
	echo foo|ddar -cf localhost:archive -N bar
	[ `ddar -xf $REMOTE_TOP/archive bar` = foo ]
}

it_stores_from_a_file() {
	echo bar > foo
	ddar -cf localhost:archive foo
	[ `ddar -xf $REMOTE_TOP/archive foo` = bar ]
}

it_stores_from_a_file_with_a_name() {
	echo bar > foo
	ddar -cf localhost:archive foo -N baz
	[ `ddar -xf $REMOTE_TOP/archive baz` = bar ]
}
