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
	echo foo > $REMOTE_TOP/foo
	ddar -cf archive localhost:foo
	result=`ddar -xf archive`
	test "$result" = "foo"
	fsck archive
}

it_stores_and_extracts_corpus0() {
	cp "$DDAR_SRC/test/corpus0" "$REMOTE_TOP"
	ddar -cf archive localhost:corpus0
	ddar -xf archive|cmp - "$DDAR_SRC/test/corpus0"
	fsck archive
}

it_stores_corpus0_and_dedups_a_second_time() {
	cp "$DDAR_SRC/test/corpus0" "$REMOTE_TOP"
	ddar -cf archive localhost:corpus0 -N first
	ddar -cf archive localhost:corpus0 -N second
	ddar -xf archive|cmp - "$DDAR_SRC/test/corpus0"
}

it_will_not_store_from_two_files() {
	echo 1 > "$REMOTE_TOP/foo"
	echo 2 > "$REMOTE_TOP/bar"
	echo 3 > baz
	ddar -cf archive localhost:foo localhost:bar && false
	[ $? -eq 2 ]
	ddar -cf archive localhost:foo baz && false
	[ $? -eq 2 ]
	ddar -cf archive baz localhost:foo && false
	[ $? -eq 2 ]
}

it_stores_from_a_file() {
	echo bar > "$REMOTE_TOP/foo"
	ddar -cf archive localhost:foo
	[ `ddar -xf archive foo` = bar ]
}

it_stores_from_a_file_with_a_name() {
	echo bar > "$REMOTE_TOP/foo"
	ddar -cf archive localhost:foo -N baz
	[ `ddar -xf archive baz` = bar ]
}
