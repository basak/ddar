before() {
	top=`mktemp --tmpdir -d ddar-test.XXXXXXXXXX`
	ddar_src=`pwd`
	PATH="$ddar_src:$PATH"
	cd $top
}

after() {
	rm -Rf $top
}

fsck() {
	ddar --fsck "$1"
}

it_shows_usage_1() {
	result=`ddar -h`
	echo "$result"|grep -qi options
}

it_shows_usage_2() {
	result=`ddar --help`
	echo "$result"|grep -qi options
}

it_stores_and_extracts_foo() {
	echo foo|ddar cf archive
	result=`ddar xf archive`
	test "$result" = "foo"
	fsck archive
}

it_stores_from_a_pipe() {
	echo foo|ddar -cf archive
	[ `ddar -xf archive` = foo ]
}

it_stores_from_a_pipe_with_a_name() {
	echo foo|ddar -cf archive -N bar
	[ `ddar -xf archive bar` = foo ]
}

it_stores_from_a_file() {
	echo bar > foo
	ddar -cf archive foo
	[ `ddar -xf archive foo` = bar ]
}

it_stores_from_a_file_with_a_name() {
	echo bar > foo
	ddar -cf archive foo -N baz
	[ `ddar -xf archive baz` = bar ]
}

it_stores_and_extracts_corpus0() {
	ddar cf archive < "$ddar_src/test/corpus0"
	ddar xf archive|cmp - "$ddar_src/test/corpus0"
	fsck archive
}

it_deletes_a_member_in_the_middle_of_the_archive() {
	echo 1|ddar cf archive -N A
	echo 2|ddar cf archive -N B
	echo 3|ddar cf archive -N C
	test `ddar xf archive A` = 1
	test `ddar xf archive B` = 2
	test `ddar xf archive C` = 3
	ddar df archive B
	test `ddar xf archive A` = 1
	test `ddar xf archive C` = 3
	! ddar xf archive B >/dev/null
	fsck archive
}

it_does_not_delete_used_blocks() {
	echo bar > foo
	ddar cf archive -N 1 < foo
	ddar cf archive -N 2 < foo
	ddar df archive 1
	ddar xf archive 2|cmp - foo
	fsck archive
}

it_will_not_shell_out_for_source() {
	ddar cf archive \!false && false
	test $? -eq 2
}
