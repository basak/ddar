before() {
	top=`mktemp --tmpdir -d ddar-test.XXXXXXXXXX`
	ddar_src=`pwd`
	PATH="$ddar_src:$PATH"
	cd $top
}

after() {
	rm -Rf $top
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
}

it_stores_and_extracts_corpus0() {
	ddar cf archive < "$ddar_src/test/corpus0"
	ddar xf archive|cmp - "$ddar_src/test/corpus0"
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
}

it_fscks_a_valid_archive() {
	echo foo|ddar cf archive
	ddar --fsck -f archive
}
