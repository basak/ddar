#!/usr/bin/python

# Prepare a ddar repository for import to git-annex without having to process
# all the data again. See:
# http://git-annex.branchable.com/forum/__34__Preseeding__34___a_special_remote/
# for details.
#
# Usage: python git-annex-convert.py [--rename] /ddarrepo/db > index
#
# index will contain an index of archive members in the form:
#   KEY <SPACE> NAME
# where KEY is a suitable git-annex key and NAME is the original member name.
#
# Using --rename will also rename all archive members to their respective
# git-annex keys as specified in the index (and also output the index).
#
# Once you do a --rename, it is essential that you do not lose the index file,
# as this will be the only record of your original member names until you have
# imported the names into your git-annex repository. You will need the index
# file to complete the import.

import argparse
import binascii

import sqlite3


def annex_key(sha256, size):
    return 'SHA256-s%(size)d--%(sha256)s' % locals()


def process(filename, rename=False):
    db = sqlite3.connect(filename)
    db.text_factory = str
    c1 = db.cursor()
    if rename:
        c2 = db.cursor()
    c1.execute('SELECT name, hash, length FROM member')
    row = c1.fetchone()
    while row:
        old_name = row[0]
        sha256 = binascii.hexlify(str(row[1]))
        size = row[2]
        key = annex_key(sha256, size)

        print ' '.join([key, old_name])
        if rename:
            c2.execute('UPDATE member SET name=? WHERE name=?', [key, old_name])

        row = c1.fetchone()

    if rename:
        db.commit()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--rename', action='store_true')
    parser.add_argument('filename')
    args = parser.parse_args()
    process(args.filename, rename=args.rename)


if __name__ == '__main__':
    main()
