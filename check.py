#!/usr/bin/python

import hashlib, sys

def main():
    data = open(sys.argv[1], 'rb')
    check = open(sys.argv[2], 'rb')

    total_size = 0
    for line in check:
        hash, size = line.split()
        total_size += int(size)
        chunk = data.read(int(size))
        assert(len(chunk) == int(size))
        hash2 = hashlib.sha256(chunk).hexdigest()
        assert(hash == hash2)

    print "total size: %r" % total_size

    more_data = data.read(1)
    assert(not more_data)

if __name__ == '__main__':
    main()

# vim: set ts=8 sts=4 sw=4 ai et :
