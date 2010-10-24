#!/usr/bin/python

import mmap, optparse, os

import rabin

def scan(filename, window=48):
    size = os.stat(filename).st_size
    assert(size >= window)
    f = open(filename, 'rb')
    map = mmap.mmap(f.fileno(), size, access=mmap.ACCESS_READ)
    r = rabin.Rabin(window)
    r2 = rabin.Rabin(window)
    hash = r.hash(str(map[0:window]))
    start = 0
    for i in xrange(0, size-window):
        if not (hash & 0xffff):
            print '%d' % (i + window)
        hash = r.next(map[i], map[i+window])
        assert(hash == r2.hash(str(map[i+1:i+1+window])))
    map.close()

def main():
    parser = optparse.OptionParser()
    options, args = parser.parse_args()
    for filename in args:
        scan(filename)

if __name__ == '__main__':
    main()

# vim: set ts=8 sts=4 sw=4 ai et :
