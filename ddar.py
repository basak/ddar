#!/usr/bin/python

import argparse, binascii, errno, hashlib, os, os.path, stat, string, sqlite3
import sys

import libdds

class Store(object):
    def __init__(self, dirname, auto_create=False):
        self.dirname = dirname

        if not os.path.exists(self.dirname):
            if auto_create:
                self._create()
            else:
                raise RuntimeError("archive %s not found" % self.dirname)
        elif not os.path.isdir(self.dirname):
            raise RuntimeError("%s exists but is not a ddar directory" %
                               self.dirname)

        try:
            format_name = self._read_small_file(self._format_filename('name'))
        except:
            raise RuntimeError('%s is not a ddar directory' %
                               self.dirname)
        else:
            if format_name.lstrip().rstrip() != 'ddar':
                raise RuntimeError('%s is not a ddar directory' %
                                   self.dirname)

        version = self._read_small_file(self._format_filename('version'))
        version = version.lstrip().rstrip()
        if version != '1':
            raise RuntimeError('%s uses ddar archive version %s but only' +
                               'version 1 is supported' % self.dirname)

        self.db = sqlite3.connect(os.path.join(self.dirname, 'db'))
        self.db.text_factory = str

    @staticmethod
    def _read_small_file(name, size_limit=1024):
        with open(name, 'r') as f:
            data = f.read(size_limit)
        return data

    @staticmethod
    def _write_small_file(name, data):
        with open(name, 'w') as f:
            f.write(data)

    def _create(self):
        os.mkdir(self.dirname)
        os.mkdir(os.path.join(self.dirname, 'format'))
        os.mkdir(os.path.join(self.dirname, 'objects'))

        self._write_small_file(self._format_filename('name'), "ddar\n")
        self._write_small_file(self._format_filename('version'), "1\n")

        db = sqlite3.connect(os.path.join(self.dirname, 'db'))
        c = db.cursor()
        c.execute('''
CREATE TABLE chunk (tag TEXT NOT NULL,
                    hash BLOB NOT NULL,
                    offset INTEGER NOT NULL,
                    length INTEGER NOT NULL,
                    UNIQUE (tag, offset))''')
        # No need for a (tag,offset) index as we have a UNIQUE
        # constraint on it. This causes sqlite to generate an index and
        # the documentatation states that there is a performance degradation
        # by having an extra index instead of relying on this fact.
        c.execute('CREATE INDEX chunk_hash_idx ON chunk(hash)')
        c.close()
        db.commit()
        db.close()

    def _object_filename(self, h):
        return os.path.join(self.dirname, 'objects', binascii.hexlify(h))

    def _format_filename(self, n):
        return os.path.join(self.dirname, 'format', n)

    def _store_chunk(self, tag, cursor, data, offset, length):
        '''Store the chunk in the database and the object store if necessary,
        but do not commit. The database insert is done last, so that if
        interrupted the database is never wrong. At worst a dangling object
        will be left in the object store.'''

        h = hashlib.sha256(data).digest()
        h_blob = buffer(h) # buffer to make sqlite use a BLOB
        cursor.execute('SELECT 1 FROM chunk WHERE hash=?', (h_blob,))
        if not cursor.fetchone():
            with open(self._object_filename(h), 'wb') as f:
                f.write(data)
        cursor.execute('INSERT INTO chunk (tag, hash, offset, length) ' +
                       'VALUES (?, ?, ?, ?)', (tag, h_blob, offset, length))

    def store(self, tag, f=sys.stdin):
        cursor = self.db.cursor()

        cursor.execute('SELECT 1 FROM chunk WHERE tag=? LIMIT 1', (tag,))
        if cursor.fetchone():
            raise RuntimeError("archive %s already exists" % tag)

        d = libdds.DDS()
        d.set_file(f)
        d.begin()

        try:
            offset = 0
            for data in d.chunks():
                length = len(data)
                self._store_chunk(tag, cursor, data, offset, length)
                offset += length
        finally:
            cursor.close()
            self.db.commit()

    def load(self, tag, f=sys.stdout):
        cursor = self.db.cursor()
        cursor.execute('SELECT hash, offset, length FROM chunk WHERE tag=? ' +
                       'ORDER BY offset', (tag,))

        next_offset = 0
        row = cursor.fetchone()
        if not row:
            raise RuntimeError('member %s not found in archive' % tag)
        while row:
            h, offset, length = row
            assert(offset == next_offset)
            with open(self._object_filename(h), 'rb') as g:
                data = g.read()
            assert(len(data) == length)
            f.write(data)

            next_offset = offset + length
            row = cursor.fetchone()

    def delete(self, tag):
        cursor = self.db.cursor()
        cursor2 = self.db.cursor()
        cursor.execute('SELECT hash FROM chunk WHERE tag=?', (tag,))
        row = cursor.fetchone()
        if not row:
            raise RuntimeError('member %s not found in archive' % tag)
        while row:
            h = row[0]
            cursor2.execute('SELECT 1 FROM chunk WHERE hash=? AND tag != ? ' +
                            'LIMIT 1', (h, tag))
            if not cursor2.fetchone():
                try:
                    os.unlink(self._object_filename(h))
                except OSError, e:
                    # ignore ENOENT to make delete idempotent on a SIGINT
                    if e.errno != errno.ENOENT:
                        raise
            row = cursor.fetchone()
        cursor.execute('DELETE FROM chunk WHERE tag=?', (tag,))
        cursor.close()
        self.db.commit()

    def list_tags(self):
        cursor = self.db.cursor()
        cursor.execute('SELECT DISTINCT tag FROM chunk')
        tag = cursor.fetchone()
        while tag:
            yield tag[0]
            tag = cursor.fetchone()

    hexdigits = set(string.hexdigits) - set(string.uppercase)
    sha256_length = len(hashlib.sha256().hexdigest())

    @classmethod
    def _valid_hex_hash(cls, h):
        return (len(h) == cls.sha256_length and
            all((d in cls.hexdigits for d in h)))

    def _fsck_fs(self):
        cursor = self.db.cursor()
        for h in os.listdir(os.path.join(self.dirname, 'objects')):
            if not self._valid_hex_hash(h):
                continue
            cursor.execute('SELECT 1 FROM chunk WHERE hash=? LIMIT 1',
                           (buffer(binascii.unhexlify(h)),))
            if not cursor.fetchone():
                print 'Unknown object %s' % h

    def _fsck_db_to_fs(self):
        cursor = self.db.cursor()
        cursor.execute('SELECT DISTINCT hash, length FROM chunk')

        row = cursor.fetchone()
        while row:
            h, length = row
            h = str(h) # sqlite3 returns a buffer for a BLOB; we want an str;
                       # otherwise comparisons never match
            try:
                with open(self._object_filename(h), 'rb') as f:
                    data = f.read(length+1)
            except IOError:
                print "Could not read chunk %s" % binascii.hexlify(h)
            else:
                if len(data) != length:
                    print "Chunk %s wrong size" % binascii.hexlify(h)
                elif hashlib.sha256(data).digest() != h:
                    print "Chunk %s corrupt" % binascii.hexlify(h)
            
            row = cursor.fetchone()

    def _fsck_db(self):
        cursor = self.db.cursor()
        cursor.execute('SELECT tag, offset, length FROM chunk ' +
                       'ORDER BY tag, offset')
        current_tag = None
        row = cursor.fetchone()
        while row:
            tag, offset, length = row
            if tag != current_tag:
                current_tag = tag
                current_offset = 0
            if offset != current_offset:
                if offset < current_offset:
                    print ("Chunk at offset %d for %s has an overlap" %
                        (offset, tag))
                else:
                    assert(offset > current_offset)
                    print ("Hole in %s found between %d and %d" %
                        (tag, current_offset, offset))
                current_offset = offset
            current_offset += length

            row = cursor.fetchone()

    def fsck(self):
        self._fsck_db()
        self._fsck_fs()
        self._fsck_db_to_fs()

def parse_args():
    # The argument parser options are modelled after GNU ar
    parser = argparse.ArgumentParser(prog='ddar')

    parser.add_argument('--name', dest='name',
                        help='use NAME for first stored member name')
    parser.add_argument('-o', dest='output_name', metavar='NAME',
                        help="use NAME as output destination ('-' for stdout)")
    group = parser.add_mutually_exclusive_group(required=True)

    group.add_argument('-r', '-q', dest='add', action='store_true',
                       help='add members to archive')
    group.add_argument('-t', dest='list', action='store_true',
                       help='list archive contents')
    group.add_argument('-x', dest='extract', action='store_true',
                       help='extract members from archive')
    group.add_argument('-d', dest='delete', action='store_true',
                       help='delete members from archive')
    group.add_argument('--fsck', dest='fsck', action='store_true',
                       help='check archive for errors')

    parser.add_argument('archive', help='ddar archive directory')
    parser.add_argument('member', nargs='*',
                        help='file to store (add) or retrieve (extract)')

    # Hack to make first hyphen optional (like GNU ar)
    try:
        first_char = sys.argv[1][0]
    except IndexError:
        parser.print_usage()
        sys.exit(1)
    if first_char != '-':
        sys.argv[1] = '-' + sys.argv[1]

    return parser.parse_args()

def main_add_one(store, filename, tag):
    if filename == '-':
        store.store(tag, sys.stdin)
    else:
        with open(filename, 'rb') as f:
            store.store(tag, f)

def main_add(store, members, name=None):
    for member in members:
        if name:
            tag = name
            name = None
        else:
            tag = os.path.basename(member)
        main_add_one(store, member, tag)

def main_extract_one(store, tag, filename):
    if filename == '-':
        store.load(tag, sys.stdout)
    else:
        with open(filename, 'wb') as f:
            store.load(tag, f)

def main_extract(store, members, name=None):
    for tag in members:
        if name:
            filename = name
            name = None
        else:
            filename = tag
        main_extract_one(store, tag, filename)

def main():
    args = parse_args()

    store = Store(args.archive, auto_create=args.add)
    if args.add:
        main_add(store, args.member, args.name)
    elif args.extract:
        main_extract(store, args.member, args.output_name)
    elif args.delete:
        for member in args.member:
            store.delete(member)
    elif args.list:
        for tag in store.list_tags():
            print tag
    elif args.fsck:
        store.fsck()
 

if __name__ == '__main__':
    main()

# vim: set ts=8 sts=4 sw=4 ai et :
