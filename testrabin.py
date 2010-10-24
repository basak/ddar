#!/usr/bin/python

import rabin, unittest

class TestCase(unittest.TestCase):
    def testBasic(self):
        r = rabin.Rabin(48)
        hash = r.hash('123456789012345678901234567890123456789012345678')
        hash_n = r.next('1', 'a')
        hash_n2 = r.hash('23456789012345678901234567890123456789012345678a')
        self.assertEqual(hash_n, hash_n2)

    def test1(self):
        r = rabin.Rabin(48)
        q = '#\xc2kN\x8d\x1e\xf1\xae\x99;\xafD \x9d\x8fk\x06\x0b\xc4b\x9aw\xb7\xaf\xb8\x19\x00\x18#Gk\xe9\xf2\xd9\x8e\xc5\x88\xbf\xa4w\xd9\xea2\xe0\x0b\xb8@\xda\x85'
        hash = r.hash(q[0:48])
        hash_n = r.next(q[0], q[48])
        hash_n2 = r.hash(q[1:49])
        self.assertEqual(hash_n, hash_n2)


if __name__ == '__main__':
    unittest.main()

# vim: set ts=8 sts=4 sw=4 ai et :
