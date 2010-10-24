#!/usr/bin/python

import ctypes

rabin = ctypes.CDLL('librabin.so.0')
rabin.rabin_init.argtypes = [ ctypes.c_int, ctypes.c_int ]
rabin.rabin_init.restype = ctypes.c_void_p
rabin.rabin_free.argtypes = [ ctypes.c_void_p ]
rabin.rabin_free.restype = None
rabin.rabin_hash.argtypes = [ ctypes.c_void_p, ctypes.c_char_p ]
rabin.rabin_hash.restype = ctypes.c_uint
rabin.rabin_hash_next.argtypes = [ ctypes.c_void_p, ctypes.c_uint,
        ctypes.c_char, ctypes.c_char ]
rabin.rabin_hash_next.restype = ctypes.c_uint

class Rabin(object):
    def __init__(self, k, a=1103515245):
        self.k = k
        self.ctx = rabin.rabin_init(a, k)
        if not self.ctx:
            raise MemoryError()

    def __del__(self):
        if self.ctx:
            rabin.rabin_free(self.ctx)

    def hash(self, data):
        if len(data) < self.k:
            raise ValueError()
        self.last_hash = rabin.rabin_hash(self.ctx, data)
        return self.last_hash

    def next(self, old, new):
        if len(old) != 1 or len(new) != 1:
            raise ValueError()
        self.last_hash = rabin.rabin_hash_next(self.ctx, self.last_hash, old, new)
        return self.last_hash

# vim: set ts=8 sts=4 sw=4 ai et :
