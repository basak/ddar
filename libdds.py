#!/usr/bin/python

from ctypes import c_void_p, c_int, c_char, POINTER, CFUNCTYPE
import ctypes

libdds = ctypes.CDLL('libdds.so.0')

scan_boundary_cb_fn_t = CFUNCTYPE(None, POINTER(c_char), c_int,
                                        POINTER(c_char), c_int,
                                        c_void_p)

for name, restype, argtypes in [
    ( 'scan_init', c_void_p, [] ),
    ( 'scan_free', None, [ c_void_p ] ),
    ( 'scan_set_fd', None, [ c_void_p, c_int ] ),
    ( 'scan_begin', None, [ c_void_p ] ),
    ( 'scan_find_chunk_boundary', c_int, [ c_void_p, scan_boundary_cb_fn_t,
                                           c_void_p ] ),
        ]:
    libdds[name].restype = restype
    libdds[name].argtypes = argtypes

class DDS(object):
    def __init__(self):
        self.h = libdds.scan_init()

    def __del__(self):
        libdds.scan_free(self.h)

    def set_fd(self, fd):
        libdds.scan_set_fd(self.h, fd)

    def set_file(self, f):
        self.set_fd(f.fileno())

    def begin(self):
        libdds.scan_begin(self.h)

    def find_chunk_boundary(self, cb):
        def _cb(buf1, buf1_size, buf2, buf2_size, data):
            if buf1_size:
                buf1 = ctypes.string_at(buf1, buf1_size)
            else:
                buf1 = ''
            if buf2_size:
                buf2 = ctypes.string_at(buf2, buf2_size)
            else:
                buf2 = ''

            cb(buf1 + buf2)

        return libdds.scan_find_chunk_boundary(self.h, scan_boundary_cb_fn_t(_cb), None)

# vim: set ts=8 sts=4 sw=4 ai et :
