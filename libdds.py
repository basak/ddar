#!/usr/bin/python

from ctypes import byref, CDLL, c_void_p, c_int, c_char, POINTER, string_at
from ctypes import Structure

libdds = CDLL('libdds.so.0')

class scan_chunk_data(Structure):
    _fields_ = [ ('buf', c_void_p),
                 ('size', c_int) ]

for name, restype, argtypes in [
    ( 'scan_init', c_void_p, [] ),
    ( 'scan_free', None, [ c_void_p ] ),
    ( 'scan_set_fd', None, [ c_void_p, c_int ] ),
    ( 'scan_set_aio', None, [ c_void_p ] ),
    ( 'scan_begin', c_int, [ c_void_p ] ),
    ( 'scan_read_chunk', c_int, [ c_void_p, POINTER(scan_chunk_data) ] ),
        ]:
    libdds[name].restype = restype
    libdds[name].argtypes = argtypes

SCAN_CHUNK_FOUND = 1
SCAN_CHUNK_LAST = 2

class DDS(object):
    def __init__(self):
        self.h = libdds.scan_init()

    def __del__(self):
        libdds.scan_free(self.h)

    def set_fd(self, fd):
        libdds.scan_set_fd(self.h, fd)

    def set_file(self, f):
        self.set_fd(f.fileno())

    def set_aio(self):
        libdds.scan_set_aio(self.h)

    def begin(self):
        if not libdds.scan_begin(self.h):
            raise RuntimeError('libdds error')

    def chunks(self):
        chunk_data = (scan_chunk_data * 2)()

        while True:
            result = libdds.scan_read_chunk(self.h, chunk_data)
            if not (result & SCAN_CHUNK_FOUND):
                raise RuntimeError('libdds error')

            data = []
            for segment in chunk_data:
                if segment.buf and segment.size:
                    data.append(string_at(segment.buf, segment.size))
            
            yield ''.join(data)

            if result & SCAN_CHUNK_LAST:
                break

# vim: set ts=8 sts=4 sw=4 ai et :
