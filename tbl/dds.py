#!/usr/bin/python

# Copyright 2010-2011 True Blue Logic Ltd
# 
# This program is free software: you can redistribute it and/or modify
# it under the terms of version 3 of the GNU General Public License as
# published by the Free Software Foundation.
# 
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import _dds

class DDS(object):
    def __init__(self):
        self.h = _dds.init()

    def set_fd(self, fd):
        _dds.set_fd(self.h, fd)

    def set_file(self, f):
        self.set_fd(f.fileno())

    def set_aio(self):
        _dds.set_aio(self.h)

    def begin(self):
        if not _dds.begin(self.h):
            raise RuntimeError('dds error')

    def chunks(self):
        while True:
            result, data = _dds.read_chunk(self.h)
            if not (result & _dds.SCAN_CHUNK_FOUND):
                raise RuntimeError('dds error')

            yield data

            if result & _dds.SCAN_CHUNK_LAST:
                break

# vim: set ts=8 sts=4 sw=4 ai et :
