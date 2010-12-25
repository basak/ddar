import hashlib, libdds

class Analyzer(object):
    def __init__(self):
        self.offset = 0

    def cb(self, data):
        length = len(data)

        print '%s,%s,%s' % (hashlib.sha256(data).hexdigest(), self.offset, length)
        self.offset += length

def cb(s):
  print '%s %s' % (len(s), hashlib.sha256(s).hexdigest())

a = Analyzer()
f = open('corpus1', 'r')
d = libdds.DDS()
d.set_file(f)
d.begin()
while(d.find_chunk_boundary(a.cb)): pass

# vim: set ts=8 sts=4 sw=4 ai et :
