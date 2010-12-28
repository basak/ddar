import hashlib, dds

f = open('corpus1', 'r')
d = dds.DDS()
d.set_file(f)
d.begin()
offset = 0
for chunk in d.chunks():
    length = len(chunk)
    print '%s,%s,%s' % (hashlib.sha256(chunk).hexdigest(), offset, length)
    offset += length

# vim: set ts=8 sts=4 sw=4 ai et :
