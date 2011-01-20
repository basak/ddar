class DecoderError(RuntimeError): pass

def encode(data):
    return '%d:%s,' % (len(data), data)

class Decoder(object):
    SIZE, DATA = range(2)

    def __init__(self, initial_data='', max_size=None):
        self.max_size = max_size
        self.state = Decoder.SIZE
        self.data = initial_data
        self.data_idx = 0
        self.size = 0

    def peek_buffer(self):
        return self.data[self.data_idx:]

    def feed(self, data=None):
        if data:
            self.data += data

        while self.data_idx < len(self.data):
            if self.state == Decoder.SIZE:
                next = self.data[self.data_idx]
                self.data_idx += 1
                if next == '0' and not self.size:
                    raise DecoderError()
                if next in '0123456789':
                    self.size *= 10
                    self.size += int(next)
                    if self.max_size and self.size > self.max_size:
                        raise DecoderError()
                elif next == ':':
                    self.state = Decoder.DATA
                else:
                    raise DecoderError()
            elif self.state == Decoder.DATA:
                if (len(self.data) - self.data_idx) > self.size: # > for ','
                    end = self.data_idx + self.size
                    if self.data[end:end+1] != ',':
                        raise DecoderError()
                    data = self.data[self.data_idx:end]
                    self.state = Decoder.SIZE
                    self.data = self.data[end+1:]
                    self.data_idx = 0
                    self.size = 0
                    more_data = (yield data)
                    if more_data:
                        self.data += more_data
                else:
                    return
            else:
                raise NotImplementedError()

if __name__ == '__main__':
    import unittest

    class TestCase(unittest.TestCase):
        def testBasicDecode(self):
            d = Decoder()
            i = d.feed('1:a,')
            self.assertEqual(i.next(), 'a')
            self.assertRaises(StopIteration, i.next)

        def testTwo(self):
            d = Decoder()
            i = d.feed('1:a,')
            self.assertEqual(i.next(), 'a')
            j = d.feed('1:b,')
            self.assertEqual(j.next(), 'b')

        def testEmpty(self):
            d = Decoder()
            i = d.feed('')
            self.assertRaises(StopIteration, i.next)

    unittest.main()

# vim: set ts=8 sts=4 sw=4 ai et :
