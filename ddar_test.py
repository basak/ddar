#!/usr/bin/python

import unittest

import ddar

class TestArgParser(unittest.TestCase):
    def check_result(self, cmdline, result):
        test_result = ddar.parse_args(cmdline.split())
        
        # Remove anything that is not set (None) for brevity in individual
        # tests. We check the None behaviour in test_none.
        test_result = dict(((k,v) for k,v in test_result.iteritems()
                                  if v is not None))
        if test_result['member'] == []:
            del test_result['member']
        self.assertEquals(test_result, result)

    def test_none(self):
        result = ddar.parse_args([])
        self.assert_(all((result[v] is None for v in 'ctxdf')))
        self.assertEqual(result['member'], [])

    def test_args(self):
        self.check_result('c', { 'c': True })
        self.check_result('-c', { 'c': True })
        self.check_result('-c', { 'c': True })
        self.check_result('--fsck', { 'fsck': True } )
        self.check_result('c foo', { 'c': True, 'member': [ 'foo' ] })
        self.check_result('c foo bar', { 'c': True,
                                         'member': [ 'foo', 'bar' ]})
        self.check_result('c foo bar -f baz', { 'c': True, 'f': 'baz',
                                                'member': [ 'foo', 'bar' ]})
        self.check_result('c foo bar -- -f baz', { 'c': True,
                                                   'member': [ 'foo', 'bar',
                                                               '-f', 'baz' ]})
        self.check_result('cfbar', { 'c': True, 'f': 'bar' })
        self.check_result('-cfbar', { 'c': True, 'f': 'bar' })
        self.check_result('-c -fbar', { 'c': True, 'f': 'bar' })
        self.check_result('-c -- -fbar', { 'c': True,
                                           'member': [ '-fbar' ] })
        self.check_result('--fsck foo', { 'fsck': True, 'member': [ 'foo' ] })
        self.check_result('cfbar --rsh ssh', { 'c': True, 'f': 'bar',
                                               'rsh': 'ssh' })
        self.check_result('cfbar --rsh=ssh', { 'c': True, 'f': 'bar',
                                               'rsh': 'ssh' })
        self.assertRaises(ddar.OptionError, ddar.parse_args,
                          '--fsck=foo foo'.split())
        self.assertRaises(ddar.OptionError, ddar.parse_args, 'ct'.split())
        self.assertRaises(ddar.OptionError, ddar.parse_args, '-ct'.split())
        self.assertRaises(ddar.OptionError, ddar.parse_args, '-c -t'.split())


# vim: set ts=8 sts=4 sw=4 ai et :
