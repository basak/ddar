#!/usr/bin/python

from distutils.core import setup, Extension

setup(name='ddar',
      version='0.1',
      author='Robie Basak',
      author_email='rb@synctus.com',
      url='http://www.synctus.com/ddar',
      packages=['tbl'],
      scripts=['ddar'],
      ext_modules=[ Extension('tbl._dds', ['scan.c', 'rabin.c',
                                           'tbl/ddsmodule.c'],
                              include_dirs=['.'],
                              libraries=['rt']) ])

# vim: set ts=8 sts=4 sw=4 ai et :
