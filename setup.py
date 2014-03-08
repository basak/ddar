#!/usr/bin/python

import sys
from setuptools import setup, Extension

if sys.platform == 'linux2':
    define_macros = [ ('HAVE_AIO', None),
                    ]
    libraries = [ 'rt' ]
else:
    define_macros = []
    libraries = []

setup(name='ddar',
      version='1.0',
      author='Robie Basak',
      author_email='rb@synctus.com',
      url='http://www.synctus.com/ddar',
      packages=['synctus'],
      scripts=['ddar'],
      ext_modules=[ Extension('synctus._dds', ['scan.c', 'rabin.c',
                                           'synctus/ddsmodule.c'],
                              include_dirs=['.'],
                              libraries=libraries,
                              define_macros=define_macros) ],
      install_requires=['protobuf'])

# vim: set ts=8 sts=4 sw=4 ai et :
