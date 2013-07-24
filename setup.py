#!/usr/bin/python

from setuptools import setup, Extension

setup(name='ddar',
      version='0.1',
      author='Robie Basak',
      author_email='rb@synctus.com',
      url='http://www.synctus.com/ddar',
      packages=['synctus'],
      scripts=['ddar'],
      ext_modules=[ Extension('synctus._dds', ['scan.c', 'rabin.c',
                                           'synctus/ddsmodule.c'],
                              include_dirs=['.']) ],
      install_requires=['protobuf'])

# vim: set ts=8 sts=4 sw=4 ai et :
