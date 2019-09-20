#
# setup.py
#
#
# The MIT License
#
# Copyright (c) 2018 TileDB, Inc.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
#
# Portions of this are based on:
# https://github.com/pybind/python_example/blob/master/setup.py
#

from setuptools import setup, find_packages, Extension
from setuptools.command.build_ext import build_ext

import os
import sys

# Directory containing this file
CONTAINING_DIR = os.path.abspath(os.path.dirname(__file__))

# Relative path of the package source
SRC_DIR = 'src/tiledbvcf'

# Installation directory of libtiledbvcf (C API).
# TODO : make this more portable
LIBTILEDBVCF_INSTALL_DIR = os.path.abspath(
    os.path.join(CONTAINING_DIR, '../../dist'))


class get_pybind_include(object):
    """Helper class to determine the pybind11 include path
    The purpose of this class is to postpone importing pybind11
    until it is actually installed, so that the ``get_include()``
    method can be invoked."""

    def __init__(self, user=False):
        self.user = user

    def __str__(self):
        import pybind11
        return pybind11.get_include(self.user)


def get_libtiledbvcf_include():
    return '{}/include'.format(LIBTILEDBVCF_INSTALL_DIR)


def get_libtiledbvcf_lib():
    libdirs = ['lib', 'lib64']
    if os.name == 'posix' and sys.platform == 'darwin':
        libname = 'libtiledbvcf.dylib'
    else:
        libname = 'libtiledbvcf.so'
    for dir in libdirs:
        path = os.path.join(LIBTILEDBVCF_INSTALL_DIR, dir, libname)
        if os.path.exists(path):
            return os.path.join(LIBTILEDBVCF_INSTALL_DIR, dir)
    raise Exception('Could not locate libtiledbvcf native library')


class BuildExt(build_ext):
    """A custom build extension for adding compiler-specific options."""

    def build_extensions(self):
        opts = ['-std=c++11', '-g', '-O2']
        link_opts = []
        for ext in self.extensions:
            ext.extra_compile_args = opts
            ext.extra_link_args = link_opts

            import pyarrow
            ext.include_dirs.append(pyarrow.get_include())
            ext.libraries.extend(pyarrow.get_libraries())
            ext.library_dirs.extend(pyarrow.get_library_dirs())

        build_ext.build_extensions(self)


ext_src_files = ['{}/binding/{}'.format(SRC_DIR, filename) for filename in
                 ['libtiledbvcf.cc', 'reader.cc', 'writer.cc']]
ext_modules = [
    Extension(
        'tiledbvcf.libtiledbvcf',
        ext_src_files,
        include_dirs=[
            get_pybind_include(),
            get_pybind_include(user=True),
            get_libtiledbvcf_include(),
        ],
        libraries=['tiledbvcf'],
        library_dirs=[
            get_libtiledbvcf_lib()
        ],
        language='c++'
    ),
]

setup(
    name='tiledbvcf',
    version='0.1.0',
    description='Efficient variant-call data storage and retrieval library '
                'using the TileDB storage library.',
    author='TileDB, Inc.',
    author_email='help@tiledb.io',
    maintainer='TileDB, Inc.',
    maintainer_email='help@tiledb.io',
    url='https://github.com/TileDB-Inc/TileDB-VCF',
    license='MIT',
    packages=find_packages('src'),
    package_dir={'': 'src'},
    setup_requires=[],
    install_requires=[],
    tests_require=[],
    test_suite='tests',
    ext_modules=ext_modules,
    cmdclass={'build_ext': BuildExt},
    zip_safe=False,
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Intended Audience :: Information Technology',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Operating System :: Unix',
        'Operating System :: POSIX :: Linux',
        'Operating System :: MacOS :: MacOS X',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
)
