#
# setup.py
#
#
# The MIT License
#
# Copyright (c) 2019 TileDB, Inc.
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
from setuptools.command.bdist_egg import bdist_egg
from wheel.bdist_wheel import bdist_wheel

import multiprocessing
import os
import shutil
import subprocess
import sys

TILEDBVCF_DEBUG_BUILD = False
TILEDBVCF_S3 = True
LIBTILEDBVCF_PATH = os.environ.get("LIBTILEDBVCF_PATH", None)
DOWNLOAD_TILEDB_PREBUILT = True

args = sys.argv[:]
for arg in args:
    if arg.find("--debug") == 0:
        TILEDBVCF_DEBUG_BUILD = True
        sys.argv.remove(arg)
    if arg.find("--libtiledbvcf") == 0:
        LIBTILEDBVCF_PATH = arg.split("=")[1]
        sys.argv.remove(arg)
    if arg.find("--disable-s3") == 0:
        TILEDBVCF_S3 = False
        sys.argv.remove(arg)
    if arg.find("--disable-download-tiledb-prebuilt") == 0:
        DOWNLOAD_TILEDB_PREBUILT = False
        sys.argv.remove(arg)


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


class PathConfig(object):
    """Helper class with some path information."""

    # Directory containing this file
    containing_dir = os.path.abspath(os.path.dirname(__file__))

    # Path of the Python package source
    pkg_src = "src/tiledbvcf"

    # Source directory of TileDB-VCF native library
    native_lib_src_dir = "../../libtiledbvcf"

    # Build directory of TileDB-VCF native library
    native_lib_build_dir = "../../libtiledbvcf/build"

    # Path where TileDB-VCF native library should be installed
    native_lib_install_dirs = ["../../dist"]


def find_libtiledbvcf():
    p = PathConfig()

    if LIBTILEDBVCF_PATH:
        p.native_lib_install_dirs = [LIBTILEDBVCF_PATH]

    libdirs = ["lib"]
    libnames = ["libtiledbvcf.dylib", "libtiledbvcf.so", "tiledbvcf.lib"]
    for root in p.native_lib_install_dirs:
        for libdir in libdirs:
            for libname in libnames:
                path = os.path.abspath(os.path.join(root, libdir, libname))
                if os.path.exists(path):
                    return path
    return None


def get_cmake_overrides():
    import sys

    conf = list()

    key = "TILEDBVCF_CMAKE_PREFIX_PATH"
    val = os.environ.get(key, default=None)
    if val:
        conf.append("-DCMAKE_PREFIX_PATH={}".format(val))

    key = "TILEDBVCF_FORCE_EXTERNAL_HTSLIB"
    val = os.environ.get(key, default="ON")
    conf.append("-DFORCE_EXTERNAL_HTSLIB={}".format(val))

    key = "TILEDBVCF_FORCE_EXTERNAL_TILEDB"
    val = os.environ.get(key, default="OFF")
    conf.append("-DFORCE_EXTERNAL_TILEDB={}".format(val))

    val = "ON" if TILEDBVCF_S3 else "OFF"
    conf.append("-DTILEDB_S3={}".format(val))

    val = "ON" if DOWNLOAD_TILEDB_PREBUILT else "OFF"
    conf.append("-DDOWNLOAD_TILEDB_PREBUILT={}".format(val))

    val = "Debug" if TILEDBVCF_DEBUG_BUILD else "Release"
    conf.append("-DCMAKE_BUILD_TYPE={}".format(val))

    conf.append("-DENABLE_ARROW_EXPORT=ON")

    return conf


def build_libtiledbvcf():
    p = PathConfig()

    install_dir = p.native_lib_install_dirs[0]
    build_dir = p.native_lib_build_dir
    src_dir = p.native_lib_src_dir
    os.makedirs(build_dir, exist_ok=True)

    cmake_cmd = [os.environ.get("CMAKE", "cmake")]
    cmake_cmd.extend(get_cmake_overrides())
    cmake_cmd.append(src_dir)

    print("CMake configure command: {}".format(cmake_cmd))

    build_cmd = ["make", "-j{}".format(multiprocessing.cpu_count() or 2)]
    install_cmd = ["make", "install-libtiledbvcf"]

    subprocess.check_call(cmake_cmd, cwd=build_dir)
    subprocess.check_call(build_cmd, cwd=build_dir)
    subprocess.check_call(install_cmd, cwd=build_dir)


def find_or_build_libtiledbvcf(setuptools_cmd):
    # Get a handle to the extension module
    tiledbvcf_ext = None
    for ext in setuptools_cmd.distribution.ext_modules:
        if ext.name == "tiledbvcf.libtiledbvcf":
            tiledbvcf_ext = ext
            break

    # Find the native library
    lib_path = find_libtiledbvcf()
    if lib_path is None:
        build_libtiledbvcf()
        lib_path = find_libtiledbvcf()
        if lib_path is None:
            raise Exception("Could not find native libtiledbvcf after building.")

    # Update the extension module with correct paths.
    lib_dir = os.path.dirname(lib_path)
    inc_dir = os.path.abspath(os.path.join(lib_dir, "..", "include"))
    tiledbvcf_ext.library_dirs += [lib_dir]
    tiledbvcf_ext.include_dirs += [inc_dir]

    # Copy native libs into the package dir so they can be found by package_data
    if LIBTILEDBVCF_PATH is None:
        native_libs = [os.path.join(lib_dir, f) for f in os.listdir(lib_dir)]
        p = PathConfig()
        package_data = []
        for obj in native_libs:
            print("Copying file {0} to {1}".format(obj, p.pkg_src))
            shutil.copy(obj, p.pkg_src)
            package_data.append(os.path.basename(obj))

        # Install shared libraries inside the Python module via package_data.
        print("Adding to package_data: {}".format(package_data))
        setuptools_cmd.distribution.package_data.update({"tiledbvcf": package_data})


def get_ext_modules():
    p = PathConfig()
    src_files = ["libtiledbvcf.cc", "reader.cc", "writer.cc"]
    src_files = [os.path.join(p.pkg_src, "binding", f) for f in src_files]
    ext_modules = [
        Extension(
            "tiledbvcf.libtiledbvcf",
            src_files,
            include_dirs=[get_pybind_include(), get_pybind_include(user=True)],
            libraries=["tiledbvcf"],
            library_dirs=[],
            language="c++",
        )
    ]
    return ext_modules


class BuildExtCmd(build_ext):
    """Builds the Pybind11 extension module."""

    def build_extensions(self):
        if sys.platform != "win32":
            opts = ["-std=c++17", "-g"]
            if TILEDBVCF_DEBUG_BUILD:
                opts.extend(["-O0"])
            else:
                opts.extend(["-O2"])
        else:  # windows
            # Note: newer versions of msvc cl may not recognize -std:c++17
            opts = ["-std:c++17", "-Zi"]
            if TILEDBVCF_DEBUG_BUILD:
                opts.extend(["-Od"])
            else:
                opts.extend(["-O2"])

        link_opts = []
        for ext in self.extensions:
            ext.extra_compile_args = opts
            ext.extra_link_args = link_opts

            import pyarrow

            # unversioned symlinks to arrow libraries are required for wheels
            # https://github.com/apache/arrow/blob/main/docs/source/python/integration/extending.rst#building-extensions-against-pypi-wheels
            pyarrow.create_library_symlinks()
            ext.libraries.extend(pyarrow.get_libraries())
            ext.include_dirs.append(pyarrow.get_include())

            # don't overlink the arrow core library
            if (sys.platform != "win32") and ("arrow" in ext.libraries):
                ext.libraries.remove("arrow")
            ext.library_dirs.extend(pyarrow.get_library_dirs())

        find_or_build_libtiledbvcf(self)
        build_ext.build_extensions(self)


class BdistEggCmd(bdist_egg):
    def run(self):
        find_or_build_libtiledbvcf(self)
        bdist_egg.run(self)


class BdistWheelCmd(bdist_wheel):
    def run(self):
        find_or_build_libtiledbvcf(self)
        bdist_wheel.run(self)


setup(
    name="tiledbvcf",
    description="Efficient variant-call data storage and retrieval library "
    "using the TileDB storage library.",
    author="TileDB, Inc.",
    author_email="help@tiledb.io",
    maintainer="TileDB, Inc.",
    maintainer_email="help@tiledb.io",
    url="https://github.com/TileDB-Inc/TileDB-VCF",
    license="MIT",
    packages=find_packages("src"),
    package_dir={"": "src"},
    setup_requires=[
        "setuptools>=18.0",
        "setuptools_scm>=1.5.4",
        "wheel>=0.30",
        "pybind11>=2.3.0",
        "setuptools_scm_git_archive",
    ],
    install_requires=[],
    tests_require=[],
    test_suite="tests",
    ext_modules=get_ext_modules(),
    cmdclass={
        "build_ext": BuildExtCmd,
        "bdist_egg": BdistEggCmd,
        "bdist_wheel": BdistWheelCmd,
    },
    zip_safe=False,
    use_scm_version={
        "version_scheme": "guess-next-dev",
        "local_scheme": "dirty-tag",
        "write_to": "apis/python/src/tiledbvcf/version.py",
        "root": "../..",
        "relative_to": __file__,
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Intended Audience :: Information Technology",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Operating System :: Unix",
        "Operating System :: POSIX :: Linux",
        "Operating System :: MacOS :: MacOS X",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
    ],
)
