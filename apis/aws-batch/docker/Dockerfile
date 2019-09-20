FROM amazonlinux:latest

WORKDIR /tmp/

# Install some dependencies
RUN yum -y install which wget tar gzip unzip gcc-c++ zlib-devel openssl-devel \
        bzip2-devel tbb-devel libcurl-devel xz-devel make automake autoconf \
    && yum clean all

# Install a newer version of CMake (TileDB requires >= 3.3)
RUN wget https://cmake.org/files/v3.12/cmake-3.12.3-Linux-x86_64.sh \
    && sh cmake-3.12.3-Linux-x86_64.sh --skip-license --prefix=/usr/ \
    && rm cmake-3.12.3-Linux-x86_64.sh

# Download and install TileDB dev
RUN wget https://github.com/TileDB-Inc/TileDB/archive/libtiledbvcf.zip \
    && unzip libtiledbvcf.zip \
    && pushd TileDB-libtiledbvcf \
    && mkdir build \
    && cd build \
    && ../bootstrap --prefix=/usr/local --enable-s3 --enable-verbose \
    && make -j2 \
    && make install-tiledb \
    && popd \
    && rm -rf TileDB-libtiledbvcf libtiledbvcf.zip

# Download and install htslib
RUN wget https://github.com/samtools/htslib/archive/1.8.zip \
    && unzip 1.8.zip \
    && pushd htslib-1.8 \
    && autoheader \
    && autoconf \
    && ./configure --prefix=/usr/local \
    && make -j2 \
    && make install \
    && popd \
    && rm -rf htslib-1.8 1.8.zip

# Copy the TileDB-Genomics directory and build it.
COPY . libtiledbvcf
RUN pushd libtiledbvcf \
    && rm -rf build \
    && mkdir build \
    && cd build \
    && cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=/usr/local \
        -DCMAKE_PREFIX_PATH=/usr/local .. \
    && make -j2 \
    && make -C libtiledbvcf install \
    && popd \
    && rm -rf libtiledbvcf

# Remove some unneeded dependencies after building.
RUN yum -y erase which wget tar gzip unzip make automake autoconf \
    && yum -y autoremove \
    && yum clean all

USER nobody
ENTRYPOINT ["/usr/local/bin/tiledbvcf"]