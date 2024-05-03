#!/usr/bin/env bash

# Install htslib dependencies
yum install -y libcurl-devel bzip2-devel xz-devel openssl-devel

cd /io/

# Build libtiledbvcf
mkdir -p libtiledbvcf/build
cd libtiledbvcf/build
cmake -DTILEDBVCF_ENABLE_TESTING=OFF ..
make -j && make install-libtiledbvcf
cd -

# Build Python wheels
wheels=/io/apis/python/wheels
rm -rf ${wheels}
/opt/python/cp39-cp39/bin/pip wheel -vvv apis/python -w ${wheels}
auditwheel repair ${wheels}/tiledbvcf-*.whl -w ${wheels}
