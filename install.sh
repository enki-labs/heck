#!/bin/bash

set -e

apt-get install python3-setuptools
apt-get install python3-minimal
apt-get install python3-dev

easy_install3 logging
easy_install3 autologging
easy_install3 pylint
easy_install3 pytz
easy_install3 numpy
easy_install3 "numexpr>=2.0"

mkdir -p tmp
pushd tmp
wget https://pypi.python.org/packages/source/C/Cython/Cython-0.19.2.tar.gz#md5=4af1218346510b464c0a6bf15500d0e2
tar -xvzf Cython-0.19.2.tar.gz
rm Cython-0.19.2.tar.gz
pushd Cython-0.19.2
python3 setup.py install
popd
rm -rf Cython-0.19.2
popd

easy_install3 tables

