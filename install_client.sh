#!/bin/bash

set -e

apt-get update

apt-get install -y build-essential
apt-get install -y git
apt-get install -y python3-setuptools
apt-get install -y python3-minimal
apt-get install -y python3-dev
apt-get install -y libboost-python-dev
apt-get install -y libssl-dev
apt-get install -y libyaml-dev

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

easy_install3 -U distribute
mkdir -p tmp
pushd tmp
wget https://raw.github.com/pypa/pip/master/contrib/get-pip.py
python3 get-pip.py
rm get-pip.py
popd

mkdir -p tmp
pushd tmp
git clone https://github.com/Spooner/bunch.git
pushd bunch
git checkout -t origin/patch-1
python3 setup.py install
popd
rm -rf bunch
popd

pip install decorator
pip install autologging
pip install pytz
pip install numpy
pip install "numexpr>=2.0"
pip install pandas
pip install pyyaml
pip install pywebhdfs
pip install python3-memcached
pip install redis
pip install Celery
pip install -U kombu==3.0.12

apt-get install -y libhdf5-serial-dev
pip install tables

apt-get install -y postgresql postgresql-contrib
apt-get install -y libpq-dev
pip install psycopg2
pip install SQLAlchemy


