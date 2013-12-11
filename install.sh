#!/bin/bash

set -e

apt-get install -y build-essential
apt-get install -y git
apt-get install -y python3-setuptools
apt-get install -y python3-minimal
apt-get install -y python3-dev
apt-get install -y libboost-python-dev
apt-get install -y libssl-dev
apt-get install -y python3-nose
apt-get install -y libyaml-dev

easy_install3 logging
easy_install3 autologging
easy_install3 pylint
easy_install3 pytz
easy_install3 numpy
easy_install3 "numexpr>=2.0"
easy_install3 pandas
easy_install3 pyyaml

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

mkdir -p tmp
pushd tmp
git clone https://github.com/Spooner/bunch.git
pushd bunch
git checkout -t origin/patch-1
python3 setup.py install
popd
rm -rf bunch
popd

mkdir -p tmp
pushd tmp

wget https://github.com/flexiondotorg/oab-java6/raw/0.3.0/oab-java.sh -O oab-java.sh
bash oab-java.sh
apt-get install -y sun-java6-jre
export JAVA_HOME=/usr/lib/jvm/java-6-sun

wget http://archive.cloudera.com/cdh4/one-click-install/precise/amd64/cdh4-repository_1.0_all.deb
dpkg -i cdh4-repository_1.0_all.deb
rm cdh4-repository_1.0_all.deb
wget http://archive.cloudera.com/cdh4/ubuntu/precise/amd64/cdh/archive.key
apt-key add archive.key
rm archive.key
apt-get update
apt-get install -y hadoop-conf-pseudo
dpkg -L hadoop-conf-pseudo
sudo -u hdfs hdfs namenode -format
popd

# add to /etc/hadoop/conf/hdfs-site.xml 
#
#<property>
#  <name>dfs.permissions</name>
#  <value>false</value>
#</property>
#
#<property>
#  <name>dfs.webhdfs.enabled</name>
#  <value>true</value>
#</property>
#

./hdfs-start.sh
#hdfs dfs -mkdir -p /inbound/bloomberg

easy_install3 pywebhdfs

easy_install3 SQLAlchemy

apt-get install -y postgresql postgresql-contrib
apt-get install -y libpq-dev
easy_install3 psycopg2
sudo -u postgres createuser heck -s -P
sudo -u postgres createdb heck

# patch
# vim /usr/local/lib/python3.2/dist-packages/pywebhdfs-0.2.1-py3.2.egg/pywebhdfs/webhdfs.py
# line 178 -  "return response.content" not "return response.text"

# patch
# vim /usr/local/lib/python3.2/dist-packages/tables-3.0.0-py3.2-linux-x86_64.egg/tables/attributeset.py 
# line 379 and 342 - "elif name == 'FILTERS':" not "elif name == 'FILTERS' and format_version >= (2, 0):"

