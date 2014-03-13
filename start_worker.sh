#!/bin/bash

# output to logger
named_pipe=/tmp/$$.tmp
trap "rm -f $named_pipe" EXIT
mknod $named_pipe p
logger <$named_pipe -t $0 &
exec 1>$named_pipe 2>&1

set -e
cd /root/heck

echo "### sync time ###"
sudo service ntp stop
sudo ntpdate europe.pool.ntp.org
sudo service ntp start

echo "### connect data store ###"
sudo sshfs -o IdentityFile=/root/.ssh/ec2key.pem root@5.9.111.76:/home/data -o allow_other /home/data

echo "### update software ###"
git pull

echo "### start worker ###"
/usr/local/bin/celery worker -l INFO -O fair --config=task_config -n worker@%h -Q celery

echo "### end  ###"

