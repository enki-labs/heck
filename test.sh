
sudo -u postgres dropdb heck_test
sudo -u postgres createdb heck_test
nosetests3 -v test/*

