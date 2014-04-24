sudo -u postgres pg_dump -Fc heck > heck_2014_04_24.bak
sudo -u postgres pg_dump heck > heck_2014_04_24.sql

#sudo -u postgres pg_restore -Fc -C heck.bak
