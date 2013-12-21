sudo -u postgres pg_dump -Fc heck > heck.bak
sudo -u postgres pg_dump heck > heck.sql

sudo -u postgres pg_restore -Fc -C heck.bak
