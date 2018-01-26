#!/usr/bin/env bash


sudo python configure.db.py
sudo yum install -y wget

LOCATION="https://download.postgresql.org/pub/repos/yum/10/redhat/rhel-7-x86_64/pgdg-centos10-10-2.noarch.rpm"
DATA_FOLDER="/var/lib/pgsql/10/data"

sudo yum install -y $LOCATION
sudo yum install -y postgresql10-server

sudo /usr/pgsql-10/bin/postgresql-10-setup initdb
sudo systemctl enable postgresql-10.service
sudo systemctl start postgresql-10.service

USER=timescale
cp create_user_db.sql /tmp/
echo $PASSWD > pw
sudo -n -u postgres bash <<-EOS
PASSWD=(cat pw)
psql -f /tmp/create_user_db.sql -v user=$USER -v passwd=$PASSWD
EOS
rm -rf pw

echo "host    $USER       $USER       0.0.0.0/0               md5" | sudo tee -a $DATA_FOLDER/pg_hba.conf

echo "listen_addresses = '*'"  | sudo tee -a $DATA_FOLDER/postgresql.conf

PACKAGE="timescaledb-0.8.0-postgresql-10-0.x86_64.rpm"
wget "https://timescalereleases.blob.core.windows.net/rpm/$PACKAGE"
sudo yum install -y $PACKAGE

echo "shared_preload_libraries = 'timescaledb'"  | sudo tee -a $DATA_FOLDER/pg_hba.conf
sudo systemctl restart postgresql-10.service

sudo -n -u postgres psql -d timescale -c "CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE"