#!/usr/bin/env bash


sudo yum install -y bzip2

export CONDA_LOCATION=/anaconda

curl https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh > miniconda.sh

sudo bash miniconda.sh -b -p $CONDA_LOCATION
echo "export PATH=$CONDA_LOCATION/bin:"'$PATH' > tmp
sudo bash <<- EOS
  cat tmp >> /etc/bashrc
  rm tmp
EOS