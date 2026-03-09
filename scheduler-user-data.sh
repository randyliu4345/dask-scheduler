#!/bin/bash
set -e

apt update
apt install -y python3 python3-pip git

cd /home/ubuntu
git https://github.com/randyliu4345/dask-scheduler dask-scheduler-main
cd dask-scheduler-main

pip3 install -r requirements.txt
pip3 install -e .

nohup dask scheduler > /home/ubuntu/scheduler.log 2>&1 &