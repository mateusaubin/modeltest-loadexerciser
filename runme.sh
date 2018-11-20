#!/bin/bash
set -e # bail-out if anything goes wrong

sudo apt update
sudo apt install python3-pip

pip3 install -r requirements.txt

export AWS_DEFAULT_REGION=$(curl -m5 -sS http://169.254.169.254/latest/meta-data/placement/availability-zone | sed 's/.$//')

./all.py $1