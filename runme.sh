#!/bin/bash
set -e # bail-out if anything goes wrong

sudo apt update

sudo apt install python3-pip
pip3 install -r requirements.txt

sudo apt install awscli


#git clone https://github.com/mateusaubin/modeltest-loadexerciser.git
#cd modeltest-loadexerciser

export AWS_DEFAULT_REGION=$(curl -m5 -sS http://169.254.169.254/latest/meta-data/placement/availability-zone | sed 's/.$//')

mkdir log/ | true

sl=${3:=1800}
iters=${2:=11}

echo ""
echo "SLEEP: $Sl"
echo "ITERS: $iters"
echo ""

for i in `seq 1 $iters`;
do
  echo "---------------------------------------------"
  echo "              step $i                        "
  echo "---------------------------------------------"

  ./all.py $1

  echo "---------------------------------------------"
  echo "---------------------------------------------"
  echo "           sleeping $sl                [$i / $iters]"
  echo "---------------------------------------------"
  echo "---------------------------------------------"
  sleep $sl
  
done 
