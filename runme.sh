#!/bin/bash
set -e # bail-out if anything goes wrong

sudo apt update

# keep time in sync
sudo apt install chrony
chronyconf='/etc/chrony/chrony.conf'
grep -q -e '169.254.169.123' $chronyconf || sudo sed -i '1iserver 169.254.169.123 prefer iburst' $chronyconf
sudo service chronyd start
sudo chronyc makestep

sudo apt install python3-pip
pip3 install -r requirements.txt

sudo apt install awscli


#git clone https://github.com/mateusaubin/modeltest-loadexerciser.git
#cd modeltest-loadexerciser

export AWS_DEFAULT_REGION=$(curl -m5 -sS http://169.254.169.254/latest/meta-data/placement/availability-zone | sed 's/.$//')

mkdir log/ | true

sl=${3:-1800}
iters=${2:-11}

echo ""
echo "SLEEP: $sl"
echo "ITERS: $iters"
echo ""

for i in `seq 1 $iters`;
do
  echo "---------------------------------------------"
  echo "              step $i                        "
  echo "---------------------------------------------"

  #  MIXED
  #./all.py $1 False || true
  
  #  LAMBDA-ONLY
  #./all.py $1 True || true
  
  #  BATCH-ONLY
  #./batchonly.py $1 False SpotComputeEnvironment-XXXXXXXXXXXXXXX BatchJobQueue-XXXXXXXXXXXXXXX BatchJobDef-XXXXXXXXXXXXXXX:1 || true

  echo "---------------------------------------------"
  echo "---------------------------------------------"
  echo "           sleeping $sl                [$i / $iters]"
  echo "---------------------------------------------"
  echo "---------------------------------------------"
  sleep $sl
  
done 
