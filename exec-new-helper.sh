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

sl=${4:-10}
mode=${3:-0}
iters=${2:-1}
type=""

echo ""
echo "SLEEP: $sl"
echo "ITERS: $iters"
echo ""

for i in `seq 1 $iters`;
do
  echo "---------------------------------------------"
  echo "              step $i                        "
  echo "---------------------------------------------"

  # 0 - MIXED
  # 1 - LAMBDA ONLY
  # 2 - BATCH ONLY

  ./all.py $1 $mode $type || true

  echo "---------------------------------------------"
  echo "---------------------------------------------"
  echo "           sleeping $sl              [$i / $iters]"
  echo "---------------------------------------------"
  echo "---------------------------------------------"
  sleep $sl

done 
