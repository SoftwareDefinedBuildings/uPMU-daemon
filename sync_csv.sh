#!/bin/bash
set -o errexit -o nounset -o xtrace

RSYNC="rsync -hav"
REMOTE="miranda.cs.berkeley.edu"
RBPATH="/home/sam/upmucsvgen/"
LPATH="/var/www/www/csv/"

eval $(ssh-agent)
ssh-add remote_identity.pem

for i in grizzly_peak switch_a6 soda_a soda_b
do
    $RSYNC $REMOTE:$RBPATH/$i $LPATH/$i
done
