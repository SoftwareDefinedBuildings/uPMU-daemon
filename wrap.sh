#!/bin/bash
cpref=$(date +%Y-%m-%d)
cdate=$(date +%Y-%m-%d_%H.%M)
fname=$cdate.$1.log
ldest=/var/www/www/logs/$cpref/
mkdir -p $ldest
bash -x /root/$1.sh >$ldest/$fname 2>&1
