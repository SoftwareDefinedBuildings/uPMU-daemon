#!/bin/bash
target=$1
scp -i upmukey sender-arm admin@$target:/tmp/newtxagent
ssh -i upmukey admin@$target "su -c 'killall 410txagent ; rm -f /tmp/410txagent.log && cp /tmp/newtxagent /root/410txagent && /etc/init.d/S80txagent start'"
