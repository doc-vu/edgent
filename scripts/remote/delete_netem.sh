#!/bin/bash

#get the name of ethernet interface
interface=`ifconfig | grep Ethernet | awk 'NR==1 { print $1 }'`
echo $interface

#delete outgoing rule
tc qdisc del dev $interface root

#delete incoming rule
tc qdisc del dev ifb0 root || true
