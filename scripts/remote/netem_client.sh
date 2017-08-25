#!/bin/bash

#get the name of ethernet interface
interface=`ifconfig | grep Ethernet | awk 'NR==1 { print $1 }'`
echo $interface

#outgoing rule
tc qdisc del dev $interface root
tc qdisc add dev $interface root netem delay 10ms #5ms distribution normal

#incoming rule
tc qdisc del dev ifb0 root || true

modprobe ifb
ip link set dev ifb0 up
tc qdisc add dev $interface ingress
tc filter add dev $interface parent ffff: protocol ip u32 match u32 0 0 flowid 1:1 action mirred egress redirect dev ifb0
tc qdisc add dev ifb0 root netem delay 10ms #5ms distribution normal
