#outgoing rule
tc qdisc del dev eth0 root
tc qdisc add dev eth0 root netem delay 10ms #5ms distribution normal

#incoming rule
tc qdisc del dev ifb0 root || true

modprobe ifb
ip link set dev ifb0 up
tc qdisc add dev eth0 ingress
tc filter add dev eth0 parent ffff: protocol ip u32 match u32 0 0 flowid 1:1 action mirred egress redirect dev ifb0
tc qdisc add dev ifb0 root netem delay 10ms #5ms distribution normal
