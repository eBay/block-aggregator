#!/usr/bin/env bash

# You could also test random drops:
sudo iptables -A OUTPUT -p tcp --dport 2181 -j REJECT --reject-with tcp-reset -m statistic --mode random --probability 0.1
sudo ip6tables -A OUTPUT -p tcp --dport 2181 -j REJECT --reject-with tcp-reset -m statistic --mode random --probability 0.1

