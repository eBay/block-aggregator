#!/usr/bin/env bash

sudo iptables -A OUTPUT -p tcp --dport 2181 -j DROP
sudo ip6tables -A OUTPUT -p tcp --dport 2181 -j DROP


