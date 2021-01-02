#!/bin/bash

cryptosess="crypto-crawl"
tmux -S /tmp/twitter has-session -t $cryptosess 2>/dev/null
if [ $? != 0 ]; then
    tmux -S /tmp/twitter new -s crypto-crawl
    tmux -S /tmp/twitter send -t crypto-crawl "bash" ENTER
    tmux -S /tmp/twitter send -t crypto-crawl "cd /home/dslab2020/twitterDSlab2020/stable" ENTER
    tmux -S /tmp/twitter send -t crypto-crawl "source .tenv/bin/activate" ENTER
    tmux -S /tmp/twitter send -t crypto-crawl "cd stable" ENTER
fi
crypto_crawl=$(pgrep -af "python stream_watcher.py crypto 0")
if [ -z "$crypto_crawl" ]; then
    tmux -S /tmp/twitter send -t crypto-crawl "python stream_watcher.py crypto 0" ENTER
fi

snp500sess="snp500-crawl"
tmux -S /tmp/twitter has-session -t $snp500sess 2>/dev/null
if [ $? != 0 ]; then
    tmux -S /tmp/twitter new -s snp500-crawl
    tmux -S /tmp/twitter send -t snp500-crawl "bash" ENTER
    tmux -S /tmp/twitter send -t snp500-crawl "cd /home/dslab2020/twitterDSlab2020/stable" ENTER
    tmux -S /tmp/twitter send -t snp500-crawl "source .tenv/bin/activate" ENTER
fi
snp500_crawl=$(pgrep -af "python stream_watcher.py snp500 1")
if [ -z "$snp500_crawl" ]; then
    tmux -S /tmp/twitter send -t snp500-crawl "python stream_watcher.py snp500 1" ENTER
fi

electionsess="election-crawl"
tmux -S /tmp/twitter has-session -t $electionsess 2>/dev/null
if [ $? != 0 ]; then
    tmux -S /tmp/twitter new -s election-crawl
    tmux -S /tmp/twitter send -t election-crawl "bash" ENTER
    tmux -S /tmp/twitter send -t election-crawl "cd /home/dslab2020/twitterDSlab2020/stable" ENTER
    tmux -S /tmp/twitter send -t election-crawl "source .tenv/bin/activate" ENTER
fi
election_crawl=$(pgrep -af "python stream_watcher.py election 2")
if [ -z "$election_crawl" ]; then
    tmux -S /tmp/twitter send -t election-crawl "python stream_watcher.py election 2" ENTER
fi

dbsess="db-store"
tmux -S /tmp/twitter has-session -t $dbsess 2>/dev/null
if [ $? != 0 ]; then
    tmux -S /tmp/twitter new -s db-store
    tmux -S /tmp/twitter send -t db-store "bash" ENTER
    tmux -S /tmp/twitter send -t db-store "cd /home/dslab2020/twitterDSlab2020/stable" ENTER
    tmux -S /tmp/twitter send -t db-store "source .tenv/bin/activate" ENTER
fi
store=$(pgrep -af "python scheduler.py")
if [ -z "$store" ]; then
    tmux -S /tmp/twitter send -t db-store "python scheduler.py" ENTER
fi
