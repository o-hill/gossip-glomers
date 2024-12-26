cargo build --bin broadcast
maelstrom/maelstrom test -w broadcast --bin target/debug/broadcast --node-count 25 --time-limit 20 --rate 100 --latency 100
cat store/latest/results.edn | grep -e "msgs-per-op" -e "stable-latencies" -C 3
