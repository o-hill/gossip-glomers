cargo build --bin kafka
# 5a (single node)
# maelstrom/maelstrom test -w kafka --bin target/debug/kafka --node-count 1 --concurrency 2n --time-limit 20 --rate 1000
# 5b (multi node)
maelstrom/maelstrom test -w kafka --bin target/debug/kafka --node-count 2 --concurrency 2n --time-limit 20 --rate 1000
