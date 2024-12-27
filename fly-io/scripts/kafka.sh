cargo build --bin kafka
# 5a (single node)
# maelstrom/maelstrom test -w kafka --bin target/debug/kafka --node-count 1 --concurrency 2n --time-limit 20 --rate 1000
# 5b (multi node), c (efficiency)
maelstrom/maelstrom test -w kafka --bin target/debug/kafka --node-count 2 --concurrency 2n --time-limit 20 --rate 1000

# │  :net {:all {:send-count 202713,
#  33   │              :recv-count 202713,
#  34   │              :msg-count 202713,
#  35   │              :msgs-per-op 12.342486},
#  36   │        :clients {:send-count 36851,
#  37   │                  :recv-count 36851,
#  38   │                  :msg-count 36851},
#  39   │        :servers {:send-count 165862,
#  40   │                  :recv-count 165862,
#  41   │                  :msg-count 165862,
#  42   │                  :msgs-per-op 10.098758},
#  43   │        :valid? true},
#  44   │  :workload {:valid? true,
#  45   │             :worst-realtime-lag {:time 32.223135,
#  46   │                                  :process 22,
#  47   │                                  :key "9",
#  48   │                                  :lag 32.137025875}
