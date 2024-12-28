cargo build --bin kafka
# 5a (single node)
# maelstrom/maelstrom test -w kafka --bin target/debug/kafka --node-count 1 --concurrency 2n --time-limit 20 --rate 1000
# 5b (multi node), c (efficiency)
maelstrom/maelstrom test -w kafka --bin target/debug/kafka --node-count 2 --concurrency 2n --time-limit 20 --rate 1000

# n0 CAS FAILURES: 21 / TOTAL APPENDS: 4061
# n1 CAS FAILURES: 21 / TOTAL APPENDS: 2755

# After 5b:
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

# After switching commits to seq-kv:
# :net {:all {:send-count 188736,
#              :recv-count 188736,
#              :msg-count 188736,
#              :msgs-per-op 12.284301},
#        :clients {:send-count 34152,
#                  :recv-count 34152,
#                  :msg-count 34152},
#        :servers {:send-count 154584,
#                  :recv-count 154584,
#                  :msg-count 154584,
#                  :msgs-per-op 10.061442},
#        :valid? true},
#  :workload {:valid? true,
#             :worst-realtime-lag {:time 36.23470875,
#                                  :process 23,
#                                  :key "9",
#                                  :lag 36.126094709},

# After breaking up logs into individual entries in lin-kv:
# n0 CAS FAILURES: 0 / TOTAL APPENDS: 5046
# n1 CAS FAILURES: 0 / TOTAL APPENDS: 2010
# n0 CAS FAILURES: 54 / TOTAL OFFSET WRITES: 1548
# n1 CAS FAILURES: 65 / TOTAL OFFSET WRITES: 5544
# :net {:all {:send-count 351271,
#              :recv-count 351271,
#              :msg-count 351271,
#              :msgs-per-op 23.640285},
#        :clients {:send-count 35103,
#                  :recv-count 35103,
#                  :msg-count 35103},
#        :servers {:send-count 316168,
#                  :recv-count 316168,
#                  :msg-count 316168,
#                  :msgs-per-op 21.277878},
#        :valid? true},
#  :workload {:valid? true,
#             :worst-realtime-lag {:time 38.661750584,
#                                  :process 24,
#                                  :key "9",
#                                  :lag 38.577971375},