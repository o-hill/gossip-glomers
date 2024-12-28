cargo build --bin kafka
# 5a (single node)
# maelstrom/maelstrom test -w kafka --bin target/debug/kafka --node-count 1 --concurrency 2n --time-limit 20 --rate 1000
# 5b (multi node), c (efficiency)
maelstrom/maelstrom test -w kafka --bin target/debug/kafka --node-count 2 --concurrency 2n --time-limit 20 --rate 1000 --nemesis partition

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

# After moving logs back into a single entry:
# n0 CAS FAILURES: 29 / TOTAL APPENDS: 4463
# n1 CAS FAILURES: 8 / TOTAL APPENDS: 2992
# :net {:all {:send-count 180186,
#              :recv-count 180186,
#              :msg-count 180186,
#              :msgs-per-op 11.430221},
#        :clients {:send-count 34948,
#                  :recv-count 34948,
#                  :msg-count 34948},
#        :servers {:send-count 145238,
#                  :recv-count 145238,
#                  :msg-count 145238,
#                  :msgs-per-op 9.213271},
#        :valid? true},
#  :workload {:valid? true,
#             :worst-realtime-lag {:time 34.380312375,
#                                  :process 9,
#                                  :key "9",
#                                  :lag 34.253547542},

# with topic sharding:
# n0 CAS FAILURES: 36 / TOTAL APPENDS: 3812
# n1 CAS FAILURES: 28 / TOTAL APPENDS: 3813
# :net {:all {:send-count 198527,
#             :recv-count 198527,
#             :msg-count 198527,
#             :msgs-per-op 12.514309},
#       :clients {:send-count 35039,
#                 :recv-count 35039,
#                 :msg-count 35039},
#       :servers {:send-count 163488,
#                 :recv-count 163488,
#                 :msg-count 163488,
#                 :msgs-per-op 10.305597},
#       :valid? true},
# :workload {:valid? true,
#            :worst-realtime-lag {:time 32.546758958,
#                                 :process 10,
#                                 :key "9",
#                                 :lag 32.425358708},

# after moving commits into a single key:
# :net {:all {:send-count 154525,
#             :recv-count 154525,
#             :msg-count 154525,
#             :msgs-per-op 9.141869},
#       :clients {:send-count 37447,
#                 :recv-count 37447,
#                 :msg-count 37447},
#       :servers {:send-count 117078,
#                 :recv-count 117078,
#                 :msg-count 117078,
#                 :msgs-per-op 6.9264627},
#       :valid? true},
# :workload {:valid? true,
#            :worst-realtime-lag {:time 30.880604791,
#                                 :process 13,
#                                 :key "9",
#                                 :lag 30.786042791},

# seems fine for now.

# How will these metrics scale with the number of nodes?
# At the moment, very poorly. All the data is wrapped into
# single keys for the sake of bringing down the number of
# messages required to complete a task. This will break
# down in a number of scenarios:
# 1. More data - if the log was too large to fit into a single
#    entry in storage this wouldn't work. Even if it could, at
#    some point the time it would take to send the data over the
#    network and process it would cause it to fail.
# 2. More nodes - more nodes means more competition on the same
#    keys. This can be solved by something like sharding where
#    each node would have a set of keys/topics it is responsible
#    for, and requests would be proxied to the shard leader.
#    I tried that approach here but it didn't have much of an
#    impact, I assume because there's not enough data to tell.
# At the moment this solution does not fail in the presence of
# network faults - this is because there is currently no
# internal communication. All of the data is stored and accessed
# in the storage for every read and write. This is a strength
# and a weakness: partitioning the nodes has no effect, but
# the storage system could be partitioned, and then the nodes
# would be completely locked. So it only really makes sense in this
# toy problem.
