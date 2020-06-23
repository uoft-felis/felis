# BUILD FILE SYNTAX: SKYLARK

includes = ['-I.', '-Ispdlog/include']

tpcc_headers = [
    'benchmark/tpcc/table_decl.h',
    'benchmark/tpcc/tpcc.h',
    'benchmark/tpcc/new_order.h',
    'benchmark/tpcc/payment.h',
    'benchmark/tpcc/delivery.h',
    'benchmark/tpcc/order_status.h',
    'benchmark/tpcc/stock_level.h',
]

tpcc_srcs = [
    'benchmark/tpcc/tpcc.cc',
    'benchmark/tpcc/tpcc_workload.cc',
    'benchmark/tpcc/new_order.cc',
    'benchmark/tpcc/payment.cc',
    'benchmark/tpcc/delivery.cc',
    'benchmark/tpcc/order_status.cc',
    'benchmark/tpcc/stock_level.cc',
]

ycsb_headers = [
    'benchmark/ycsb/table_decl.h',
    'benchmark/ycsb/ycsb.h',
]

ycsb_srcs = [
    'benchmark/ycsb/ycsb.cc',
    'benchmark/ycsb/ycsb_workload.cc',
]

db_headers = [
    'console.h', 'felis_probes.h', 'epoch.h', 'gc.h', 'index.h', 'index_common.h',
    'log.h', 'mem.h', 'module.h', 'opts.h', 'node_config.h', 'probe_utils.h', 'promise.h',
    'masstree_index_impl.h', 'hashtable_index_impl.h', 'varstr.h', 'sqltypes.h',
    'txn.h', 'txn_cc.h', 'vhandle.h', 'vhandle_sync.h', 'contention_manager.h', 'locality_manager.h',
    'shipping.h', 'completion.h', 'entity.h',
    'slice.h', 'vhandle_cch.h', 'tcp_node.h',
    'util/arch.h', 'util/factory.h', 'util/linklist.h', 'util/locks.h', 'util/lowerbound.h', 'util/objects.h', 'util/random.h', 'util/types.h'
]

db_srcs = [
    'epoch.cc', 'txn.cc', 'log.cc', 'vhandle.cc', 'vhandle_sync.cc', 'contention_manager.cc', 'locality_manager.cc',
    'gc.cc', 'index.cc', 'mem.cc',
    'promise.cc', 'masstree_index_impl.cc', 'hashtable_index_impl.cc',
    'node_config.cc', 'console.cc', 'console_client.cc',
    'shipping.cc', 'entity.cc', 'iface.cc', 'slice.cc', 'tcp_node.cc',
    'felis_probes.cc',
    'json11/json11.cpp',
    'spdlog/src/spdlog.cpp', 'spdlog/src/fmt.cpp', 'spdlog/src/stdout_sinks.cpp', 'spdlog/src/async.cpp', 'spdlog/src/cfg.cpp', 'spdlog/src/color_sinks.cpp', 'spdlog/src/file_sinks.cpp',
    'xxHash/xxhash.c', 'util/os_linux.cc', 'util/locks.cc',
    'gopp/gopp.cc', 'gopp/channels.cc',
    'gopp/asm.S'] + [
        ('masstree/kvthread.cc', ['-include', 'masstree/build/config.h']),
	('masstree/string.cc', ['-include', 'masstree/build/config.h']),
	('masstree/straccum.cc', ['-include', 'masstree/build/config.h']),
    ]

libs = ['-pthread', '-lrt', '-ldl']
#test_srcs = ['test/promise_test.cc', 'test/serializer_test.cc', 'test/shipping_test.cc']
test_srcs = ['test/xnode_measure_test.cc']

cxx_library(
    name='tpcc',
    srcs=tpcc_srcs,
    compiler_flags=includes,
    headers=db_headers + tpcc_headers,
    link_whole=True,
)

cxx_library(
    name='ycsb',
    srcs=ycsb_srcs,
    compiler_flags=includes,
    headers=db_headers + ycsb_headers,
    link_whole=True,
)

cxx_binary(
    name='db',
    srcs=['main.cc', 'module.cc'] + db_srcs,
    headers=db_headers,
    compiler_flags=includes,
    linker_flags=libs,
    deps=[':tpcc', ':ycsb'],
)

cxx_test(
    name='dbtest',
    srcs=test_srcs + db_srcs,
    headers=db_headers,
    compiler_flags=includes,
    linker_flags=libs + ['-lgtest_main', '-lgtest'],
    deps=[':tpcc']
)
