# BUILD FILE SYNTAX: SKYLARK

includes = ['-I.', '-Ispdlog/include']

tpcc_headers = [
    'benchmark/tpcc/table_decl.h',
    'benchmark/tpcc/tpcc.h',
    'benchmark/tpcc/new_order.h',
    'benchmark/tpcc/payment.h',
    'benchmark/tpcc/delivery.h',
]

tpcc_srcs = [
    'benchmark/tpcc/tpcc.cc',
    'benchmark/tpcc/tpcc_workload.cc',
    'benchmark/tpcc/new_order.cc',
    'benchmark/tpcc/payment.cc',
    'benchmark/tpcc/delivery.cc',
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
    'log.h', 'mem.h', 'module.h', 'opts.h', 'node_config.h', 'probe_utils.h', 'promise.h', 'sqltypes.h',
    'txn.h', 'util.h', 'vhandle.h', 'vhandle_sync.h', 'vhandle_batchappender.h',
    'shipping.h', 'completion.h', 'entity.h',
    'slice.h', 'vhandle_cch.h', 'tcp_node.h',
]

db_srcs = [
    'epoch.cc', 'txn.cc', 'log.cc', 'vhandle.cc', 'vhandle_sync.cc', 'vhandle_batchappender.cc',
    'gc.cc', 'index.cc', 'mem.cc',
    'promise.cc', 'masstree_index_impl.cc', 'node_config.cc', 'console.cc', 'console_client.cc',
    'shipping.cc', 'entity.cc', 'iface.cc', 'slice.cc', 'tcp_node.cc',
    'felis_probes.cc',
    'json11/json11.cpp',
    'spdlog/src/spdlog.cpp',
    'xxHash/xxhash.c',
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
