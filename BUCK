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

smallbank_headers = [
    'benchmark/smallbank/table_decl.h',
    'benchmark/smallbank/smallbank.h',
    'benchmark/smallbank/balance.h',
    'benchmark/smallbank/deposit_checking.h',
    'benchmark/smallbank/transact_saving.h',
    'benchmark/smallbank/amalgamate.h',
    'benchmark/smallbank/write_check.h',
]

smallbank_srcs = [
    'benchmark/smallbank/smallbank.cc',
    'benchmark/smallbank/smallbank_workload.cc',
    'benchmark/smallbank/balance.cc',
    'benchmark/smallbank/deposit_checking.cc',
    'benchmark/smallbank/transact_saving.cc',
    'benchmark/smallbank/amalgamate.cc',
    'benchmark/smallbank/write_check.cc',
]

mcbm_headers = [
    'benchmark/mcbm/table_decl.h',
    'benchmark/mcbm/mcbm.h',
    'benchmark/mcbm/mcbm_insert.h',
    'benchmark/mcbm/mcbm_lookup.h',
    'benchmark/mcbm/mcbm_rangescan.h',
    'benchmark/mcbm/mcbm_update.h',
    'benchmark/mcbm/mcbm_delete.h',
]

mcbm_srcs = [
    'benchmark/mcbm/mcbm.cc',
    'benchmark/mcbm/mcbm_workload.cc',
    'benchmark/mcbm/mcbm_insert.cc',
    'benchmark/mcbm/mcbm_lookup.cc',
    'benchmark/mcbm/mcbm_rangescan.cc',
    'benchmark/mcbm/mcbm_update.cc',
    'benchmark/mcbm/mcbm_delete.cc',
]

db_headers = [
    'console.h', 'felis_probes.h', 'epoch.h', 'routine_sched.h', 'gc.h', 'gc_dram.h', 'index.h', 'index_common.h',
    'log.h', 'mem.h', 'pmem_mgr.h', 'module.h', 'opts.h', 'node_config.h', 'probe_utils.h', 'piece.h', 'piece_cc.h',
    'masstree_index_impl.h', 'dptree_index_impl.h', 'hashtable_index_impl.h', 'varstr.h', 'sqltypes.h',
    'txn.h', 'txn_cc.h', 'index_info.h', 'vhandle.h', 'vhandle_sync.h', 'contention_manager.h', 'locality_manager.h', 'threshold_autotune.h',
    'commit_buffer.h', 'shipping.h', 'completion.h', 'entity.h',
    'slice.h', 'vhandle_cch.h', 'tcp_node.h',
    'util/arch.h', 'util/factory.h', 'util/linklist.h', 'util/locks.h', 'util/lowerbound.h', 'util/objects.h', 'util/random.h', 'util/types.h',
    'pwv_graph.h', 'dptree/include/concur_dptree.hpp', 'masstree/masstree_insert.hh', 'dptree/include/art_idx.hpp'
]

db_srcs = [
    'epoch.cc', 'routine_sched.cc', 'txn.cc', 'log.cc', 'index_info.cc', 'vhandle.cc', 'vhandle_sync.cc', 'contention_manager.cc', 'locality_manager.cc',
    'gc.cc', 'gc_dram.cc', 'index.cc', 'mem.cc', 'pmem_mgr.cc',
    'piece.cc', 'masstree_index_impl.cc', 'dptree_index_impl.cc', 'hashtable_index_impl.cc',
    'node_config.cc', 'console.cc', 'console_client.cc',
    'commit_buffer.cc', 'shipping.cc', 'entity.cc', 'iface.cc', 'slice.cc', 'tcp_node.cc',
    'felis_probes.cc',
    'json11/json11.cpp',
    'spdlog/src/spdlog.cpp', 'spdlog/src/fmt.cpp', 'spdlog/src/stdout_sinks.cpp', 'spdlog/src/async.cpp', 'spdlog/src/cfg.cpp', 'spdlog/src/color_sinks.cpp', 'spdlog/src/file_sinks.cpp',
    'xxHash/xxhash.c', 'util/os_linux.cc', 'util/locks.cc',
    'pwv_graph.cc',
    'gopp/gopp.cc', 'gopp/channels.cc',
    'gopp/asm.S'] + [
        ('masstree/kvthread.cc', ['-include', 'masstree/build/config.h']),
	('masstree/string.cc', ['-include', 'masstree/build/config.h']),
	('masstree/straccum.cc', ['-include', 'masstree/build/config.h']),
    ] + [
        ('dptree/src/util.cpp', ['-include', 'dptree/include/util.h']),
        ('dptree/src/MurmurHash2.cpp', ['-include', 'dptree/include/MurmurHash2.h']),
        ('dptree/src/art_idx.cpp')
    ]

#shirley pmem: add libs for dptree
libs = ['-pthread', '-lrt', '-ldl', '-ltbb', '-ltcmalloc_minimal', '-lpmem', '-lpmemobj']
#libs = ['-pthread', '-lrt', '-ldl']
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

cxx_library(
    name='smallbank',
    srcs=smallbank_srcs,
    compiler_flags=includes,
    headers=db_headers + smallbank_headers,
    link_whole=True,
)

cxx_library(
    name='mcbm',
    srcs=mcbm_srcs,
    compiler_flags=includes,
    headers=db_headers + mcbm_headers,
    link_whole=True,
)

cxx_binary(
    name='db',
    srcs=['main.cc', 'module.cc'] + db_srcs,
    headers=db_headers,
    compiler_flags=includes,
    linker_flags=libs,
    deps=[':tpcc', ':smallbank', ':mcbm', ':ycsb'],
)

cxx_test(
    name='dbtest',
    srcs=test_srcs + db_srcs,
    headers=db_headers,
    compiler_flags=includes,
    linker_flags=libs + ['-lgtest_main', '-lgtest'],
    deps=[':tpcc']
)
