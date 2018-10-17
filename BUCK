# BUILD FILE SYNTAX: SKYLARK

common_cflags = ['-pthread', '-Wstrict-aliasing', '-DCACHE_LINE_SIZE=64']
includes = ['-I.', '-Ispdlog/include']

tpcc_srcs = [
    'benchmark/tpcc/tpcc.cc',
    'benchmark/tpcc/tpcc_workload.cc',
    'benchmark/tpcc/new_order.cc',
]

db_headers = [
    'console.h', 'felis_probes.h', 'epoch.h', 'gc.h', 'index.h', 'index_common.h',
    'log.h', 'mem.h', 'module.h', 'node_config.h', 'probe.h', 'promise.h', 'sqltypes.h',
    'txn.h', 'util.h', 'vhandle.h',
]

cxx_library(
    name='tpcc',
    srcs=tpcc_srcs,
    compiler_flags=includes,
    headers=db_headers,
    link_whole=True,
)

db_srcs = [
    'epoch.cc', 'txn.cc', 'log.cc', 'vhandle.cc', 'gc.cc', 'index.cc', 'mem.cc',
    'promise.cc', 'masstree_index_impl.cc', 'node_config.cc', 'console.cc', 'console_client.cc',
    'felis_probes.cc',
    'json11/json11.cpp',
    'xxHash/xxhash.c',
    'gopp/gopp.cc', 'gopp/channels.cc',
    'gopp/asm.S'] + [
        ('masstree/kvthread.cc', ['-include', 'masstree/build/config.h']),
	('masstree/string.cc', ['-include', 'masstree/build/config.h']),
	('masstree/straccum.cc', ['-include', 'masstree/build/config.h']),
    ]

libs = ['-pthread', '-lrt', '-ldl', '-lnuma', '-l:libjemalloc.a']
test_srcs = ['test/promise_test.cc']

cxx_binary(
    name='db',
    srcs=['main.cc', 'module.cc', 'iface.cc'] + db_srcs,
    compiler_flags=includes,
    linker_flags=libs,
    deps=[':tpcc'],
)

cxx_test(
    name='dbtest',
    srcs=test_srcs + db_srcs,
    compiler_flags=includes,
    linker_flags=libs + ['-lgtest_main', '-lgtest'],
    deps=[':tpcc']
)
