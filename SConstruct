# -*- mode: python -*-

if GetOption('help'):
    print(
'''
Arguments:

BUILD_TYPE=debug|release|asan_debug|asan_release
Default: release

CC=C Compiler
Default: clang

CXX=C++ Compiler
Default: clang++

CCFLAGS=Extra C/C++ Compile Flags. For both C and C++

CFLAGS=Extra C Compile Flags.

CXXFLAGS=Extra C++ Compile Flags.

LINKFLAGS=Extra Link Flags.
Similar to LDFLAGS in autotools.
    
''')
    exit(0)

import os
import shutil

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
    'console.h', 'felis_probes.h', 'epoch.h', 'routine_sched.h', 'gc.h', 'index.h', 'index_common.h',
    'log.h', 'mem.h', 'module.h', 'opts.h', 'node_config.h', 'probe_utils.h', 'piece.h', 'piece_cc.h',
    'masstree_index_impl.h', 'hashtable_index_impl.h', 'varstr.h', 'sqltypes.h',
    'txn.h', 'txn_cc.h', 'vhandle.h', 'vhandle_sync.h', 'contention_manager.h', 'locality_manager.h', 'threshold_autotune.h',
    'commit_buffer.h', 'shipping.h', 'completion.h', 'entity.h',
    'slice.h', 'vhandle_cch.h', 'tcp_node.h',
    'util/arch.h', 'util/factory.h', 'util/linklist.h', 'util/locks.h', 'util/lowerbound.h', 'util/objects.h', 'util/random.h', 'util/types.h',
    'pwv_graph.h'
]

db_srcs = [
    'epoch.cc', 'routine_sched.cc', 'txn.cc', 'log.cc', 'vhandle.cc', 'vhandle_sync.cc', 'contention_manager.cc', 'locality_manager.cc',
    'gc.cc', 'index.cc', 'mem.cc',
    'piece.cc', 'hashtable_index_impl.cc',
    'node_config.cc', 'console.cc', 'console_client.cc',
    'commit_buffer.cc', 'shipping.cc', 'entity.cc', 'iface.cc', 'slice.cc', 'tcp_node.cc',
    'felis_probes.cc',
    'json11/json11.cpp',
    'spdlog/src/spdlog.cpp', 'spdlog/src/fmt.cpp', 'spdlog/src/stdout_sinks.cpp', 'spdlog/src/async.cpp', 'spdlog/src/cfg.cpp', 'spdlog/src/color_sinks.cpp', 'spdlog/src/file_sinks.cpp',
    'xxHash/xxhash.c', 'util/os_linux.cc', 'util/locks.cc',
    'pwv_graph.cc',
    'gopp/gopp.cc', 'gopp/channels.cc',
    'gopp/start-x86_64.S',
]

masstree_srcs = [
    'masstree_index_impl.cc',
    'masstree/kvthread.cc',
    'masstree/string.cc',
    'masstree/straccum.cc',
]

passthrough_opts = {k: os.environ.get(k, '') + ARGUMENTS.get(k, '') for k in ['CC', 'CXX', 'CCFLAGS', 'CXXFLAGS', 'CFLAGS', 'LINKFLAGS']}

env = Environment(
    ENV={
        'PATH': os.environ['PATH'],
    },
    **passthrough_opts,
)
env.Append(CPPPATH=['.', 'spdlog/include'], CPPDEFINES=['CACHE_LINE_SIZE=64', 'SPDLOG_COMPILED_LIB'])

if env['CC'] == '':
    env.Tool('clang')
    env.Tool('clangxx')

def ccls_conf_action(target, source, env):
    import subprocess

    p = subprocess.run([env['CXX'], '-E', '-v', '-stdlib=libc++', '-xc++', '-'],
                       stdin=subprocess.PIPE, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    s = p.stderr.split(b'\n')

    ccls_inc_path = []

    started = False
    for l in s:
        if l.endswith(b'> search starts here:'):
            started = True
            continue
        if not started:
            continue
        if l.startswith(b' '):
            ccls_inc_path.append(b'-isystem' + l[1:])
        else:
            started = False

    with open('.ccls', 'wb') as f:
        f.write(b'\n'.join(b'clang++ -std=c++17 -I. -Ispdlog/include -DCACHE_LINE_SIZE=64 -DSPDLOG_COMPILED_LIB'.split(b' ') + ccls_inc_path))
        f.write(b'\n')

env.Append(BUILDERS={'LSPConfig': Builder(action=ccls_conf_action)})

compiler_path = ''
if not GetOption('clean'):
    compiler_exe_path = shutil.which(env['CC'])
    compiler_path = os.path.dirname(compiler_exe_path)
    print('sconstruct: found compiler at ' + compiler_path)
    env.LSPConfig('.ccls', compiler_exe_path)
    env.Alias('lsp', '.ccls')

env.Append(
    CCFLAGS=[
        '-pthread', '-Wstrict-aliasing', '-fno-omit-frame-pointer', '-mno-omit-leaf-frame-pointer',
    ],
    CXXFLAGS=[
        '-std=c++17', '-stdlib=libc++', '-Wno-vla-cxx-extension', '-Wno-unqualified-std-cast-call',
    ],
    LINKFLAGS=[
        '-fuse-ld=lld', '-nostdlib++', '-pthread', '-lrt', '-ldl',
        '-L' + compiler_path + '/../lib', '-Wl,-Bstatic', '-l:libc++.a', '-l:libc++abi.a', '-Wl,-Bdynamic',
    ],
)

release_env = env.Clone()
release_env.Append(
    CCFLAGS=[
        '-O3', '-ffast-math', '-march=native', '-flto=thin',
        '-fwhole-program-vtables', '-fvisibility=hidden', '-fvisibility-inlines-hidden',
        '-fforce-emit-vtables', '-fstrict-vtable-pointers',
    ],
    CPPDEFINES=['NDEBUG'],
    LINKFLAGS=[
        '-O3', '-ffast-math', '-flto=thin',
        '-fwhole-program-vtables', '-fvisibility=hidden', '-fvisibility-inlines-hidden',
        '-fforce-emit-vtables', '-fstrict-vtable-pointers', '-march=native',
    ],
)

debug_env = env.Clone()
debug_env.Append(
    CCFLAGS=[
        '-g', '-O0', '-fstandalone-debug', '-U_FORTIFY_SOURCE',
    ],
    LINKFLAGS=[
         '-g', '-O0', '-fstandalone-debug'
    ],
)

def asan_env_of(parent):
    asan_env = parent.Clone()
    asan_env.Append(
        CCFLAGS=[
            '-fsanitize=address', '-fsanitize-recover=address',
        ],
        LINKFLAGS=[
            '-fsanitize=address', '-fsanitize-recover=address',
        ]
    )
    return asan_env

build_configurations = {
    'debug': debug_env,
    'release': release_env,
    'asan_debug': asan_env_of(debug_env),
    'asan_release': asan_env_of(release_env),
}

build_conf = ARGUMENTS.get('BUILD_TYPE', 'release')
VariantDir(build_conf, '.', duplicate=False)

build = build_configurations[build_conf]
build_src = lambda s: build_conf + '/' + s

SetOption('num_jobs', os.cpu_count())
print('sconstruct: using %d number of parallel jobs' % GetOption('num_jobs'))

tpcc = build.StaticLibrary(target=build_conf + '/tpcc', source=[build_src(s) for s in tpcc_srcs])
ycsb = build.StaticLibrary(target=build_conf + '/ycsb', source=[build_src(s) for s in ycsb_srcs])

full_srcs = [build_src(s) for s in db_srcs + ['main.cc', 'module.cc']]
full_srcs += [
    build.Object(build_src(s), CXXFLAGS=['-include', 'masstree/build/config.h', '-Wno-deprecated-declarations'] + build['CXXFLAGS'])
    for s in masstree_srcs
]
full_srcs += env.Command('-Wl,--whole-archive', [], '') + [tpcc, ycsb] + env.Command('-Wl,--no-whole-archive', [], '')

build.Program(target=build_conf + '/db', source=full_srcs)

