# -*- mode: python -*-
class Configuration(object):
    CFLAGS=['-pthread', '-Wstrict-aliasing', '-DCACHE_LINE_SIZE=64']
    LDFLAGS=[]
    
    def file(self, filename, extra_flags=[]):
        if filename.endswith('.cc'):
            return (filename, ['-std=c++17'] + self.CFLAGS + extra_flags)
        elif filename.endswith('.c'):
            return (filename, self.CFLAGS + extra_flags)
        elif filename.endswith('.S'):
            return (filename, [])
        else:
            return (filename, [])

    def files(self, filenames, extra_flags=[]):
        return map(lambda x: self.file(x, extra_flags=extra_flags), filenames)

    def build(self):
        includes = ['-I.', '-Ispdlog/include']
        cxx_library(
            name=self.PREFIX + '-tpcc',
            srcs=self.files([
                'benchmark/tpcc/tpcc.cc',
                'benchmark/tpcc/tpcc_workload.cc',
                'benchmark/tpcc/new_order.cc',
            ]),
            compiler_flags=includes,
            linker_flags=self.LDFLAGS,
            link_whole=True,
        )
        db_srcs = self.files([
            'epoch.cc', 'txn.cc', 'log.cc', 'vhandle.cc', 'gc.cc', 'index.cc', 'mem.cc',
            'promise.cc', 'masstree_index_impl.cc', 'node_config.cc', 'console.cc', 'console_client.cc',
            'felis_probes.cc',
            'json11/json11.cpp',
            'xxHash/xxhash.c',
            'gopp/gopp.cc', 'gopp/channels.cc',
            'gopp/asm.S']) + self.files([
                'masstree/kvthread.cc', 'masstree/string.cc', 'masstree/straccum.cc'
            ], extra_flags=['-include', 'masstree/build/config.h'])
        db_headers = [
            'console.h', 'felis_probes.h', 'epoch.h', 'gc.h', 'index.h', 'index_common.h',
            'log.h', 'mem.h', 'module.h', 'node_config.h', 'probe.h', 'promise.h', 'sqltypes.h',
            'txn.h', 'util.h', 'vhandle.h',
        ]
        libs = ['-pthread', '-lrt', '-ldl', '-lnuma', '-l:libjemalloc.a']
        test_srcs = self.files(['test/promise_test.cc'])
        cxx_binary(
            name=self.PREFIX + '-db',
            srcs=self.files(['main.cc', 'module.cc', 'iface.cc']) + db_srcs,
            headers=db_headers,
            compiler_flags=includes,
            linker_flags=self.LDFLAGS + libs,
            deps=[':%s-tpcc' % self.PREFIX],
        )
        cxx_test(
            name=self.PREFIX + '-test',
            srcs=test_srcs + db_srcs,
            headers=db_headers,
            compiler_flags=includes,
            linker_flags=self.LDFLAGS + libs + ['-lgtest_main', '-lgtest'],
            deps=[':%s-tpcc' % self.PREFIX]
        )

class DebugConfiguration(Configuration):
    # CFLAGS=['-g', '-fsanitize=address'] + Configuration.CFLAGS
    # LDFLAGS=['-fsanitize=address']
    CFLAGS=['-g'] + Configuration.CFLAGS
    PREFIX='debug'

class ReleaseConfiguration(Configuration):
    CFLAGS=['-Ofast', '-flto', '-fno-omit-frame-pointer', '-mno-omit-leaf-frame-pointer'] + Configuration.CFLAGS
    LDFLAGS=['-Ofast', '-flto', '-fwhole-program', '-fno-omit-frame-pointer', '-mno-omit-leaf-frame-pointer']
    PREFIX='release'

DebugConfiguration().build()
ReleaseConfiguration().build()
