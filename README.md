Build
-----

If you on the CSL cluster, you don't need to install any
dependencies. Otherwise, you need to install Clang 8 manually.

1. If you have not runned before, you can run the configure script

```
./configure
```

This script will check for `buck` build tool and download if
necessary.

3. Now you can build

```
buck build db
```

This will generate a binary to `buck-out/gen/db#debug`. By default,
the build is debug build. If you need optimized build you can run.

```
buck build db_release
```

This will generate the release binary to `buck-out/gen/db#release`

If you are not on the CSL cluster, you need to install buck or use the
`./buck.pex` that the configure script has downloaded.

Test
----

FIXME: Unit tests are broken now. You may skip this section.

Use

```
./buck build test
```

to build the test binary. Then run the `buck-out/gen/dbtest` to run
all unit tests. We use google-test. Run run partial test, please look
at
https://github.com/google/googletest/blob/master/googletest/docs/advanced.md#running-a-subset-of-the-tests
.


Logs
----

If you are running the debug version, the logging level is "debug" by
default, otherwise, the logging level is "info". You can always tune
the debugging level by setting the `LOGGER` environmental
variable. Possible values for `LOGGER` are: `trace`, `debug`, `info`,
`warning`, `error`, `critical`, `off`.

The debug level will output to a log file named `dbg-hostname.log`
where hostname is your node name. This is to prevent debugging log
flooding your screen.

Run
---

Setting Things Up
=================

First, you need to run the `felis-controller`. Please refer to the
README file there.

Second, Felis need to use HugePages for memory allocation (to reduce
the TLB misses). Common CSL cluster machines should have these already
setup, and you may skip this step. The following pre-allocates 400GB
of HugePages. You can adjust the amount depending on your memory
size. (Each HuagePage is 2MB by default in Linux.)

```
echo 204800 > /proc/sys/vm/nr_hugepages
```

Run The Workload
================

Nodes need to be specified first. They are speicified in `config.json`
on the felis-controller side. Once they are defined, you can run:

```
buck-out/gen/db#release -c 127.0.0.1:<rpc_port> -n host1 -w tpcc -Xcpu16 -Xmem20G -XVHandleBatchAppend -XVHandleParallel
```

`-c` is the felis-controller IP address, `-n` is the host name for
this node, and `-w` means run `tpcc` workload. With the default
configuration, you are able to run two nodes: `host1` and
`host2`. `-X` are for the extended arguments. For a list of `-X`,
please refer to `opts.h`. Mostly you will need `-Xcpu` and `-Xmem` to
specifies how many cores and how much memory to use. (Currently,
number of CPU must be multiple of 8. That's bug, but we don't have
time to fix it though.)

The node will initialize workload dataset and once they are idle, they
are waiting for further commands from the controller. When all of them
finishes initialization, you can tell the controller that everybody
can proceed:

```
curl localhost:8666/broadcast/ -d '{"type": "status_change", "status": "connecting"}'
```

This would broadcast to every node to start running the
benchmark. When it all finishes, you can also use the following
commands to safely shutdown.

```
curl localhost:8666/broadcast/ -d '{"type": "status_change", "status": "exiting"}'
```

Development Setup
=================

We use `ccls` <https://github.com/MaskRay/ccls> for development. If
you have run the `./configure` script, it would generate a `.ccls`
configuration file for you. `ccls` supports
[Emacs](https://github.com/MaskRay/ccls/wiki/lsp-mode),
[Vim](https://github.com/MaskRay/ccls/wiki/vim-lsp) and
[VSCode](https://github.com/MaskRay/ccls/wiki/Visual-Studio-Code).

Mike has a precompiled `ccls` binary on the cluster machine. You can
download at <http://fs.csl.utoronto.ca/~mike/ccls>.

Zhiqi has some experience with using ccls with VSCode.
