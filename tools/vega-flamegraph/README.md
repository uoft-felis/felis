# Generating Flamegraph from `perf`

This tool can generate and share flamegraph using the performance sample taken from `perf`.

## Using `perf`

To use this tool, you need to record a performance trace with full stacktrace. For example:

```bash
perf record -g -p <process_pid>
```

For Felis, you should avoid the experiment loading phase. After Felis successfully loads the experiment, it will wait for a start signal from the controller. This is when you can start `perf record`.

```bash
perf record -g -p `cat /tmp/felis-<username>-host1.pid`
```

Remember to replace `<username>` with your own username. If you are running the distributed experiment, you need to replace `host1` with your host identifier too.

By default `perf record` saves the sample in `perf.data` file under the current working directory. If you want to use a different output name, you can add `-o <filename>` to perf.

## Generating Flamegraph JSON

We have a program that transforms perf's output to the flamegraph JSON. It's written in C++ so you have to compile it first.

```bash
clang++ -std=c++17 -O3 script.cc -o script
```

Then, you can:

``` shell
perf script | tools/vega-flamegraph/script > flamegraph.json
```

If you want to use a custom file name, you can

```bash
perf script -i <filename> | tools/vega-flamegraph/script > flamegraph.json
```

## Deploy and See Your Flamegraph

You need a static web server to view the flamegraph. On the CSL Cluster, the web server serves your `~/public_html` directory. You can soft-link or copy `flamegraph.html`, `flamegraph.vg.json` and `flamegraph.json` into any directory that's under `~/public_html`. Unfortunately, the data filename has to be `flamegraph.json` currently. After this, you can see your flamegraph at `http://fs.csl.utoronto.ca/~<username>/<path_of_flamegraph.html>`

For example, you can see Mike's flamegraph [here](http://fs.csl.utoronto.ca/~mike/flamegraph/flamegraph.html).