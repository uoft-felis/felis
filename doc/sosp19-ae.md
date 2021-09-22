# SOSP 19 AE Instructions

## Logging In the CSL Cluster

Our experiments are performed inside our own cluster. To login, you can use the private key we sent on hotcrp to `ssh evaluator@fs.csl.utoronto.ca`. Caracal is evaluated on machine `c153`, so login to `c153` with the same user from `fs.csl.utoronto.ca`.

## Clone the Source Code

You need to clone two repositories under the `workspace/` directory:

```
mkdir ~/workspace/
cd ~/workspace/
git clone --recursive https://github.com:uoft-felis/felis.git
git clone https://github.com:uoft-felis/felis-controller.git
```

Felis is the code name for our paper, Caracal. The felis repository has many submodules, so please make sure they are also initialized. Our experiment scripts look for binaries under the `workspace/` directory.

## Build the Source Code

To build felis on the CSL cluster, use the following commands:

```
cd ~/workspace/felis/
./configure
buck db_release
```

To build felis-controller on the CSL cluster, use the following commands:

```
cd ~/workspace/felis-controller/
mill FelisController.assembly
mill FelisExperiments.assembly
```

If mill complains about Java, make sure your `JAVA_HOME` is `/pkg/java/j9/`.

## Running Single-Node Experiments

The Caracal paper consists two set of experiments: single-node and multi-node. The single-node experiment is the focus of the paper because that validates our contention management technique. This experiment can be performed on `c153`. However, both these experiments need to run a felis-controller instance.

First, we need to create a config file for felis-controller.

```
cd ~/workspace/felis-controller/
vim config.json.local
```

My `config.json.local` looks like the following

```json
{
  "type": "status_change",
  "status": "configuring",
  "mem": {},
  "nodes": [
    {
      "name": "host1",
      "worker": {"host": "127.0.0.1", "port": 11091},
      "index_shipper": {"host": "127.0.0.1", "port": 53411}
    }
  ],
  "tpcc": {
    "hot_warehouses": [],
    "offload_nodes": []
  },
  "controller": {
    "http_port": 8666,
    "rpc_port": 3148
  }
}
```

Then, fire off felis-controller.

```
java -jar out/FelisController/assembly/dest/out.jar config.json.local
```

Now, you can run all single node experiments:

```
java -jar out/FelisExperiments/assembly/dest/out.jar runXXX
```

Where `runXXX` is one of ` runEpochSizeTuning`, `runTestOpt`, `runTpccSingle`, `runTuning`, `runYcsb`.

## Plot the Graph

You can use the same jar to plot the graph.

```
java -jar out/FelisExperiments/assembly/dest/out.jar plotXXX
```

Where `plotXXX` is one of `plotEpochSizeTuning`, `plotTestOpt`, `plotTpccSingle`,  `plotTpccTuning`,  `plotYcsb`,  `plotYcsbTuning`.

The output is generated under `static/`. You may copy to the directory to `~/public_html`, which will be available for browsing under `http://fs.csl.utoronto.ca/~evaluator/graph.html?showTitle`. For example: `http://fs.csl.utoronto.ca/~mike/graph.html?showTitle` is the graph I generated.

## Running Multi-Node Experiments

The last distributed transaction experiment is not fully automated yet. So we need to run the experiment manually. First, we need to find a central controller node. I recommend running this on `fs.csl.utoronto.ca`. We need to prepare a config.json.multi for felis-controller. Mine looks like the following:

```
{
  "type": "status_change",
  "status": "configuring",
  "mem": {
  },
  "nodes": [
    {
      "name": "host1",
      "ssh_hostname": "c186",
      "worker": {"host": "142.150.234.186", "port": 1091},
      "index_shipper": {"host": "142.150.234.186", "port": 43411}
    },
    {
      "name": "host2",
      "ssh_hostname": "c154",
      "worker": {"host": "142.150.234.154", "port": 1091},
      "index_shipper": {"host": "142.150.234.154", "port": 43411}
    },
    {
      "name": "host3",
      "ssh_hostname": "c165",
      "worker": {"host": "142.150.234.165", "port": 1091},
      "index_shipper": {"host": "142.150.234.165", "port": 43411}
    },
    {
      "name": "host4",
      "ssh_hostname": "c172",
      "worker": {"host": "142.150.234.172", "port": 1091},
      "index_shipper": {"host": "142.150.234.172", "port": 43411}
    }
  ],
  "tpcc": {
    "hot_warehouses": [],
    "offload_nodes": []
  },
  "controller": {
    "http_port": 8666,
    "rpc_port": 3148
  }
}
```

This configuration file declares 4 hosts. `c169`, `c154`, `c165` and `c172`. Then, login to these machines and run

```
~/workspace/felis/buck-out/gen/db#release -c 142.150.234.2:3148 -n host1 -w tpcc -XMaxNodeLimit2 -XTpccWarehouses32 -Xcpu16 -Xmem14g
```

The hostname `-n host1` needs to match with the configuration file. For example, `c169` should use `-n host1`, but `c154` should use `-n host2`. `-XMaxNodeLimit` can limit how many nodes to use for a run.

After all these database instances boot up. We can issue a go signal to the controller.

```
curl -vvv 142.150.234.2:8666/broadcast/ -d '{"type": "status_change", "status": "connecting"}'
```

We are also working on automating this process so that you only need to configure felis-controller without spawning these instances manually.

## Contact

Contact mike@eecg.toronto.edu if you have any question. He is also on telegram [@mikeandmore](https://t.me/mikeandmore) and twitter [@__broken_mike](https://twitter.com/__broken_mike).