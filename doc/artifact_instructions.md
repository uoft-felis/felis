# EuroSys 23 AE Instructions


## Clone the Source Code

You need to clone two repositories to your local directory:

```
git clone --recursive https://github.com:uoft-felis/felis.git -b pmem
git clone https://github.com:uoft-felis/felis-controller.git
```

Felis is the code name for Caracal. The felis repository has many submodules, so please make sure they are also initialized.

## Installing Dependencies

Install clang 8
```
sudo apt install clang
```

Install mill
```
wget https://github.com/lihaoyi/mill/releases/download/0.3.5/0.3.5 -O mill
chmod +x mill
sudo mv mill /usr/local/bin/
```

Install the following packages if you don't already have them
```
sudo apt install default-jre
sudo apt install python2
sudo apt-get install libc++-dev
sudo apt install gcc
sudo apt install g++
Sudo apt install lld
sudo apt-get install libc++abi-dev
```

## Build the Source Code

Download the build tool `buck` by running the configure script

```
cd ~/your-directory/felis/
./configure
./buck.pex build db_release
```

To build felis-controller, use the following commands:

```
cd ~/your-directory/felis-controller/
mill FelisController.assembly
```

## Setting Things Up
Caracal needs to use HugePages for memory allocation. The following pre-allocates 64GB of HugePages. You can adjust the amount depending on your memory size. (Each HugePage is 2MB by default in Linux.)

```
echo 32768 > /proc/sys/vm/nr_hugepages
```

Our pmem mount directory is `/mnt/pmem0`. If you use a different path, you need to specify it in mem.cc lines 1185 and 1203.

## Running Experiments

To run the experiments, you need to also run a felis-controller instance. It can be run either on the same machine where you'll run the experiments or on another machine. Please check the README in felis-controller for more details.

First, we need to create a config file for felis-controller.

```
cd ~/your-directory/felis-controller/
vim config.json
```

My `config.json` looks like the following

```json
{
  "type": "status_change",
  "status": "configuring",
  "mem": {
  },
  "nodes": [
    {
      "name": "host1",
      "ssh_hostname": "pm",
      "worker": {"host": "127.0.0.1", "port": 1091},
      "index_shipper": {"host": "127.0.0.1", "port": 43411}
    }
  ],
  "controller": {
    "http_port": 8666,
    "rpc_port": 3148
  }
}
```

Then, run felis-controller.

```
java -jar out/FelisController/assembly/dest/out.jar config.json
```

As long as the configuration doesn't change, you can let the controller
run all the time.


Once the controller is initialized, you can run the experiments:

```
buck-out/gen/db#release -c <controller_ip>:<rpc_port> -n host1 -w tpcc -Xcpu8 -Xmem12G -XLogInput
e.g. buck-out/gen/db#release -c 127.0.0.1:3148 -n host1 -w tpcc -Xcpu8 -Xmem12G -XLogInput
```

`-c` is the felis-controller IP address (<rpc_port> and <http_port>
below are specified in config.json as well), `-n` is the host name for
this node, and `-w` means the workload it will run (tpcc/ycsb/smallbank).

For high-contention TPCC, add the flag -XTpccWarehouses1

For high-contention YCSB, add the flag -XYcsbContentionKey7

For high-contention smallbank, set the hotspot_number to 10000 in ~/your-directory/felis/benchmark/smallbank/smallbank.cc line 20 and rebuild the database.


## Contact

Contact shirley.wang@mail.utoronto.ca if you have any question.