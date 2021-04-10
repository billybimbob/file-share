# Async Chunked Peer-to-Peer Downloader

This project creates creates a peer-to-peer downloading file system, which is based on the [Python asyncio](https://docs.python.org/3/library/asyncio.html) package, specifically with the cooperative multitasking style. All socket communication and concurrency was (mostly) done with async tasks instead of threads.

## Requirements

The required minimum version is **Python 3.9**

All of the modules utilized in this project, besides the matplotlib library for graphing, are part of the standard Python library. As a result from the lack of 3rd party libraries, as well as Python being an interpreted language, there is no Makefile.

### Docker

All the requirements, including the graphing capabilities can be initialized in a docker container:

1. Create the docker image:

    ```bash
    docker build -t cp2p .
    ```

2. Create a new container and run a new shell:

    ```bash
    docker run -it --rm cp2p /bin/bash
    ```

3. All commands specified [below](#running) can be called in the container shell

## Running

The project comprises of three main components:

* `indexer.py`: creates a indexing server
* `peer.py`: creates a peer server/client
* `evaluation.py`: runs various strong/weak peer configurations and times the performance

In order to run any of the above scripts run in the terminal the following command:

```bash
./{indexer.py | peer.py | scripts/evaluation.py} [ARGS...]
```

Each of the scripts have their unique arguments which can be viewed by passing in the `-h` argument.

Both `indexer.py` and `peer.py` can both take in a configuration file instead of just command line arguments. The expected file format is an [INI file](https://en.wikipedia.org/wiki/INI_file). View the sample configuration files in `configs` folder. A mix of command line arguments and configuration file can also be applied, with the configuration file overriding any duplicate args:

```bash
./{indexer.py | peer.py} -c CONFIG_FILE [ARGS...]
```

### Indexer/Peer

Running the script `indexer.py` and `peer.py` are the actual peer-to-peer programs, and they work in conjunction with each other. This form of running has quite a bit of steps, so it is not recommended to run this way. Create quite demo runs are recommended to use the interactive mode for the evaluation script, specified in the [Evaluation section](#evaluation).

If this manual form wants to be used:

1. Run the `indexer.py` script with some specified arguments or a given config file
2. Wait for the indexer to initialize
3. Run `peer.py` any amount of times, making sure that args like `address` reference the correct indexer, and that the `port` number is unique (if multiple peers are running on the same machine)
4. Wait for the peer to initialize, start typing and interacting with either of the indexer or peer clis

A peer has its file directory specified, but the exposed directory cannot show nested directories. Also make sure that peer directories are not the same as other peers on the same machine when specifying args.

### Evaluation

The `evaluation.py` script automates much of the initialization steps listed above in the [Indexer/Peer section](#indexerpeer), with an assumption certain config files existing in the `configs` folder. When ran, the folder `servers` and `peers` is created and populated, which are the source peer files, and peer directories.

The main purpose of this script is to time the performance of the peer-peer and peer-indexer communication in different configuration contexts. The process to running the evaluations is simply running the script as specified above while supplying the args.

This script also can be used as an entry point to run intractable runs of the peer-to-peer system, and is the recommended way to run the system. To run the intractable mode, pass the `-i` argument, which will automatically go through the initialization steps, and also create another peer endpoint to view the system and files.

#### Log Generation

For convenience, all of the generated log files can be retested by using the bash script `eval-loop.sh`, which runs evaluations on multiple run configurations:

```bash
./scripts/eval-loop.sh
```

#### Log Folder Naming Convention

The naming scheme of each of the log folders in each evaluation folder indicates configuration the log was ran with:

```bash
{NUMBER_OF_PEERS}p{SIZE_OF_FILES}f
```

### Visualization/Graphing

While not required to run the server and clients, another script, `plot_times.py` generates graphs based on the generated log files. Running the script is in similar fashion to the above programs:

```bash
./scripts/plot_times.py [ARGS...]
```

All of the parsed and generated output are stored in the `times` directory

#### Install dependencies

Note: this dependency is only required for optional script `plot_times.py`:

```bash
pip3 install matplotlib
```

The dependencies are already installed if ran in the docker container.
