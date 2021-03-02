# Async Client-Sever Downloader

This project creates creates a client-server downloading file system, which is based on the [Python asyncio](https://docs.python.org/3/library/asyncio.html) package, specifically with the cooperative multitasking style. All socket communication and concurrency was (mostly) done with async tasks instead of threads.

## Requirements

All of the modules utilized in this project, besides the matplotlib library for graphing, are part of the standard Python library. As a result of a lock of 3rd party libraries and Python being an interpreted language, there is a lack of a Makefile.

The main requirement is that the minimum required version is **Python 3.8**

## Running

The project comprises of three main components:

* `server.py`: creates a server
* `client.py`: creates clients
* `evaluation.py`: runs various server-client configurations and times performance

In order to run any of the above scripts run in the terminal the following command:

```bash
python3 {server.py | client.py | evaluation.py} [ARGS...]
```

Each of the scripts have their unique arguments which can be viewed by passing in the `-h` argument.

Both `server.py` and `client.py` can both take in a configuration file instead of just command line arguments. The expected file format is an [INI file](https://en.wikipedia.org/wiki/INI_file). View the sample configuration files in `configs` folder. A mix of command line arguments and configuration file can also be applied, with the configuration file overriding any duplicate args:

```bash
python3 {server.py | client.py} -c CONFIG_FILE [ARGS...]
```

### Server/Client

Running the script `server.py` and `client.py` are the actual server-client program, and they work in conjunction with each other. In order to run:

1. Run the `server.py` script with some specified arguments
2. Wait for the server to initialize
3. Run `client.py` any amount of times, making sure that args like `address` reference the location of the server, in order for the client to connect
4. Wait for the client to initialize, start typing and interacting with either of the server or client clis.

### Evaluation

The `evaluation.py` script automates much of the initialization listed above in order, with an assumption of the below directory structure. The main purpose is to time the performance of the client-server communication in different configuration contexts. The process to running the evaluations is simply running the script, but one recommendation is to file redirect the evaluations to avoid the shell being shown for each client:

```bash
python3 evaluation.py [ARGS...] > REDIRECT
```

#### Directory Structure

Multiple extra directories were introduced for organization sake. All of the folders below are specified to be run with `evaluation.py`:

1. **configs**: some predefined configuration files for testing and evaluation
2. **logs**: the output files while running either `server.py` or `client.py`
    * the naming scheme of each of the log files indicates the configuration is ran with the convention:

        ```
        {CLIENT_ID | SERVER_ID}-{NUMBER_OF_CLIENTS}c{FILE_SIZE}f.log
        ```

3. **servers**: files that can be hosted and distributed by the servers

    * each of the server folders are divided by file size

4. **times**: raw and aggregate recordings of server/client configuration runtimes
