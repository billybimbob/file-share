# Async Client-Sever Downloader

This project creates creates a client-server downloading file system, which is heavily built upon the Python asyncio module, specifically with the cooperative multitasking paradigm. All socket communication as well as concurrency was (mostly) done with async tasks instead of threads.

## Running

The project comprises of three main components:
    *`server.py`: creates a server
    *`client.py`: creates clients
    *`evaluation.py`: runs various server-client configurations and times performance

In order to run any of the above scripts run in the terminal the following command:

`python3 {server.py | client.py | evaluation.py} [--args...]`

Each of the scripts have their unique arguments which can be viewed by passing in the `-h` argument.

Running the script `server.py` and `client.py` are the actual server-client program, and they work in conjunction with each other. In order to run:
    1. Run the `server.py` script with some specified arguments
    2. Wait for the server to initialize
    3. Run `client.py` any amount of times, making sure that args like `address` are in sync with the args for the server, in order for the client to connect
    4. Wait for the client to initialize, start typing and interacting with either of the server or client clis.

The other script `evaluation.py` automates much of the initialization listed above in order to time the performance of the client-server communication in different configuration contexts. The process to running the evaluations is simply running the script, but one recommendation is to file redirection the evaluations to avoid the shell cli being shown:

`python3 evaluation.py [--args..] > *some-redirect*`

## Requirements

All of the modules utilized in this project, besides the matplotlib library for graphing, are part of the standard Python library.

The main requirement is that the required Python version is >=3.8

## Directory Structure

## Server Deployment
