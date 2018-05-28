[![DOI](https://zenodo.org/badge/134968216.svg)](https://zenodo.org/badge/latestdoi/134968216)

# Cascade Depth Prediction

## Synopsis
This code base allows to on train a model which predicts to what extent content will diffuse across a given network. 

## Code Example


## Motivation


## Installation
This codebase can be run on an individual machine or a Hadoop cluster. 

To install the necessary code on your local machine use pip:
```pip install -r requirements.txt```

This will install all the local dependencies. 

### Running Code Locally

To run code local one has to simply execute one of the jobs on the local machine with the correct settings. 

```python DiffusionSimulation.py -v ./data/loop.txt --network ./data/go-pickle.gpickle -o ./output/geo-sim/ --no-output```

### Running Code on a Custer
To distribute the execution of the code this framework uses MRJob. This manages both code distribution, remote dependency management and code execution across a Hadoop cluster. 

All this is managed at the point of execution of a job. Thus to run a diffusion simulation job, run the following command:

```python DiffusionSimulation.py -v ./data/loop.txt --network ./data/go-pickle.gpickle -o ./output/geo-sim/ --no-output --conf-path ./etc/mrjob.conf ```

To configure setting for the cluster please edit the file:

```ect\mrjob.conf```

You can find more details about MRJob settings at: https://pythonhosted.org/mrjob/guides/configs-basics.html 

## Contributors

Daniel Kershaw - Lancaster University

## References
