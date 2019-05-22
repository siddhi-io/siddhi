# Siddhi 5.x User Guide

This section provides information on developing and running Siddhi.

## Siddhi Application

A self contained stream processing logic can be written as a Siddhi Application and put together in a single file with `.siddhi` extension. The stream processing constructs, such as streams and queries, defined within a Siddhi App is not visible even to the other Siddhi Apps running in the same JVM.

It is recommended to have different business usecase in separate Siddhi Applications, where it allow users to selectively deploy the applications based on business needs.
It is also recommended to move the repeated steam processing logic that exist in multiple Siddhi Applications, such as message retrieval and preprocessing, to a common Siddhi Application, whereby reducing code duplication and improving maintainability.
In this case, to pass the events from one Siddhi App to another, configure them using a common topic using [In-Memory Sink](http://siddhi-io.github.io/siddhi/api/latest/#inmemory-sink) and [In-Memory Source](http://siddhi-io.github.io/siddhi/api/latest/#inmemory-source).

For writing Siddhi Application using Streaming SQL refer [Siddhi Query Guide](../query-guide-5.x/)

## Execution Environments

Siddhi can run in multiple environments as follows.

* [As a Java Library](../siddhi-as-a-java-library-5.x/)
* [As a Local Microservice](../siddhi-as-a-local-microservice-5.x/)
* [As a Docker Microservice](../siddhi-as-a-docker-microservice-5.x/)
* [As a Kubernetes Microservice](../siddhi-as-a-kubernetes-microservice-5.x/)
* As a Python Library _(WIP)_

## System Requirements

For all execution modes following are the general system requirements.

1. **Memory**   - 128 MB (minimum), 500 MB (recommended), higher memory might be needed based on in-memory data stored for processing
2. **Cores**    - 2 cores (recommended), use lower number of cores after testing Siddhi Apps for performance
3. **JDK**      - 8 or 11
4. To build Siddhi from the Source distribution, it is necessary that you have JDK version 8 or 11 and Maven 3.0.4 or later
