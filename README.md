# ETL Spark Jobs

Spark Applications for ETLs

## Getting Started

### Pre-requisites

- Install IntelliJ Community Edition.
- You will need the Scala IntelliJ plugin.
- Install Java 1.8 installed.

### Setup

Open in IntelliJ:

- Ensure you have the correct version of Java (1.8).
- Ensure you have the correct version of Scala (2.11).
- Install Scala Plugin IntelliJ IDEA -> Preferences -> Plugins -> Marketplace -> Scala

### Setup local test data

There are 2 ways to do this. One is to run the jobs against mock data, the other is to start docker containers. Starting Docker containers will allow you to test Oracle write backs. Running the jobs locally, will set you up for local developement.

### Getting conformed data locally

Jobs location:

```bash
~/src/main/scala/com/bob/lee/sc/jobs/
```

The conformed data will be available at

```bash
~/src/test/resources/mockdata/conformed/
```

The mock data is located

```bash
~/src/test/resources/mockdata/landing/
```
