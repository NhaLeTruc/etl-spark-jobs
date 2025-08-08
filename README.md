# KYC-SC-Spark
<hr />
KYC Simple Companies Spark Application

## Getting Set Up
This repo includes a Git commit message hook to enforce each Git commit message to include a JIRA issue ID. this is to ensure that changes in the repo are linked to JIRA issues (the link will then appear inside the JIRA issue).

To install the Git hook run the following in the root folder of this repo:

```bash
make init
```

For Mac users, installing XCode will give you the make command. If you don't have make you can run the following to install the Git hook from the root folder of this repo:

```bash
./hooks/install-githooks.sh
```

# Set up your local environment.
<hr />

## Pre-requisites

 - Install IntelliJ Community Edition (You can install this via Mac's Self Service)
   - You will need the Scala IntelliJ plugin.
 - Install Java 1.8 installed. (You can install this via Mac's Self Service)
 - Request Artifactory access to: (Gaining access taks 20/30 min.)

```yaml
Repository name: `FCMP-SBT`
Access role: `BUILDUSER`
```

## Setup

1. Clone the repo:
   - If you get teh error "Permission denied ...", you'll need to set up SSH keys. follow:
   <https://confluence.dss.ext.national.com.au/display/BOPD/How+to+enable+SSH+GIT+access>
   ```bash
   git clone git@github.aus.thenational.com:odyssey/kyc-sc-spark.git 
   ```
2. Calypso set up. Once you have artifactory access to FCMP-SBT:
   - You can follow the instructions here: <https://confluence.dss.ext.national.com.au/display/SSS/How-to%3A+Include+Calypso+in+your+project> or follow below:
     - Got to the Artifactory repo -> Artifacts.
     - Click "Set me Up"
     - Enter your Credentials Simultaneously, open ~/.bash_profile (or ~/.zsh if you're using zsh)
     - And the following environment variables to your shell config.
     - Copy and paste the username and password from the Artifactory "Set Me Up" screen.
     ```bash
     export ARTIFACTORY_CREDS_USR="<USERNAME>"
     export ARTIFACTORY_CREDS_PSW="<PASSWORD>"
     ```
     - Run source ~/.bash_profile to load the environment variables.
     - For more information on how to use Calypso: <https://confluence.dss.ext.national.com.au/display/SSS/How-to%3A+Use+Calypso+in+your+project+and+create+a+job>
3. Open in IntelliJ:
   - Ensure you have the correct version of Java (1.8).
   - Ensure you have the correct version of Scala (2.11).
   - Install Scala Plugin IntelliJ IDEA -> Preferences -> Plugins -> Marketplace -> Scala
   - Copy and paste the Artifactory usr/psw (from previous step) into build.sbt:
    ```sbt
    sys.env.get("ARTIFACTORY_CREDS_USR").getOrElse("pXXXXX"),
    sys.env.get("ARTIFACTORY_CREDS_PSW").getOrElse("XXXXX")
    ```
   - Load SBT changes by clicking on one of the refresh buttons shown in the screenshot below.
   - Remove the credentials you entered in build.sbt

> The reason for pasting the credentials into the file is because IntelliJ's UI does not read the shell's environement variables.

4. To ensure that you're correctly set up: Ensure that your external libraries have populated.

# Setup local test data
<hr />

There are 2 ways to do this. One is to run the jobs against mock data, the other is to start docker containers. Starting Docker containers will allow you to test Oracle write backs. Running the jobs locally, will set you up for local developement.

VM options
```bash
-Dspark.master=local[*]
-Dother-config.oracle-data-store-config.db-url="jdbc:oracle:thin:FCSRAR/FCSRAR#123@//localhost:11521/roadie"
-Dother-config.oracle-data-store-config.db-user="FCSRAR"
-Dother-config.oracle-data-store-config.db-password="FCSRAR#123"
-Dother-config.data-comparison-environment.env-name=bda
```
Note: If you want to load mockdata into ppte from locally stored parquet files then just replace the db.url with PPTE URL - "jdbc:oracle:thin@//dpxdt02a-scan.austest.thenational.com:1621/PPTEFCST_OLT" and correspondingly provide PPTE user and password.

Program Arguments
```bash
--run-date 2020-09-25 --config-file resource://restore-landing-data.conf
```

## Getting conformed data locally.
Jobs location:
```bash
~/src/main/scala/au/com/nab/rna/sc/jobs/
```

The conformed data will be available at:
```bash
~/src/test/resources/mockdata/conformed/
```

The mock data is located:
```bash
~/src/test/resources/mockdata/landing/
```

Run job(s)
Note: Run scripts have been configured in IntelliJ, so you shouldn't need to change anything.
1. Locate the job you wish to run.
2. Click on the green arrow next to the job name nad Select Run from the dropdown.
   - While the job is running, there will be several warning which can be ignored.
3. In case of an error, check the configuration of each job, you may need to change the run config:
   - You should see a page like this.
4. Make sure these values are filled out for each individual job:
   - VM options:
   - You can leave the password and url values empty for local testing. If you need to connect to a Database, provide those details. There is a local DB created via Docker that you can use in <https://github.aus.thenational.com/odyssey/kyc-sc-dags>
   ```bash
   -Dspark.master=local
   -Dother-config.oracle-data-store-config.db-user=FCSRAA
   -Dother-config.oracle-data-store-config.db-password='' 
   -Dother-config.oracle-data-store-config.db-url='' 
   ```
   - Program arguments: please note, each job has a different config file.
   ```bash
   --config-file resource://sc-bix-job.conf --run-date 2020-09-25 
   ```
   - Environment variable:
   ```bash
   ENVIRONMENT_PREFIX_DATABASE=;ENVIRONMENT_PREFIX_CONFORMED=;ENVIRONMENT_PREFIX_APPLICATION=;ENVIRONMENT_BASE_PATH=$PROJECT_DIR$/src/test/resources/mockdata 
   ```
   
## Running docker containers
To run the docker containers and test writing to oracle, Clone the following repo and follow the instructions in
<https://github.aus.thenational.com/odyssey/kyc-sc-dags/README.md>

# Running Data Comparison
<hr />
your snapshot baseline is located at src/test/resources/mockdata/conformed.

prerequisites
1. Modify the 'Environment variable' configuration for ScBixReporting: ENVIRONMENT_PREFIX_CONFORMED=/test