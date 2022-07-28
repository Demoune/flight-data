# flight-data

## Overview
+ This is a simple scala/spark project providing functionality to generate 4 reports on flights and passengers data
+ Flights and passengers data is provided in the defined format in csv
+ This project allows to generate report files on your local machine and doesn't require using remote hadoop/spark cluster
+ This project allows to run the same code on spark cluster, but spark-submit scripts are out of scope

## Project setup
+ Clone the repository from https://github.com/Demoune/flight-data
+ git clone git@github.com:Demoune/flight-data.git

## Tools and pre-requisites
### IntelliJ Idea
+ Current project is created using IntelliJ Idea and includes corresponding configuration and modules definitions
+ Community edition is enough to work on this project and run all the tests / produce the result
+ You need to have "Scala" plugin installed in your IntelliJ Idea

### Local Hadoop setup
+ To be able to run spark tests on your machine in local mode - you need to have Hadoop and WinUtils (for Windows) installed.
+ Please follow instructions in the section "Install Hadoop on Windows" below

### JDK
+ To be able to run spark tests for spark 3.2+ - you need one of the following JDK versions - 1.8/11/17.
+ JDK 18 is not supported by Spark.
+ If you already have spark version prior to 3.2 - you can't use JDK 17 and limited to only versions 1.8 and 11.

### sbt
+ The current project uses sbt as a build tool
+ It's supported by IntelliJ out of the box and doesn't require any additional configuration given
+ If you don't have direct connectivity to sbt repositories - please specify http proxy configuration adding these Java options:
+ -Dhttp.proxyHost=<yourserver> -Dhttp.proxyPort=<proxy_port> -Dhttp.proxyUser=<username> -Dhttp.proxyPassword=<password>

## Generating results
+ To generate result files on your local machine in one go - please run ExerciseFlowTest
+ The result csv files are going to be generaed in your project folder "flight-data" under "output" in 4 corresponding folders:
1) monthly-flights
2) top-100-flyers
3) longest-run-without-uk
4) flown-together-min3
+ You will find a single csv file with random name with all the results in every folder
+  Renaming csv files and removing .crc and _SUCCESS files is out of scope for this exercise
+ Building fat jar for spark-submit is out of scope

## Additional instructions
### Install Hadoop on Windows
You will need local hadoop setup to be able to run spark code on your windows machine.
Follow the instructions below to get it setup.
+ Download hadoop distributive from here:
  https://www.apache.org/dyn/closer.cgi/hadoop/common/hadoop-3.3.3/hadoop-3.3.3.tar.gz
+ Extract into your libraries folder, i.e. C:\app
+ Copy your machine ssh key to your github account
+ If you don't have it - Generate a new ssh key pair on your machine:
  1) Run Command prompt as Administrator
  2) Type and run command "ssh-keygen"
  3) Use default key names
  4) Find your key pair generated under C:\Users\your_username/.ssh
+ Copy content of generated public key file in your ~/.ssh folder to ssh key section under your account on github
+ Clone repository with hadoop winutils for windows 
git clone git@github.com:kontext-tech/winutils.git
+ Copy all files from hadoop-3.3.1/bin subfoder into previously extracted hadoop distributive, i.e. C:\app\hadoop-3.3.3\bin
+ Add new environment variable in your System properties:
 Key: HADOOP_HOME
 Value: C:\app\hadoop-3.3.3\



