# flight-data

## Project setup
Clone the repository from https://github.com/Demoune/flight-data

## Install Hadoop on Windows
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



