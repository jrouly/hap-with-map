#!/bin/bash

set -e

# DOWNLOAD MAHOUT
echo "cd ~"
cd ~
echo "wget https://s3.amazonaws.com/jrouly-test/lib/mahout-distribution-0.6.tar.gz"
wget https://s3.amazonaws.com/jrouly-test/lib/mahout-distribution-0.6.tar.gz
echo "gunzip mahout-distribution-0.6.tar.gz"
gunzip mahout-distribution-0.6.tar.gz
echo "tar xvf mahout-distribution-0.6.tar"
tar xvf mahout-distribution-0.6.tar

# INSTALL MAHOUT SYMLINK
echo "cd /usr/lib"
cd /usr/lib
echo "sudo ln -s /home/hadoop/mahout-distribution-0.6 mahout"
sudo ln -s /home/hadoop/mahout-distribution-0.6 mahout
echo "cd ~"
cd ~

# INSTALL MAHOUT LIBRARY ON PATH
echo "export PATH=$PATH:/usr/lib/mahout/bin"
export PATH=$PATH:/usr/lib/mahout/bin

# INSTALL MAHOUT JARS ON HADOOP_CLASSPATH
echo "export from /usr/lib/mahout/*.jar"
for lib in /usr/lib/mahout/*.jar
do
  export MAHOUT_CLASSPATH=$lib:$MAHOUT_CLASSPATH
done

echo "export from /usr/lib/mahout/lib/*.jar"
for lib in /usr/lib/mahout/lib/*.jar
do
  export MAHOUT_CLASSPATH=$lib:$MAHOUT_CLASSPATH
done

echo "export HADOOP_CLASSPATH=$MAHOUT_CLASSPATH:$HADOOP_CLASSPATH"
export HADOOP_CLASSPATH=$MAHOUT_CLASSPATH:$HADOOP_CLASSPATH

echo "echo "export PATH=$PATH:/usr/lib/mahout/bin" >> ~/.bashrc"
echo "export PATH=$PATH:/usr/lib/mahout/bin" >> ~/.bashrc
