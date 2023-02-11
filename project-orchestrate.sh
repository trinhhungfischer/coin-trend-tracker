#!/bin/sh

######################################################################
# title: project-orchestrate.sh
# author: Trinh Hung
# date: 2022-12-11
# url: httpS://github.com/trinhhungfischer
# description: This script is used to orchestrate the project
######################################################################

# Create cassandra schema
docker exec cassandra-coin-trendy cqlsh -u cassandra -p cassandra -f /cassandra/createTweetsTable.cql

# Create kafka topic
docker exec kafka-coin-trendy kafka-topics --create --topic twitter-data-event --zookeeper zookeeper-coin-trendy:2181 --replication-factor 1 --partitions 1

# Create spark job


# Create our folders on Hadoop file system and total permission to those
docker exec namenode hdfs dfs -rm -r /cointrendy
docker exec namenode hdfs dfs -mkdir /cointrendy
docker exec namenode hdfs dfs -mkdir /cointrendy/checkpoint
docker exec namenode hdfs dfs -chmod -R 777 /cointrendy
docker exec namenode hdfs dfs -chmod -R 777 /cointrendy/checkpoint

# Create virtual environment for python and install requirements
pip install virtualenv
virtualenv .venv
source .venv/bin/activate
pip install -r requirements.txt
