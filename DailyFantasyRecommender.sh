#!/bin/sh

spark_path=$1
no_clusters=$2
# rec_mode=$3

if [ -z $spark_path ]; then
	echo "Please provide path to Apache Spark"
	exit 1
fi

spark_path="$spark_path/bin/spark-submit"

# if [ -z $no_clusters ]; then
# 	echo "Please provide number of clusters to create"
# 	exit 1
# fi


$spark_path scripts/tiers.py
# $spark_path scripts/recommend.py 'default' > rosterRecommendation.txt
# optional hybrid mode that combines clustering and roster recommender
$spark_path scripts/recommend.py 'hybrid' > rosterRecommendation.txt
