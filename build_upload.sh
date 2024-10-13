mvn clean install

docker cp A.txt namenode:/tmp/A.txt
docker cp B.txt namenode:/tmp/B.txt
docker cp target/hadoop-job-1.0-SNAPSHOT.jar namenode:/tmp/job.jar

docker exec namenode bash -c "hdfs dfs -rm -r /input"
docker exec namenode bash -c "hdfs dfs -rm -r /intermediate"
docker exec namenode bash -c "hdfs dfs -rm -r /output"
docker exec namenode bash -c "hdfs dfs -mkdir /input"
docker exec namenode bash -c "hdfs dfs -put /tmp/A.txt /input/A.txt"
docker exec namenode bash -c "hdfs dfs -put /tmp/B.txt /input/B.txt"

docker exec namenode bash -c "hadoop jar /tmp/job.jar MatrixMultiplicationJob /input/ /output/"

docker exec namenode bash -c "hdfs dfs -get /output/part-r-00000 /tmp/C.txt"
docker exec namenode bash -c "cat /tmp/C.txt"