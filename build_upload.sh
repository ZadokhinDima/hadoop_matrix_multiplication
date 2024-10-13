mvn clean install
docker cp A.txt namenode:/tmp/A.txt
docker cp B.txt namenode:/tmp/B.txt
docker cp target/hadoop-job-1.0-SHAPSHOT namenode:/tmp/job.jar
