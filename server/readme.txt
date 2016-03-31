cluster:
make build
spark-submit --class Main target/scala-2.10/sbdsample-assembly-0.1-SNAPSHOT.jar

test:
sbt test
test-only AliyunDataset

run:
sbt run