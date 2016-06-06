all: build submit

jar = sbdsample-assembly-0.1-SNAPSHOT.jar
args += --driver-java-options "-Dlog4j.configuration=file:conf/log4j.properties"
args += target/scala-2.10/$(jar)

build:
	sbt assembly -Dsbt.log.noformat=true

clean:
	rm -r target

submit:
	spark-submit --class Main $(args)

dataload:
	spark-submit --class DataMassage $(args)

start:
	daemonize -l server.lock -p server.pid -e server.err -o server.out -c `pwd` \
		`which spark-submit` --class Main $(args)

stop:
	kill `cat server.pid` || true

restart:
	make stop
	sleep 1
	make start
