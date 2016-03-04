scalaVersion := "2.10.5"
name := "sbdsample"

libraryDependencies ++= Seq(
    "io.spray" %% "spray-can"       % "1.3.3",
    "io.spray" %% "spray-http"      % "1.3.3",
    "io.spray" %% "spray-routing"   % "1.3.3",
    "io.spray" %% "spray-client"    % "1.3.3",
    "io.spray" %% "spray-json"      % "1.3.2",

    "log4j" % "log4j" % "1.2.17",
    
    "org.apache.spark" %% "spark-core"      % "1.4.1" % "provided",
    "org.apache.spark" %% "spark-sql"       % "1.4.1" % "provided",
    "org.apache.spark" %% "spark-hive"      % "1.4.1" % "provided",
    "org.apache.spark" %% "spark-mllib"     % "1.4.1" % "provided",
    "org.apache.spark" %% "spark-streaming" % "1.4.1" % "provided",
  	"org.apache.spark" %% "spark-streaming-kafka" % "1.4.1" % "provided",
  	"org.apache.spark" %% "spark-streaming-flume" % "1.4.1" % "provided",
    
    "org.slf4j" % "slf4j-api" % "1.7.5",
    "org.slf4j" % "slf4j-log4j12" % "1.7.5",

    "org.scalatest" %% "scalatest" % "2.0" % "test",
    "org.scalacheck" %% "scalacheck" % "1.12.5" % "test",
    "com.typesafe.akka" %% "akka-testkit" % "2.3.9" % "test"
)

test in assembly := {}

fork := true

run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run)) 
