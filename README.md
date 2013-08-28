recording-solver
================

Packet trace recorder for the Owl Platform

#Compile#
mvn clean package

#Run Temperature Collect#
java -cp target/recording-solver-1.0.0-SNAPSHOT-jar-with-dependencies.jar \\
  com.owlplatform.solver.recording.TempRecordingSolver agg.host.com -o test
