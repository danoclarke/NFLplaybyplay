#/bin/bash

BASEDIR=/user/danielclarke

#COMPILE JAVA CODE (MR)
cd src
javac -classpath `hadoop classpath` *.java
jar cf ../playbyplay.jar *.class
cd ..

#REMOVE/CREATE NEEDED FOLDERS AND PLACE DATA
$HADOOP_HOME/bin/hdfs dfs -rm -r $BASEDIR/input
$HADOOP_HOME/bin/hdfs dfs -rm -r $BASEDIR/output
$HADOOP_HOME/bin/hdfs dfs -rm -r $BASEDIR/salaries
$HADOOP_HOME/bin/hdfs dfs -put -f input $BASEDIR/input
$HADOOP_HOME/bin/hdfs dfs -mkdir $BASEDIR/salaries
$HADOOP_HOME/bin/hdfs dfs -put -f salaries/salaries.csv $BASEDIR/salaries/

#RUN MR
$HADOOP_HOME/bin/hadoop jar playbyplay.jar PlayByPlayDriver $BASEDIR/input $BASEDIR/output

#CREATE TABLES
hive -S -f HIVEbuild.hql
