NFLplaybyplay
=============

Hive database of NFL play by play data.

The following explains each of the files involved:

NOTE: PlayByPlay Map-Reduce jobs are forks of code written by Jesse Anderson (https://github.com/eljefe6a/nfldata)

PLAYBYPLAYDRIVER.java:
Java file that runs the Map Reduce job. Call both PLAYBYPLAYMAPPPER.java and PLAYBYPLAYREDUCER.java

PLAYBYPLAYMAPPER.java:
Map Reduce mapping file. This file does most of the work in parsing the "description" key in the raw data, and building the new feilds for the output file.

PLAYBYPLAYREDUCER.java:
Map Reduce reducer file. This file is responsible for looking at an entire game, and adding a few feilds to the end of each play (who won, who lost, winning score, losing score).

HIVEBUILD.hql
This file builds the HIVE database. The main table is .playbyplay. which uses the MR output as a source. The other table created is .salaries. which uses a parses .csv file as a source

QUERYSET.hql
A list of the various queries I ran to analyze the data that was presented in my paper and presentation.

ETL.sh
Shell script file that runs full job. Assumes one has hadoop/hdfs running and has the noted BASEDIR created. One must also creat /usr/hive/warehouse in order to build new tables in HIVE.
