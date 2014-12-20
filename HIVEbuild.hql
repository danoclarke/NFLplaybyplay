drop table if exists playbyplay;
CREATE EXTERNAL TABLE playbyplay (
	Game STRING COMMENT 'Game Id',
	Quarter INT COMMENT 'Game Quarter',
	GameMinutes INT COMMENT 'Game time countdown left in minutes',
	GameSeconds INT COMMENT 'Game time countdown left in seconds',
	Offense STRING COMMENT 'Team on offense',
	Defense STRING COMMENT 'Team on defense',
	Down INT COMMENT 'Down number',
	YardsToGo INT COMMENT 'Number of yards for a first down',
	YardLine INT COMMENT 'Yard line where the ball is',
	PlayDesc STRING COMMENT 'The original description of the play',
	OffenseScore INT COMMENT 'The offenses score as of the current play',
	DefenseScore INT COMMENT 'The defenses score as of the current play',
	Year INT COMMENT 'The year of the season',
	QB STRING COMMENT 'The QB/Punter/Kicker in a play',
	OffensivePlayer STRING COMMENT 'The receiver or runner',
	DefensivePlayer1 STRING COMMENT 'The name of the defensive player on the play',
	DefensivePlayer2 STRING COMMENT 'The name of the other defensive player on the play',
	Penalty BOOLEAN COMMENT 'Whether or not there was a penalty on the play',
	Fumble BOOLEAN COMMENT 'Whether or not there was a fumble on the play',
	Incomplete BOOLEAN COMMENT 'Whether or not there was an incomplete pass on the play',
	IsGoalGood BOOLEAN COMMENT 'For a extra point or field goal kick, whether or not it was good',
	PlayType STRING COMMENT '(Possible Values:PASS,INTERCEPTION,PUNT,RUN,KICKOFF,SPIKE,FIELDGOAL,EXTRAPOINT,SACK,KNEEL,REVIEW,SCRAMBLE,END) - The type of play that was run',
	HomeTeam STRING COMMENT 'The name of the home team',
	AwayTeam STRING COMMENT 'The name of the away team',
	DatePlayed STRING COMMENT 'The data of the game',
	PlayId STRING COMMENT 'The unique id of the play',
	Touchdown BOOLEAN COMMENT 'Whether or not there was a touchdown on the play',
	KickDistance STRING COMMENT 'Distance of Kick is play type is FIELDGOAL',
	Winner STRING COMMENT 'The name of the team that eventually wins',
	HomeTeamScore INT COMMENT 'The home teams score at the end of the game',
	AwayTeamScore INT COMMENT 'The away teams score at the end of the game'
	)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION "hdfs://localhost:9000/user/danielclarke/output";

drop table if exists salaries;
CREATE EXTERNAL TABLE salaries (
	Name STRING COMMENT 'Player Name',
	Pos STRING COMMENT 'Player Position',
	Team STRING COMMENT 'Player Team',
	Salary FLOAT COMMENT 'Player Salary',
	Year INT COMMENT 'Season Year'
	)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION "hdfs://localhost:9000/user/danielclarke/salaries";