 --3RD DOWN PLAY TYPE BASED ON DISTANCE
 set hive.cli.print.header=true;
 select playbyplay.playtype, playbyplay.yardstogo, playbyplay.down, playbyplay.totalperplay, totalstable.total, ((playbyplay.totalperplay / totalstable.total) * 100) as percentage from  
   (select playtype, yardstogo, down, count(*) as totalperplay from playbyplay where yardstogo<=40 and down=3 group by yardstogo, playtype, down) playbyplay  
 join 
   (select yardstogo, count(*) as total from playbyplay group by yardstogo) totalstable
   on totalstable.yardstogo = playbyplay.yardstogo
 order by yardstogo, percentage, playtype;

-- 4TH QUARTER COMPLETION % (Ovr, Winning, Losing) PER QB
 set hive.cli.print.header=true;
 select playbyplay.qb, 
 comp.completes as ovrcomp, 
 incomp.incompletes as ovrincomp, 
 (comp.completes / (comp.completes + incomp.incompletes)) as cpoverall, 
 comp4w.completes as 4thWcomp, incomp4w.incompletes as 4thWincomp, 
 (comp4w.completes / (comp4w.completes + incomp4w.incompletes)) as cp4thW, 
 comp4l.completes as 4thLcomp, incomp4l.incompletes as 4thLincomp, 
 (comp4l.completes / (comp4l.completes + incomp4l.incompletes)) as cp4thL 
 from (select qb from playbyplay group by qb) playbyplay 
 join (select qb, count(*) as completes from playbyplay where playtype = 'PASS' and incomplete = false group by qb) comp on comp.qb = playbyplay.qb 
 join (select qb, count(*) as incompletes from playbyplay where playtype = 'PASS' and incomplete = true group by qb) incomp on incomp.qb = playbyplay.qb
 join (select qb, count(*) as completes from playbyplay where playtype = 'PASS' and incomplete = false and quarter = '4' and offensescore >= defensescore group by qb) comp4w on comp4w.qb = playbyplay.qb
 join (select qb, count(*) as incompletes from playbyplay where playtype = 'PASS' and incomplete = true and quarter = '4' and offensescore >= defensescore group by qb) incomp4w on incomp4w.qb = playbyplay.qb
 join (select qb, count(*) as completes from playbyplay where playtype = 'PASS' and incomplete = false and quarter = '4' and offensescore < defensescore group by qb) comp4l on comp4l.qb = playbyplay.qb
 join (select qb, count(*) as incompletes from playbyplay where playtype = 'PASS' and incomplete = true and quarter = '4' and offensescore < defensescore group by qb) incomp4l on incomp4l.qb = playbyplay.qb
 where (comp.completes + incomp.incompletes) >= 400;

-- KICKER SUCCESS PER KICKER
 set hive.cli.print.header=true;
 select playbyplay.qb, fgsmade.fgmade, fgsmiss.fgmiss, (fgsmade.fgmade/(fgsmade.fgmade + fgsmiss.fgmiss)) as fgperc, ((fgsmade.fgmade)*3) as ptsscored, playbyplay.year
 from (select qb, year, quarter from playbyplay group by qb, year, quarter) playbyplay
 join (select qb, count(*) as fgmade from playbyplay where playtype = "FIELDGOAL" and isgoalgood = true group by qb) fgsmade on playbyplay.qb = fgsmade.qb
 join (select qb, count(*) as fgmiss from playbyplay where playtype = "FIELDGOAL" and isgoalgood = false group by qb) fgsmiss on playbyplay.qb = fgsmiss.qb
 where (fgsmade.fgmade + fgsmiss.fgmiss)>=100 
 order by year, fgperc DESC;

-- KICKER SUCCESS (2MIN) PER KICKER
 set hive.cli.print.header=true;
 select playbyplay.qb, fgsmade.fgmade, fgsmiss.fgmiss, (fgsmade.fgmade/(fgsmade.fgmade + fgsmiss.fgmiss)) as fgperc, ((fgsmade.fgmade)*3) as ptsscored
 from (select qb, quarter, gameminutes from playbyplay where quarter = 4 and gameminutes <= 1 group by qb, quarter, gameminutes) playbyplay
 join (select qb, count(*) as fgmade from playbyplay where playtype = "FIELDGOAL" and isgoalgood = true group by qb) fgsmade on playbyplay.qb = fgsmade.qb
 join (select qb, count(*) as fgmiss from playbyplay where playtype = "FIELDGOAL" and isgoalgood = false group by qb) fgsmiss on playbyplay.qb = fgsmiss.qb
 where (fgsmade.fgmade + fgsmiss.fgmiss)>=100 
 order by fgperc DESC;

-- KICKER SUCCESS (2MIN) PER KICKER
 set hive.cli.print.header=true;
 select count(isgoalgood) as num, isgoalgood from playbyplay_fgs where quarter = 4 and gameminutes <= 1 group by isgoalgood;

-- OVR KICKER SUCCESS PER QUARTER
 set hive.cli.print.header=true;
 select fgatt.numatt as attmp, fgmade.nummade as nummade, fgmade.nummade/fgatt.numatt as fgperc, fgperq.quarter from
 (select * from playbyplay_fgs) as fgperq
 join (select count(isgoalgood) as numatt, quarter from playbyplay_fgs group by quarter) fgatt on fgperq.quarter = fgatt.quarter
 join (select count(isgoalgood) as nummade, quarter from playbyplay_fgs where isgoalgood = true group by quarter) fgmade on fgperq.quarter = fgmade.quarter
 group by fgperq.quarter, fgatt.numatt, fgmade.nummade;

-- OVR KICKER SUCCESS PER QUARTER (WHEN LOSING)
set hive.cli.print.header=true;
select fgatt.numatt as attmp, fgmade.nummade as nummade, fgmade.nummade/fgatt.numatt as fgperc, fgperq.quarter from
(select * from playbyplay_fgs where offensescore < defensescore) as fgperq
join (select count(isgoalgood) as numatt, quarter from playbyplay_fgs where offensescore < defensescore group by quarter) fgatt on fgperq.quarter = fgatt.quarter
join (select count(isgoalgood) as nummade, quarter from playbyplay_fgs where isgoalgood = true and offensescore < defensescore group by quarter) fgmade on fgperq.quarter = fgmade.quarter
group by fgperq.quarter, fgatt.numatt, fgmade.nummade;

-- OVR KICKER SUCCESS PER QUARTER (WHEN WINNING)
set hive.cli.print.header=true;
select fgatt.numatt as attmp, fgmade.nummade as nummade, fgmade.nummade/fgatt.numatt as fgperc, fgperq.quarter from
(select * from playbyplay_fgs where offensescore > defensescore) as fgperq
join (select count(isgoalgood) as numatt, quarter from playbyplay_fgs where offensescore > defensescore group by quarter) fgatt on fgperq.quarter = fgatt.quarter
join (select count(isgoalgood) as nummade, quarter from playbyplay_fgs where isgoalgood = true and offensescore > defensescore group by quarter) fgmade on fgperq.quarter = fgmade.quarter
group by fgperq.quarter, fgatt.numatt, fgmade.nummade;

-- OVR KICKER SUCCESS PER QUARTER (WHEN TIED)
set hive.cli.print.header=true;
select fgatt.numatt as attmp, fgmade.nummade as nummade, fgmade.nummade/fgatt.numatt as fgperc, fgperq.quarter from
(select * from playbyplay_fgs where offensescore = defensescore) as fgperq
join (select count(isgoalgood) as numatt, quarter from playbyplay_fgs where offensescore = defensescore group by quarter) fgatt on fgperq.quarter = fgatt.quarter
join (select count(isgoalgood) as nummade, quarter from playbyplay_fgs where isgoalgood = true and offensescore = defensescore group by quarter) fgmade on fgperq.quarter = fgmade.quarter
group by fgperq.quarter, fgatt.numatt, fgmade.nummade;

-- OVR KICKER SUCCESS PER QUARTER (WHEN LOSING - KICK TO TIE/TAKE LEAD)
set hive.cli.print.header=true;
select fgatt.numatt as attmp, fgmade.nummade as nummade, fgmade.nummade/fgatt.numatt as fgperc, fgperq.quarter from
(select * from playbyplay_fgs where (offensescore-defensescore) < 0 and (offensescore-defensescore) >= -3) as fgperq
join (select count(isgoalgood) as numatt, quarter from playbyplay_fgs where (offensescore-defensescore) < 0 and (offensescore-defensescore) >= -3 group by quarter) fgatt on fgperq.quarter = fgatt.quarter
join (select count(isgoalgood) as nummade, quarter from playbyplay_fgs where isgoalgood = true and (offensescore-defensescore) < 0 and (offensescore-defensescore) >= -3 group by quarter) fgmade on fgperq.quarter = fgmade.quarter
group by fgperq.quarter, fgatt.numatt, fgmade.nummade;

-- KICK SUCCESS VS DISTANCE (WHEN LOSING - KICK TO TIE/TAKE LEAD)
set hive.cli.print.header=true;
select fgatt.numatt as attmp, fgmade.nummade as nummade, fgmade.nummade/fgatt.numatt as fgperc, fgperq.quarter, fgperq.kickdistance from
(select * from playbyplay_fgs where (offensescore-defensescore) < 0 and (offensescore-defensescore) >= -3 and kickdistance >=35) as fgperq
join (select count(isgoalgood) as numatt, quarter, kickdistance from playbyplay_fgs where (offensescore-defensescore) < 0 and (offensescore-defensescore) >= -3 and kickdistance >=35 group by quarter, kickdistance) fgatt on (fgperq.quarter = fgatt.quarter and fgperq.kickdistance = fgatt.kickdistance)
join (select count(isgoalgood) as nummade, quarter, kickdistance from playbyplay_fgs where isgoalgood = true and (offensescore-defensescore) < 0 and (offensescore-defensescore) >= -3 and kickdistance >=35 group by quarter, kickdistance) fgmade on (fgperq.quarter = fgmade.quarter and fgperq.kickdistance = fgmade.kickdistance)
group by fgperq.quarter, fgperq.kickdistance, fgatt.numatt, fgmade.nummade;

-- KICK SUCCESS VS DISTANCE (OVR) (GROUPED)
(select count(isgoalgood) as made, isgoalgood as total from playbyplay_fgs where kickdistance <= 20 group by isgoalgood) kick20
(select count(isgoalgood) as made, isgoalgood from playbyplay_fgs where kickdistance <= 30 and kickdistance > 20 group by isgoalgood) kick30
(select count(isgoalgood) as made, isgoalgood from playbyplay_fgs where kickdistance <= 40 and kickdistance > 30 group by isgoalgood) kick40
(select count(isgoalgood) as made, isgoalgood from playbyplay_fgs where kickdistance <= 45 and kickdistance > 40 group by isgoalgood) kick45
(select count(isgoalgood) as made, isgoalgood from playbyplay_fgs where kickdistance <= 50 and kickdistance > 45 group by isgoalgood) kick50
(select count(isgoalgood) as made, isgoalgood from playbyplay_fgs where kickdistance <= 55 and kickdistance > 50 group by isgoalgood) kick55
(select count(isgoalgood) as made, isgoalgood from playbyplay_fgs where kickdistance <= 60 and kickdistance > 55 group by isgoalgood) kick60
(select count(isgoalgood) as made, isgoalgood from playbyplay_fgs where kickdistance > 60 group by isgoalgood) kickp

-- KICK SUCCESS VS DISTANCE (WHEN LOSING - KICK TO TIE/TAKE LEAD) (GROUPED)
select count(isgoalgood) as made, isgoalgood as total from playbyplay_fgs where kickdistance <= 20 and (offensescore-defensescore) < 0 and (offensescore-defensescore) >= -3 group by isgoalgood; kick20
select count(isgoalgood) as made, isgoalgood from playbyplay_fgs where kickdistance <= 30 and kickdistance > 20 and (offensescore-defensescore) < 0 and (offensescore-defensescore) >= -3 group by isgoalgood; kick30
select count(isgoalgood) as made, isgoalgood from playbyplay_fgs where kickdistance <= 40 and kickdistance > 30 and (offensescore-defensescore) < 0 and (offensescore-defensescore) >= -3 group by isgoalgood; kick40
select count(isgoalgood) as made, isgoalgood from playbyplay_fgs where kickdistance <= 45 and kickdistance > 40 and (offensescore-defensescore) < 0 and (offensescore-defensescore) >= -3 group by isgoalgood; kick45
select count(isgoalgood) as made, isgoalgood from playbyplay_fgs where kickdistance <= 50 and kickdistance > 45 and (offensescore-defensescore) < 0 and (offensescore-defensescore) >= -3 group by isgoalgood; kick50
select count(isgoalgood) as made, isgoalgood from playbyplay_fgs where kickdistance <= 55 and kickdistance > 50 and (offensescore-defensescore) < 0 and (offensescore-defensescore) >= -3 group by isgoalgood; kick55
select count(isgoalgood) as made, isgoalgood from playbyplay_fgs where kickdistance <= 60 and kickdistance > 55  and (offensescore-defensescore) < 0 and (offensescore-defensescore) >= -3 group by isgoalgood; kick60
select count(isgoalgood) as made, isgoalgood from playbyplay_fgs where kickdistance > 60 and (offensescore-defensescore) < 0 and (offensescore-defensescore) >= -3 group by isgoalgood; kickp

-- KICK SUCCESS IN HIGH PRESSURE VS TIME REMAINING IN GAME
select count(isgoalgood) as made, isgoalgood as total from playbyplay_fgs where quarter = 4 and gameminutes = 4 and (offensescore-defensescore) < 0 and (offensescore-defensescore) >= -3 group by isgoalgood;
select count(isgoalgood) as made, isgoalgood as total from playbyplay_fgs where quarter = 4 and gameminutes = 3 and (offensescore-defensescore) < 0 and (offensescore-defensescore) >= -3 group by isgoalgood; 
select count(isgoalgood) as made, isgoalgood as total from playbyplay_fgs where quarter = 4 and gameminutes = 2 and (offensescore-defensescore) < 0 and (offensescore-defensescore) >= -3 group by isgoalgood; 
select count(isgoalgood) as made, isgoalgood as total from playbyplay_fgs where quarter = 4 and gameminutes = 1 and (offensescore-defensescore) < 0 and (offensescore-defensescore) >= -3 group by isgoalgood; 
select count(isgoalgood) as made, isgoalgood as total from playbyplay_fgs where quarter = 4 and gameminutes = 0 and (offensescore-defensescore) < 0 and (offensescore-defensescore) >= -3 group by isgoalgood;

-- KICK SUCCESS IN HIGH PRESSURE VS TIME REMAINING IN GAME (SECONDS)
select gameseconds, count(isgoalgood) as made, isgoalgood as total from playbyplay_fgs where quarter = 4 and gameminutes = 0 and (offensescore-defensescore) < 0 and (offensescore-defensescore) >= -3 group by gameseconds, isgoalgood;

-- BEST KICKERS IN HIGH PRESSURE
select main.qb, fgatt.made as att, fgmade.made as made, (fgmade.made/fgatt.made) as perc from
(select distinct qb from playbyplay_fgs where quarter = 4 and gameminutes = 0 and (offensescore-defensescore) < 0 and (offensescore-defensescore) >= -3) main
join (select qb, count(*) as made from playbyplay_fgs where quarter = 4 and gameminutes = 0 and (offensescore-defensescore) < 0 and (offensescore-defensescore) >= -3 group by qb) fgatt on main.qb = fgatt.qb
join (select qb, count(*) as made from playbyplay_fgs where isgoalgood = true and quarter = 4 and gameminutes = 0 and (offensescore-defensescore) < 0 and (offensescore-defensescore) >= -3 group by qb) fgmade on main.qb = fgmade.qb
where (fgmade.made/fgatt.made) >= 0.75 and fgatt.made >= 3
order by perc desc, att desc;

-- BEST KICKERS IN HIGH PRESSURE (2002-2009)
select main.qb, fgatt.made as att, fgmade.made as made, (fgmade.made/fgatt.made) as perc from
(select distinct qb from playbyplay_fgs where quarter = 4 and gameminutes = 0 and (offensescore-defensescore) < 0 and (offensescore-defensescore) >= -3) main
join (select qb, count(*) as made from playbyplay_fgs where quarter = 4 and gameminutes = 0 and (offensescore-defensescore) < 0 and (offensescore-defensescore) >= -3 and year >=2002 and year<=2009 group by qb) fgatt on main.qb = fgatt.qb
join (select qb, count(*) as made from playbyplay_fgs where isgoalgood = true and quarter = 4 and gameminutes = 0 and (offensescore-defensescore) < 0 and (offensescore-defensescore) >= -3 and year >=2002 and year<=2009 group by qb) fgmade on main.qb = fgmade.qb
where (fgmade.made/fgatt.made) >= 0.75 and fgatt.made >= 3
order by perc desc, att desc;

-- BEST KICKERS SALARY
select * from salaries where name = 'J.Wilkins' or name = 'J.Feely' or name = 'J.Brown' or name = 'R.Longwell' or name = 'M.Stover' or name = 'J.Hanson' or name = 'J.Carney' or name = 'J.Elam' or name = 'J.Scobee' or name = 'R.Lindell' order by name desc, year asc;

-- ALL PTS SCORED
 set hive.cli.print.header=true;
 select sum(playbyplay.hometeamscore) + sum(playbyplay.awayteamscore) as totalpoints, (tds.numtds * 6) as offtdpts, ((fgs.numfg * 3) + xps.numxp) as kickerpts 
 from (select distinct game, hometeamscore, awayteamscore from playbyplay) as playbyplay, 
 (select count(*) as numfg from playbyplay where playtype = 'FIELDGOAL' and isgoalgood = true) as fgs, 
 (select count(*) as numxp from playbyplay where playtype = 'EXTRAPOINT' and isgoalgood = true) as xps,
 (select count(*) as numtds from playbyplay where (playtype = 'RUN' OR playtype = 'PASS' OR playtype = 'SCRAMBLE') and touchdown = true) as tds
 group by fgs.numfg, xps.numxp, tds.numtds;

-- ALL PTS SCORED 2002-2009
 set hive.cli.print.header=true;
 select sum(playbyplay.hometeamscore) + sum(playbyplay.awayteamscore) as totalpoints, (tds.numtds * 6) as offtdpts, (runtds.numtds * 6) as runtdpts, (passtds.numtds * 6) as passtdpts, (scramtds.numtds * 6) as scramtdpts,((fgs.numfg * 3) + xps.numxp) as kickerpts 
 from (select distinct game, hometeamscore, awayteamscore from playbyplay where year < 2010) as playbyplay, 
 (select count(*) as numfg from playbyplay where playtype = 'FIELDGOAL' and isgoalgood = true and year < 2010) as fgs, 
 (select count(*) as numxp from playbyplay where playtype = 'EXTRAPOINT' and isgoalgood = true and year < 2010) as xps,
 (select count(*) as numtds from playbyplay where (playtype = 'RUN') and touchdown = true and year < 2010) as runtds,
 (select count(*) as numtds from playbyplay where (playtype = 'PASS') and touchdown = true and year < 2010) as passtds,
 (select count(*) as numtds from playbyplay where (playtype = 'SCRAMBLE') and touchdown = true and year < 2010) as scramtds,
 (select count(*) as numtds from playbyplay where (playtype = 'RUN' OR playtype = 'PASS' OR playtype = 'SCRAMBLE') and touchdown = true and year < 2010) as tds
 group by fgs.numfg, xps.numxp, tds.numtds, runtds.numtds, passtds.numtds, scramtds.numtds;

-- TOTAL SALARY FOR KICKERS
 set hive.cli.print.header=true;
 select sum(salaries.salary) as kickersal, count(*) as totalKs, sum(salaries.salary)/count(*) as kickersalpp from salaries where salaries.pos = 'K';

-- TOTAL SALARY FOR QB/RB/WR/TE
 set hive.cli.print.header=true;
 select sum(salaries.salary) as kickersal, count(*) as totalp, sum(salaries.salary)/count(*) as salpp from salaries where (salaries.pos = 'QB' or salaries.pos = 'RB' or salaries.pos = 'WR' or salaries.pos = 'TE');

-- SALARY PER POS
 set hive.cli.print.header=true;
 select sum(salaries.salary) as totalsalary, salaries.pos, count(salaries.salary) as totalpperpos, sum(salaries.salary)/count(salaries.salary) as salperpos from salaries group by salaries.pos order by salperpos desc;
