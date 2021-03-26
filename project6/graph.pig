graph = LOAD '$G' USING PigStorage(',') AS (src:long, dest:long);
graph_group = GROUP graph by src;
neighbours_count = FOREACH graph_group GENERATE group, COUNT(graph);
c = GROUP neighbours_count by $1;
o = FOREACH c GENERATE group, COUNT(neighbours_count);
STORE o INTO '$O' USING PigStorage(',') ; 