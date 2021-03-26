drop table graph1;
drop table graph2;

create table graph1 (
node1 int,
neighbor_node1 int)
row format delimited fields terminated by ',' stored as textfile;

create table graph2 (
node2 int,
neighbor_node2 int);

load data local inpath '${hiveconf:G}' overwrite into table graph1;

INSERT INTO graph2
SELECT graph1.node1,COUNT(graph1.neighbor_node1)
FROM graph1
GROUP BY graph1.node1;

Select graph2.neighbor_node2,COUNT(graph2.node2)
FROM graph2
GROUP BY graph2.neighbor_node2
ORDER BY graph2.neighbor_node2 DESC;
