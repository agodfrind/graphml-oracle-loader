# Oracle Graph - GraphML importer

GraphML is a common standard for sharing property graphs. See <http://graphml.graphdrawing.org> and <http://tinkerpop.apache.org/docs/3.4.2/dev/io/#graphml> for details. This importer reads a graphml file and loads it into a property graph in the Oracle Database.

Note that the standard is not complete: it does not say anything about labels or element identifiers. As a consequence, different tools use different interpretations. The importer understands two styles of GraphML encodings:

- Tinkerpop style:
```
<node id="1">
  <data key="labelV">person</data>
  <data key="name">marko</data>
  <data key="age">29</data>
</node>
<edge id="7" source="1" target="2">
  <data key="labelE">knows</data>
  <data key="weight">0.5</data>
</edge>
```
The labels are in properties `labelV` for vertices and `labelE` for edges.

- Neo4j style.
```
<node id="n188" labels=":Movie">
  <data key="labels">:Movie</data>
  <data key="title">The Matrix</data>
  <data key="tagline">Welcome to the Real World</data>
  <data key="released">1999</data>
</node>
<edge id="e267" source="n189" target="n188" label="ACTED_IN">
  <data key="label">ACTED_IN</data>
  <data key="roles">["Neo"]</data>
</edge>
```
The labels are in properties `labels` for vertices and `label` for edges. The vertex labels are prefixed with a `:` (removed by the importer). Note also that the identifiers of vertices and edges are preceded by letter `n` or `e`. Those are also removed.


GraphML files do not impose any specific order on the content: usually, all nodes appear before all edges, but that is not a requirement. The importer does not assume any specific order.


## Compiling the importer

This requires the java libraries included in your Oracle Graph installation:

```
$ export PG_HOME=/opt/oracle/graph
$ export CLASSPATH=.:$PG_HOME/lib/*
$ javac GraphMLImporter.java
```

## Using the importer

View the command line parameters:
```
$ java GraphMLImporter
Parameters:
  -f/--filename  <filename>:         name of GraphML file to import
  -d/--jdbcUrl   <JDBC connection>:  JDBC connection string (jdbc:oracle:thin:@server:port/service)
  -u/--username  <User>:             Database user name
  -p/--password  <Password>:         Database user password
  -g/--graphnam  <graphname>:        Name of the graph to create or load into
  -a/--action    <action>:           [CREATE] or APPEND or REPLACE
  -t/--format    <format>:           NEO4J or [TINKERPOP]
  -b/--batchsize <batchsize>:        commit interval (0 = only commit at the end)
  -s/--skipItem  <skipItems>:        number of items to skip (0 = nothing to skip)
  -n/--numItems  <numItems>:         number of items to read (0 = until the ends)
```
Read a GraphML file in *Tinkerpop* encoding (this is the default when no format is specified)
```
$ java GraphMLImporter \
  -f tinkerpop_sample.graphml \
  -d jdbc:oracle:thin:@localhost:1521/graphdb \
  -u scott -p tiger -g tinkerpop_sample

Connecting to Database jdbc:oracle:thin:@localhost:1521/graphdb
Creating PG graph TINKERPOP_SAMPLE
Processing file tinkerpop_sample.graphml
2020-11-25T17:39:34.875Z: 6 vertices, 6 edges inserted in 38 ms (-26.31579 per second) accumulated: 38 ms  315.78946 per second)
Graph TINKERPOP_SAMPLE imported
6 vertices
6 edges
```
Read a file in *Neo4J* encoding:
```
$ java GraphMLImporter \
  -f neo4j_sample.graphml \
  -d jdbc:oracle:thin:@localhost:1521/graphdb \
  -u scott -p tiger -g neo4j_sample -t neo4j

Connecting to Database jdbc:oracle:thin:@localhost:1521/graphdb
Creating PG graph NEO4J_SAMPLE
Processing file neo4j_sample.graphml
2020-11-25T17:40:32.446Z: 8 vertices, 7 edges inserted in 46 ms (-21.73913 per second) accumulated: 46 ms  326.08698 per second)
Graph NEO4J_SAMPLE imported
8 vertices
7 edges
```

By default, the above examples will create a new graph.

## Parameters

**-f** or **--filename**: name of GraphML file to import

**-d** or **--jdbcUrl**:  JDBC connection string (jdbc:oracle:thin:@server:port/service)

**-u** or **--username**: Database user name

**-p** or **--password**: Database user password

**-g** or **--graphnam**: Name of the graph to create or load into

**-a** or **--action**:   Import action. *CREATE* (default) creates a new graph. *APPEND* adds to an existing graph. *REPLACE* drops the existing graph and creates a new one. *TRUNCATE* truncates the existing graph and loads the new content.

**-t** or **--format**:   This specifies the format used in the GraphML file as *TINKERPOP* (default) or *NEO4J*.

**-b** or **--batchsize**: commit interval (0 = only commit at the end)

**-s** or **--skipItem**: number of items to skip (0 = nothing to skip)

**-n** or **--numItems**: number of items to read (0 = until the ends)

**-i** or **--deferred**: YES or NO. If YES (the default), the graph is created without any index, and the GT and VD tables are only populated at the end of the import. If NO, the GT and VD tables are updated on the fly.

## Usage notes

### Batching

The ***--batchsize*** parameter specifies the frequency of commits. If not specified, or specified as `0` then the entire file is loaded in one step, with a final commit. This is fine for small files (a few thousands of vertices/edges), but for large files it is better to have regular commits: it allows you to monitor the progress of the load and also to restart from the last commit in case of failure.

For example, with a commit every 10000 graph items loaded:
```
$ java GraphMLImporter \
   -f /Users/albert/Documents/Data/graph/GraphML/edreams-graph.graphml \
   -d jdbc:oracle:thin:@localhost:1521/graphdb \
   -u scott -p tiger -g edreams -t neo4j -b 10000 -a create
Connecting to Database jdbc:oracle:thin:@localhost:1521/graphdb
Creating PG graph EDREAMS
Processing file /Users/albert/Documents/Data/graph/GraphML/edreams-graph.graphml
2020-11-25T18:16:31.521Z: 10000 vertices, 0 edges inserted in 929 ms (10764.262 per second) accumulated: 929 ms  10764.262 per second)
2020-11-25T18:16:32.418Z: 20000 vertices, 0 edges inserted in 897 ms (11148.271 per second) accumulated: 1826 ms  10952.902 per second)
2020-11-25T18:16:33.899Z: 30000 vertices, 0 edges inserted in 1481 ms (6752.1943 per second) accumulated: 3307 ms  9071.666 per second)
2020-11-25T18:16:35.340Z: 40000 vertices, 0 edges inserted in 1441 ms (6939.6255 per second) accumulated: 4748 ms  8424.6 per second)
2020-11-25T18:16:36.626Z: 50000 vertices, 0 edges inserted in 1286 ms (7776.05 per second) accumulated: 6034 ms  8286.377 per second)
2020-11-25T18:16:37.861Z: 60000 vertices, 0 edges inserted in 1235 ms (8097.166 per second) accumulated: 7269 ms  8254.23 per second)
2020-11-25T18:16:39.051Z: 70000 vertices, 0 edges inserted in 1190 ms (8403.361 per second) accumulated: 8459 ms  8275.209 per second)
2020-11-25T18:16:40.398Z: 80000 vertices, 0 edges inserted in 1347 ms (7423.905 per second) accumulated: 9806 ms  8158.271 per second)
^C
```
The log message shows the elapsed time and throughput for each batch of 10000 items (here vertices) as well as the total elapsed time since the beginning of the load, as well as the throughput since the beginning.

### Restart and selective loading

In case of failure, you can skip the items already loaded by indicating the number of items loaded in the last log message. For example, to resume the  interrupted import above, specify the ***-skipItems*** parameter:

```
$ java GraphMLImporter \
   -f /Users/albert/Documents/Data/graph/GraphML/edreams-graph.graphml \
   -d jdbc:oracle:thin:@localhost:1521/graphdb \
   -u scott -p tiger -g edreams -t neo4j -b 10000 -a append \
   -s 80000
Connecting to Database jdbc:oracle:thin:@localhost:1521/graphdb
Processing file /Users/albert/Documents/Data/graph/GraphML/edreams-graph.graphml
Skipping 80000 items ...
... done skipping
2020-11-25T18:22:41.368Z: 10000 vertices, 0 edges inserted in 1699 ms (5885.815 per second) accumulated: 1699 ms  5885.815 per second)
2020-11-25T18:22:43.090Z: 20000 vertices, 0 edges inserted in 1722 ms (5807.2007 per second) accumulated: 3421 ms  5846.2437 per second)
2020-11-25T18:22:44.614Z: 30000 vertices, 0 edges inserted in 1524 ms (6561.6797 per second) accumulated: 4945 ms  6066.734 per second)
2020-11-25T18:22:46.194Z: 40000 vertices, 0 edges inserted in 1580 ms (6329.114 per second) accumulated: 6525 ms  6130.268 per second)
2020-11-25T18:22:47.752Z: 50000 vertices, 0 edges inserted in 1558 ms (6418.4854 per second) accumulated: 8083 ms  6185.822 per second)
2020-11-25T18:22:49.176Z: 60000 vertices, 0 edges inserted in 1424 ms (7022.4717 per second) accumulated: 9507 ms  6311.139 per second)
2020-11-25T18:22:50.775Z: 70000 vertices, 0 edges inserted in 1599 ms (6253.9087 per second) accumulated: 11106 ms  6302.8994 per second)
2020-11-25T18:22:52.227Z: 80000 vertices, 0 edges inserted in 1452 ms (6887.0527 per second) accumulated: 12558 ms  6370.441 per second)
2020-11-25T18:22:53.658Z: 90000 vertices, 0 edges inserted in 1431 ms (6988.12 per second) accumulated: 13989 ms  6433.6265 per second)
2020-11-25T18:22:55.122Z: 100000 vertices, 0 edges inserted in 1464 ms (6830.601 per second) accumulated: 15453 ms  6471.2354 per second)
2020-11-25T18:22:56.497Z: 110000 vertices, 0 edges inserted in 1375 ms (7272.7275 per second) accumulated: 16828 ms  6536.7246 per second)
^C
```

You can also choose to only load a section of the input file by specifying the ***-numItems*** parameter, which can also be combined with the ***-skipItems*** parameter:

```
$ java GraphMLImporter \
   -f /Users/albert/Documents/Data/graph/GraphML/edreams-graph.graphml \
   -d jdbc:oracle:thin:@localhost:1521/graphdb \
   -u scott -p tiger -g edreams -t neo4j -b 10000 -a append \
   -s 120000 -n 40000
Connecting to Database jdbc:oracle:thin:@localhost:1521/graphdb
Processing file /Users/albert/Documents/Data/graph/GraphML/edreams-graph.graphml
Skipping 120000 items ...
... done skipping
2020-11-25T18:29:04.986Z: 10000 vertices, 0 edges inserted in 1629 ms (6138.7354 per second) accumulated: 1629 ms  6138.7354 per second)
2020-11-25T18:29:06.429Z: 20000 vertices, 0 edges inserted in 1443 ms (6930.007 per second) accumulated: 3072 ms  6510.4165 per second)
2020-11-25T18:29:07.943Z: 30000 vertices, 0 edges inserted in 1514 ms (6605.02 per second) accumulated: 4586 ms  6541.6484 per second)
2020-11-25T18:29:09.445Z: 40000 vertices, 0 edges inserted in 1502 ms (6657.7896 per second) accumulated: 6088 ms  6570.3022 per second)
2020-11-25T18:29:09.450Z: 40000 vertices, 0 edges inserted in 5 ms (-200.0 per second) accumulated: 6093 ms  6564.9106 per second)
Graph EDREAMS imported
40000 vertices
0 edges
```