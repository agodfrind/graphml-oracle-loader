import java.io.*;
import java.util.*;
import java.time.*;
import java.sql.*;
import oracle.jdbc.*;
import javax.xml.stream.*;
import javax.xml.stream.events.*;

public class GraphMLImporter {

  static boolean isNeo4J;
  static Instant start;
  static Instant instant;

  public static void main(String[] args) throws Exception {

    String filename   = null;
    String jdbcUrl    = null;
    String username   = null;
    String password   = null;
    String graphname  = null;
    String action     = "CREATE";
    String format     = "TINKERPOP";
    int    batchsize  = 0;
    long   skipItems  = 0;
    long   numItems   = 0;

    int i=0;
    while (i<args.length) {
      switch (args[i]) {
        case "-f" : case "--filename":  filename   = args[i+1]; break;
        case "-d" : case "--jdbcUrl":   jdbcUrl    = args[i+1]; break;
        case "-u" : case "--username":  username   = args[i+1]; break;
        case "-p" : case "--password":  password   = args[i+1]; break;
        case "-g" : case "--graphname": graphname  = args[i+1]; break;
        case "-a" : case "--action":    action     = args[i+1]; break;
        case "-t" : case "--format":    format     = args[i+1]; break;
        case "-b" : case "--batchsize": batchsize  = Integer.parseInt(args[i+1]); break;
        case "-s" : case "--skipItems": skipItems  = Integer.parseInt(args[i+1]); break;
        case "-n" : case "--numItems":  numItems   = Integer.parseInt(args[i+1]); break;
      }
      i++;
    }

    if (filename == null | jdbcUrl == null | username == null | password == null | graphname == null) {
      System.out.println ("Parameters:");
      System.out.println ("  -f/--filename  <filename>:         name of GraphML file to import");
      System.out.println ("  -d/--jdbcUrl   <JDBC connection>:  JDBC connection string (jdbc:oracle:thin:@server:port/service)");
      System.out.println ("  -u/--username  <User>:             Database user name");
      System.out.println ("  -p/--password  <Password>:         Database user password");
      System.out.println ("  -g/--graphnam  <graphname>:        Name of the graph to create or load into");
      System.out.println ("  -a/--action    <action>:           [CREATE] or APPEND or REPLACE");
      System.out.println ("  -t/--format    <format>:           NEO4J or [TINKERPOP]");
      System.out.println ("  -c/--batchsize <batchsize>:        commit interval (0 = only commit at the end)");
      System.out.println ("  -s/--skipItem  <skipItems>:        number of items to skip (0 = nothing to skip)");
      System.out.println ("  -n/--numItems  <numItems>:         number of items to read (0 = until the ends)");
      return;
    }

    isNeo4J = format.toUpperCase().equals("NEO4J") ? true : false;

    System.out.println("Connecting to Database "+jdbcUrl);
    Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
    conn.setAutoCommit(false);
    switch (action.toUpperCase()) {
      case ("CREATE"):
        createGraph(conn, graphname.toUpperCase());
        break;
      case ("REPLACE"):
        clearGraph(conn, graphname.toUpperCase());
        break;
    }
    processFile (filename, conn, graphname, batchsize, skipItems, numItems);
  }

  static void createGraph(Connection conn, String graphname)
  throws Exception {
    System.out.println ("Creating PG graph "+graphname.toUpperCase());
    String createPG = "BEGIN OPG_APIS.CREATE_PG(:1, DOP=>8, NUM_HASH_PTNS=>16, OPTIONS=>'SKIP_INDEX=T'); END;";
    CallableStatement cs = conn.prepareCall(createPG);
    cs.setString(1, graphname);
    cs.execute();

    // Set all tables to nologging
    cs = conn.prepareCall(createPG);
    PreparedStatement ps;
    conn.prepareStatement("alter table "+ graphname + "VT$ nologging").execute();
    conn.prepareStatement("alter table "+ graphname + "GE$ nologging").execute();
    conn.prepareStatement("alter table "+ graphname + "SS$ nologging").execute();
    conn.prepareStatement("alter table "+ graphname + "GT$ nologging").execute();
    conn.prepareStatement("alter table "+ graphname + "IT$ nologging").execute();
  }

  static void clearGraph(Connection conn, String graphname)
  throws Exception {
    System.out.println ("Clearing PG graph "+graphname.toUpperCase());
    CallableStatement cs = conn.prepareCall("begin opg_apis.clear_pg(:1); end;");
    cs.setString(1, graphname);
    cs.execute();
    System.out.println ("Graph cleared");
  }

  static void processFile(
    String filename, Connection conn, String graphname, int batchsize, long skipItems, long numItems
  ) throws Exception {
    System.out.println ("Processing file "+filename);
    XMLInputFactory inputFactory = XMLInputFactory.newInstance();
    XMLStreamReader xmlReader = inputFactory.createXMLStreamReader(new FileInputStream(filename));
    Map<String, String> keyIdMap = new HashMap<>();
    Map<String, String> keyTypesMaps = new HashMap<>();

    // Vertex Data
    long vid = 0;
    String vLabel = null;
    Map<String, Object> vProps = null;
    boolean inV = false;
    Long vCounter = 0L;

    // Edge Data
    long eid = 0;
    long svid = 0;
    long dvid = 0;
    String eLabel = null;
    Map<String, Object> eProps = null;
    boolean inE = false;
    Long eCounter = 0L;

    // Insert statements
    PreparedStatement vInsert = conn.prepareStatement(
      "INSERT INTO " + graphname + "VT$ (vid,vl,k,t,v) VALUES (?,?,?,?,?)"
    );
    PreparedStatement eInsert = conn.prepareStatement(
      "INSERT INTO " + graphname + "GE$ (eid,svid,dvid,el,k,t,v) VALUES (?,?,?,?,?,?,?)"
    );

    // Label identifiers (passed as properties)
    String vLabelKey;
    String eLabelKey;
    if (isNeo4J) {
      vLabelKey = "labels";
      eLabelKey = "label";
    } else {
      vLabelKey = "labelV";
      eLabelKey = "labelE";
    }
    String eLabelDefault = "edge";
    String vLabelDefault = "vertex";

    // Skip counters
    long skipCounter = skipItems;
    long numCounter = 0;

    if (skipItems > 0)
      System.out.println ("Skipping "+skipItems+" items ...");

    instant = Instant.now();
    start = instant;
    while (xmlReader.hasNext()) {
      Integer xmlEvent = xmlReader.next();

      // Process start element
      if (xmlEvent.equals(XMLEvent.START_ELEMENT)) {
        String xmlTag = xmlReader.getName().getLocalPart();
        switch (xmlTag) {
          // Process <key> element
          // <key id="GENRE" for="node" attr.name="GENRE" attr.type="string"></key>
          case "key":
            String id = xmlReader.getAttributeValue(null, "id");
            String attributeName = xmlReader.getAttributeValue(null, "attr.name");
            String attributeType = xmlReader.getAttributeValue(null, "attr.type");
            keyIdMap.put(id, attributeName);
            keyTypesMaps.put(id, attributeType);
            break;
          // Process <node> element
          // <node id="1"><data key="labelV">PERSON</data><data key="ROLE">political authority</data>...</node>
          case "node":
            if (isNeo4J)
              vid = Long.parseLong(xmlReader.getAttributeValue(null, "id").substring(1));
            else
              vid = Long.parseLong(xmlReader.getAttributeValue(null, "id"));
            inV = true;
            vProps = new HashMap<>();
            break;
          // Process <edge> element
          // <edge id="1000" source="1" target="2"><data key="labelE">COLLABORATES</data><data key="WEIGHT">1.0</data></edge>
          case "edge":
            String s;
            if (isNeo4J) {
              eid =  Long.parseLong(xmlReader.getAttributeValue(null, "id").substring(1));
              svid = Long.parseLong(xmlReader.getAttributeValue(null, "source").substring(1));
              dvid = Long.parseLong(xmlReader.getAttributeValue(null, "target").substring(1));
            }
            else {
              eid =  Long.parseLong(xmlReader.getAttributeValue(null, "id"));
              svid = Long.parseLong(xmlReader.getAttributeValue(null, "source"));
              dvid = Long.parseLong(xmlReader.getAttributeValue(null, "target"));
            }
            inE = true;
            eProps = new HashMap<>();
            break;
          // Process <data> element (vertex/edge property)
          // <data key="COUNTRY">United States</data>
          case "data":
            String key = xmlReader.getAttributeValue(null, "key");
            String dataAttributeName = keyIdMap.get(key);
            String value = xmlReader.getElementText();
            if (inV) {
              if (key.equals(vLabelKey))
                if (isNeo4J)
                  // Remove leading ":" from vertex labels
                  vLabel = value.substring(1);
                else
                  vLabel = value;
              else
                vProps.put(key, typeCastValue(key, value, keyTypesMaps));
            } else if (inE) {
              if (key.equals(eLabelKey))
                eLabel = value;
              else
                eProps.put(key, typeCastValue(key, value, keyTypesMaps));
            }
            break;
        }

      // Process end element
      } else if (xmlEvent.equals(XMLEvent.END_ELEMENT)) {
        String xmlTag = xmlReader.getName().getLocalPart();
        switch (xmlTag) {
          case "node":
            if (skipCounter-- <= 0) {
              if (skipCounter == -1 && skipItems > 0) {
                System.out.println("... done skipping");
                instant = Instant.now();
                start = instant;
              }
              numCounter++;
              // If no properties exist for this vertex, then add a dummy blank property
              if (vProps.size()==0)
                vProps.put(" ", " ");
              // If no label exists, use default label
              if (vLabel == null)
                vLabel = vLabelDefault;
              vCounter++;
              // Write the vertex to the vertex table
              writeVertex(vInsert,vid,vLabel,vProps);
              // Commit batch as requested
              if (batchsize > 0)
                commitBatch (vInsert,eInsert,vCounter,eCounter,batchsize,conn);
              // Reset context
              vid = 0;
              vLabel = null;
              vProps = null;
              inV = false;
            }
            break;

          case "edge":
            if (skipCounter-- <= 0) {
              if (skipCounter == -1 && skipItems > 0) {
                System.out.println("... done skipping");
                instant = Instant.now();
                start = instant;
              }
              numCounter++;
              // If no properties exist for this edge, then add a dummy blank property
              if (eProps.size()==0)
                eProps.put(" ", " ");
              // If no label exists, use default label
              if (eLabel == null)
                eLabel = eLabelDefault;
              eCounter++;
              // Write the edge to the edge table
              writeEdge(eInsert,eid,eLabel,svid, dvid, eProps);
              // Commit batch as requested
              if (batchsize > 0)
                commitBatch (vInsert,eInsert,vCounter,eCounter,batchsize,conn);
              // Reset context
              eid = 0;
              svid = 0;
              dvid = 0;
              eLabel = null;
              eProps = null;
              inE = false;
            }
            break;
        }
      }

      if (numItems > 0)
        if (numCounter >= numItems)
          break;

    }
    // Final commit
    commitBatch (vInsert,eInsert,vCounter,eCounter,-1,conn);
    System.out.println ("Graph "+graphname.toUpperCase()+" imported");
    System.out.println (vCounter+" vertices");
    System.out.println (eCounter+" edges");
  }

  // Write a vertex to database
  static void writeVertex(PreparedStatement vInsert, Long vid, String vLabel, Map<String, Object> vProps)
  throws Exception {
    for (Map.Entry<String, Object> e : vProps.entrySet()) {
      String k = e.getKey();
      Object v = e.getValue();
      vInsert.setLong   (1, vid);                   // VID (vertex id)
      vInsert.setString (2, vLabel.toUpperCase());  // VL  (label)
      vInsert.setString (3, k.toUpperCase());       // K   (property name)
      vInsert.setLong   (4, 1);                     // T   (data type)
      vInsert.setString (5, v.toString());          // V   (string value)
      vInsert.addBatch();
    }
  }

  // Write an edge to database
  static void writeEdge(PreparedStatement eInsert, Long eid, String eLabel, Long svid, Long dvid, Map<String, Object> eProps)
  throws Exception {
    for (Map.Entry<String, Object> e : eProps.entrySet()) {
      String k = e.getKey();
      Object v = e.getValue();
      eInsert.setLong   (1, eid);                   // EID (vertex id)
      eInsert.setLong   (2, svid);                  // SVID (source vertex id)
      eInsert.setLong   (3, dvid);                  // DVID (destination vertex id)
      eInsert.setString (4, eLabel.toUpperCase());  // EL  (label)
      eInsert.setString (5, k.toUpperCase());       // K   (property name)
      eInsert.setLong   (6, 1);                     // T   (data type)
      eInsert.setString (7, v.toString());          // V   (string value)
      eInsert.addBatch();
    }
  }

  // Commit and log progress
  static void commitBatch (PreparedStatement vInsert, PreparedStatement eInsert, long vCounter, long eCounter, int batchsize, Connection conn)
  throws Exception {
    if ((vCounter+eCounter) % batchsize == 0) {
      vInsert.executeBatch();
      eInsert.executeBatch();
      Instant now = Instant.now();
      System.out.println (
        Instant.now() + ": " +
        vCounter + " vertices, "+eCounter +" edges inserted " +
        "in " + ( now.toEpochMilli()-instant.toEpochMilli() ) + " ms " +
        "("  + ( (float)batchsize/(now.toEpochMilli()-instant.toEpochMilli())*1000) + " per second) " +
        "accumulated: " + (now.toEpochMilli()-start.toEpochMilli()) + " ms " +
        " "  + ( (float)(vCounter+eCounter)/(now.toEpochMilli()-start.toEpochMilli())*1000) + " per second) "
      );
      conn.commit();
      instant = now;
    }
  }

  // Cast property values to the proper data type
  static Object typeCastValue(String key, String value, Map<String, String> keyTypes) {
    String type = keyTypes.get(key);
    if (null == type || type.equals("string"))
        return value;
    else if (type.equals("float"))
        return Float.valueOf(value);
    else if (type.equals("int"))
        return Integer.valueOf(value);
    else if (type.equals("double"))
        return Double.valueOf(value);
    else if (type.equals("boolean"))
        return Boolean.valueOf(value);
    else if (type.equals("long"))
        return Long.valueOf(value);
    else
        return value;
  }
}