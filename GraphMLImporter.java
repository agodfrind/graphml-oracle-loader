import java.io.*;
import java.util.*;
import java.time.*;
import java.sql.*;
import oracle.jdbc.*;
import javax.xml.stream.*;
import javax.xml.stream.events.*;

public class GraphMLImporter {

  static boolean isNeo4J = false;
  static boolean buildTopology = true;
  static boolean makeUppercase = true;

  static Instant start;
  static Instant previous;

  static Map<String, String> keyIdMap = new HashMap<>();
  static Map<String, Integer> keyTypeMap = new HashMap<>();

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
        case "-o" : case "--topology":  buildTopology = args[i+1].toUpperCase().equals("YES") ? true : false; break;
        case "-U" : case "--uppercase": makeUppercase = args[i+1].toUpperCase().equals("YES") ? true : false; break;
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
      System.out.println ("  -a/--action    <action>:           [CREATE] or APPEND or REPLACE or TRUNCATE");
      System.out.println ("  -t/--format    <format>:           NEO4J or [TINKERPOP]");
      System.out.println ("  -b/--batchsize <batchsize>:        commit interval (0 = only commit at the end)");
      System.out.println ("  -s/--skipItem  <skipItems>:        number of items to skip (0 = nothing to skip)");
      System.out.println ("  -n/--numItems  <numItems>:         number of items to read (0 = until the ends)");
      System.out.println ("  -o/--topology  YES/NO:             [YES]: populate topology tables / NO: do not populate");
      System.out.println ("  -U/--uppercase YES/NO:             [YES]: make all property names and labels uppercase");
      System.exit(0);
    }

    isNeo4J = format.toUpperCase().equals("NEO4J") ? true : false;

    // Make sure we use a US locale. This is so that the database interprets '.' as decimal points
    // when inserting numbers as strings
    Locale.setDefault(Locale.US);

    System.out.println("Connecting to Database "+jdbcUrl);
    Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
    conn.setAutoCommit(false);
    switch (action.toUpperCase()) {
      case ("CREATE"):
        createGraph(conn, graphname.toUpperCase());
        break;
      case ("TRUNCATE"):
        clearGraph(conn, graphname.toUpperCase());
        break;
      case ("REPLACE"):
        dropGraph(conn, graphname.toUpperCase());
        createGraph(conn, graphname.toUpperCase());
        break;
    }
    processFile (filename, conn, graphname, batchsize, skipItems, numItems);
  }

  static void createGraph(Connection conn, String graphname)
  throws Exception {
    DatabaseMetaData md = conn.getMetaData();
    ResultSet t = md.getTables(null, null, graphname+"VT$", null);
    if (t.next()) {
      // Graph tables already exists
      System.out.println ("PG graph "+graphname.toUpperCase()+ " already exists");
      System.out.println ("  Use '-a append' to add to the existing graph");
      System.out.println ("  Use '-a truncate' to clear the existing graph before importing");
      System.out.println ("  Use '-a replace' to drop and re-create the graph");
      System.exit(0);
    }
    else {
      // Graph does not exist yet
      System.out.println ("Creating PG graph "+graphname.toUpperCase());
      String createPG = "BEGIN OPG_APIS.CREATE_PG(:1, DOP=>8, NUM_HASH_PTNS=>8, OPTIONS=>'SKIP_INDEX=T'); END;";
      CallableStatement cs = conn.prepareCall(createPG);
      cs.setString(1, graphname);
      cs.execute();

      // Set all tables to nologging
      conn.prepareStatement("alter table "+ graphname + "VT$ nologging").execute();
      conn.prepareStatement("alter table "+ graphname + "GE$ nologging").execute();
      conn.prepareStatement("alter table "+ graphname + "SS$ nologging").execute();
      conn.prepareStatement("alter table "+ graphname + "GT$ nologging").execute();
      conn.prepareStatement("alter table "+ graphname + "IT$ nologging").execute();
      conn.prepareStatement("alter table "+ graphname + "VD$ nologging").execute();
    }
  }

  static void clearGraph(Connection conn, String graphname)
  throws Exception {
    System.out.println ("Clearing PG graph "+graphname.toUpperCase());
    CallableStatement cs = conn.prepareCall("begin opg_apis.clear_pg(:1); end;");
    cs.setString(1, graphname);
    cs.execute();
    System.out.println ("Graph cleared");
  }

  static void dropGraph(Connection conn, String graphname)
  throws Exception {
    System.out.println ("Dropping PG graph "+graphname.toUpperCase());
    CallableStatement cs = conn.prepareCall("begin opg_apis.drop_pg(:1); end;");
    cs.setString(1, graphname);
    try {
      cs.execute();
    } catch (SQLException e) {
      if (e.getErrorCode() != 942)
        throw e;
    }
  }

  static void processFile(
    String filename, Connection conn, String graphname, int batchsize, long skipItems, long numItems
  ) throws Exception {
    System.out.println ("Processing file "+filename);
    XMLInputFactory inputFactory = XMLInputFactory.newInstance();
    XMLStreamReader xmlReader = inputFactory.createXMLStreamReader(new FileInputStream(filename));

    // Vertex Data
    long vid = 0;
    String vLabel = null;
    Map<String, String> vProps = null;
    boolean inV = false;
    Long vCounter = 0L;

    // Edge Data
    long eid = 0;
    long svid = 0;
    long dvid = 0;
    String eLabel = null;
    Map<String, String> eProps = null;
    boolean inE = false;
    Long eCounter = 0L;

    // Insert statements
    PreparedStatement vInsert = conn.prepareStatement(
      "INSERT INTO " + graphname + "VT$ (vid,vl,k,t,v,vn) VALUES (?,?,?,?,?,?)"
    );
    PreparedStatement eInsert = conn.prepareStatement(
      "INSERT INTO " + graphname + "GE$ (eid,svid,dvid,el,k,t,v,vn) VALUES (?,?,?,?,?,?,?,?)"
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

    start = Instant.now();
    previous = start;
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
            int attributeTypeCode = 1; // if not data type, assume string
            keyIdMap.put(id, attributeName);
            // Map the data types to the code in the Oracle PG graph format.
            if (attributeType != null)
              switch (attributeType) {
                case "string":  attributeTypeCode = 1; break;
                case "int":     attributeTypeCode = 2; break;
                case "integer": attributeTypeCode = 2; break;
                case "float":   attributeTypeCode = 3; break;
                case "double":  attributeTypeCode = 4; break;
                case "boolean": attributeTypeCode = 6; break;
                case "long":    attributeTypeCode = 7; break;
                default:        attributeTypeCode = 1; break;
              }
            keyTypeMap.put(id, attributeTypeCode);
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

          // Process <data> element (vertex/edge property or label)
          // <data key="Country">United States</data>
          // <data key="labelV">Customer</data>
          case "data":
            String key = xmlReader.getAttributeValue(null, "key");
            String value = xmlReader.getElementText();
            if (inV) {
              if (key.equals(vLabelKey))
                if (isNeo4J)
                  // Remove leading ":" from vertex labels
                  vLabel = value.substring(1);
                else
                  vLabel = value;
              else
                vProps.put(key,value);
            } else if (inE) {
              if (key.equals(eLabelKey))
                eLabel = value;
              else
                eProps.put(key,value);
            }
            break;
        }

      // Process end element
      } else if (xmlEvent.equals(XMLEvent.END_ELEMENT)) {
        String xmlTag = xmlReader.getName().getLocalPart();
        switch (xmlTag) {

          // Process </node> element
          case "node":
            if (skipCounter-- <= 0) {
              if (skipCounter == -1 && skipItems > 0) {
                System.out.println("... done skipping");
                start = Instant.now();
                previous = previous;
              }
              numCounter++;
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

          // Process </edge> element
          case "edge":
            if (skipCounter-- <= 0) {
              if (skipCounter == -1 && skipItems > 0) {
                System.out.println("... done skipping");
                start = Instant.now();
                previous = previous;
              }
              numCounter++;
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

    // Log total import time
    System.out.println ("Graph "+graphname.toUpperCase()+" imported in " +
      ((Instant.now().toEpochMilli()-start.toEpochMilli())/1000) + " sec ");

    // Build topology and/or indexes
    Instant startFinish = Instant.now();
    String finishPG;
    if (buildTopology) {
      System.out.println ("Creating topology and indexes ...");
      finishPG = "BEGIN OPG_APIS.MIGRATE_PG_TO_CURRENT(:1, DOP=>8); END;";
    }
    else
    {
      System.out.println ("Creating indexes ...");
      finishPG = "BEGIN OPG_APIS.CREATE_PG(:1, DOP=>8, OPTIONS=>'SKIP_TABLE=T'); END;";
    }
    CallableStatement cs = conn.prepareCall(finishPG);
    cs.setString(1, graphname);
    cs.execute();
    System.out.println ("...completed in " +
      ((Instant.now().toEpochMilli()-startFinish.toEpochMilli())/1000) + " sec ");

    // Log final time
    System.out.println ("Graph "+graphname.toUpperCase()+" processed in " +
      ((Instant.now().toEpochMilli()-start.toEpochMilli())/1000) + " sec ");
    System.out.println ("- "+vCounter+" vertices");
    System.out.println ("- "+eCounter+" edges");
  }

  // Write a vertex to database
  static void writeVertex(PreparedStatement vInsert, Long vid, String vLabel, Map<String, String> vProps)
  throws Exception {
    if (vProps.size() > 0) {
      // Write all properties
      for (Map.Entry<String, String> e : vProps.entrySet()) {
        String k = e.getKey();
        String v = e.getValue();
        if (makeUppercase) {
          k = k.toUpperCase();
          vLabel = vLabel.toUpperCase();
        }
        int t = keyTypeMap.getOrDefault(k,1);
        vInsert.setLong   (1, vid);                   // VID (vertex id)
        vInsert.setString (2, vLabel);                // VL  (label)
        vInsert.setString (3, k);                     // K   (property name)
        vInsert.setLong   (4, t);                     // T   (data type)
        vInsert.setString (5, v);                     // V   (string value)
        if (t==2|t==3||t==4||t==7)                    // Is this a numeric value ?
          vInsert.setString (6, v);                   // VN  (numeric value)
        else
          vInsert.setString (6, null);                // VN  (numeric value) set to NULL
        vInsert.addBatch();
      }
    } else {
      // Write empty property (when a vertex has no properties)
      vInsert.setLong   (1, vid);                     // VID (vertex id)
      vInsert.setString (2, vLabel);                  // VL  (label)
      vInsert.setString (3, null);                    // K   (property name)
      vInsert.setString (4, null);                    // T   (data type)
      vInsert.setString (5, null);                    // V   (string value)
      vInsert.setString (6, null);                    // VN  (numeric value) set to NULL
      vInsert.addBatch();
    }
  }

  // Write an edge to database
  static void writeEdge(PreparedStatement eInsert, Long eid, String eLabel, Long svid, Long dvid, Map<String, String> eProps)
  throws Exception {
    if (eProps.size() > 0) {
      // Write all properties
      for (Map.Entry<String, String> e : eProps.entrySet()) {
        String k = e.getKey();
        String v = e.getValue();
        if (makeUppercase) {
          k = k.toUpperCase();
          eLabel = eLabel.toUpperCase();
        }
        int t = keyTypeMap.getOrDefault(k,1);
        eInsert.setLong   (1, eid);                   // EID  (vertex id)
        eInsert.setLong   (2, svid);                  // SVID (source vertex id)
        eInsert.setLong   (3, dvid);                  // DVID (destination vertex id)
        eInsert.setString (4, eLabel);                // EL   (label)
        eInsert.setString (5, k);                     // K    (property name)
        eInsert.setLong   (6, t);                     // T    (data type)
        eInsert.setString (7, v);                     // V    (string value)
        if (t==2|t==3||t==4||t==7)                    // Is this a numeric value ?
          eInsert.setString (8, v);                   // VN   (numeric value)
        else
          eInsert.setString (8, null);                // VN   (numeric value) set to NULL
        eInsert.addBatch();
      }
    } else {
      // Write empty property (when an edge has no properties)
      eInsert.setLong   (1, eid);                   // EID  (vertex id)
      eInsert.setLong   (2, svid);                  // SVID (source vertex id)
      eInsert.setLong   (3, dvid);                  // DVID (destination vertex id)
      eInsert.setString (4, eLabel);                // EL   (label)
      eInsert.setString (5, null);                  // K    (property name)
      eInsert.setString (6, null);                  // T    (data type)
      eInsert.setString (7, null);                  // V    (string value)
      eInsert.setString (8, null);                  // VN   (numeric value) set to NULL
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
        "in " + ( now.toEpochMilli()-previous.toEpochMilli() ) + " ms " +
        "("  + ( (float)batchsize/(now.toEpochMilli()-previous.toEpochMilli())*1000) + " per second) " +
        "accumulated: " + (now.toEpochMilli()-start.toEpochMilli()) + " ms " +
        " "  + ( (float)(vCounter+eCounter)/(now.toEpochMilli()-start.toEpochMilli())*1000) + " per second) "
      );
      /*
      System.out.println(
        "Memory use:" +
        "\tFree MB:" + Runtime.getRuntime().freeMemory()/1024/1024 +
        "\tUsed MB:" + Runtime.getRuntime().totalMemory()/1024/1024 +
        "\tMax MB:"  + Runtime.getRuntime().maxMemory()/1024/1024
      );
      */
      conn.commit();
      previous = now;
    }
  }

}