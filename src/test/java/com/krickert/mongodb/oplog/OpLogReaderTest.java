/**
 * 
 */
package com.krickert.mongodb.oplog;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import junit.framework.TestCase;

import org.bson.types.BSONTimestamp;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;

import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

/**
 * @author krickert
 * 
 */
public class OpLogReaderTest extends TestCase {

  private BSONTimestamp ts;
  private DBObject logLine;
  private OpLogReader oplogReader;
  private DBCursor cursor;

  private BlockingQueue<OplogLine> queue;

  @Override
  public void setUp() {
    Mongo m = mock(Mongo.class);
    ts = mock(BSONTimestamp.class);
    when(ts.getTime()).thenReturn(1283805780);

    DBObject noOpLogData = null, insertLogData = null, updateLogData = null, deleteLogData = null;
    try {
      noOpLogData = createMockDBObject("{ }");
      insertLogData = createMockDBObject("{ \"name\" : \"id_1_unique_\" , \"ns\" : \"sampledb.people\" , \"key\" : { \"id\" : 1 , \"unique\" : true}}");
      updateLogData = createMockDBObject("{ \"$set\" : { \"name\" : \"Sir James\"}}");
      deleteLogData = createMockDBObject("{ \"_id\" : { \"$oid\" : \"4c85344f2cf052a76dbe59a0\"}}");
    } catch (IOException e) {
      fail("Failed making log data" + e);
    }

    logLine = mock(DBObject.class);
    when(logLine.get("op")).thenReturn("n", "i", "u", "d");
    when(logLine.get("ns")).thenReturn(null, "sampledb.system.indexes", "sampledb.people", "sampledb.people");
    when(logLine.get("o")).thenReturn(noOpLogData, insertLogData, updateLogData, deleteLogData);
    when(logLine.get("ts")).thenReturn(ts);

    // we're testing 4 lines of the oplog and then exiting
    cursor = mock(DBCursor.class);
    when(cursor.hasNext()).thenReturn(true, true, true, true, false);
    when(cursor.next()).thenReturn(logLine);

    queue = new LinkedBlockingQueue<OplogLine>();

    oplogReader = new OpLogReader(m, queue);

  }

  /**
   * Test method for {@link com.krickert.mongodb.oplog.OpLogReader#()}. Here,
   * we're going to insert the four operations into the queue and then poll for
   * them out of order to ensure they are coming in the right order. So the
   * operations will check: op1->insert op1->check insert op2->insert
   * op3->insert op2->check insert op4->insert op3->check insert op4->check
   * insert check that queue is null at this point
   */
  @Test
  public void testInsertOplogInQueue() {
    oplogReader.insertOplogInQueue(cursor, ts);
    assertEquals(
        "OplogLine [operation=NoOp, timestamp=2010-09-06T16:43:00.000-04:00, timestamp milliseconds=1283805780000, nameSpace=null, data={ }]",
        queue.poll().toString());
    oplogReader.insertOplogInQueue(cursor, ts);
    oplogReader.insertOplogInQueue(cursor, ts);
    assertEquals(
        queue.poll().toString(),
        "OplogLine [operation=Insert, timestamp=2010-09-06T16:43:00.000-04:00, timestamp milliseconds=1283805780000, nameSpace=sampledb.system.indexes, data={ \"ns\" : \"sampledb.people\" , \"name\" : \"id_1_unique_\" , \"key\" : { \"id\" : 1 , \"unique\" : true}}]");
    oplogReader.insertOplogInQueue(cursor, ts);
    assertEquals(
        queue.poll().toString(),
        "OplogLine [operation=Update, timestamp=2010-09-06T16:43:00.000-04:00, timestamp milliseconds=1283805780000, nameSpace=sampledb.people, data={ \"$set\" : { \"name\" : \"Sir James\"}}]");
    assertEquals(
        queue.poll().toString(),
        "OplogLine [operation=Delete, timestamp=2010-09-06T16:43:00.000-04:00, timestamp milliseconds=1283805780000, nameSpace=sampledb.people, data={ \"_id\" : { \"$oid\" : \"4c85344f2cf052a76dbe59a0\"}}]");

    assertNull(queue.poll());
  }

  /**
   * Test method for
   * {@link com.krickert.mongodb.oplog.OpLogReader#parseLogLine()}.
   */
  @Test
  public void testParseLogLine() {

    OplogLine line1 = oplogReader.parseLogLine(ts, logLine);
    assertEquals(line1.toString(),
        "OplogLine [operation=NoOp, timestamp=2010-09-06T16:43:00.000-04:00, timestamp milliseconds=1283805780000, nameSpace=null, data={ }]");
    OplogLine line2 = oplogReader.parseLogLine(ts, logLine);
    assertEquals(
        line2.toString(),
        "OplogLine [operation=Insert, timestamp=2010-09-06T16:43:00.000-04:00, timestamp milliseconds=1283805780000, nameSpace=sampledb.system.indexes, data={ \"ns\" : \"sampledb.people\" , \"name\" : \"id_1_unique_\" , \"key\" : { \"id\" : 1 , \"unique\" : true}}]");
    OplogLine line3 = oplogReader.parseLogLine(ts, logLine);
    assertEquals(
        line3.toString(),
        "OplogLine [operation=Update, timestamp=2010-09-06T16:43:00.000-04:00, timestamp milliseconds=1283805780000, nameSpace=sampledb.people, data={ \"$set\" : { \"name\" : \"Sir James\"}}]");
    OplogLine line4 = oplogReader.parseLogLine(ts, logLine);
    assertEquals(
        line4.toString(),
        "OplogLine [operation=Delete, timestamp=2010-09-06T16:43:00.000-04:00, timestamp milliseconds=1283805780000, nameSpace=sampledb.people, data={ \"_id\" : { \"$oid\" : \"4c85344f2cf052a76dbe59a0\"}}]");
  }

  /**
   * Create a DBObject from a set of BSON that we can expect
   * 
   * @param bsonData
   * @return the DBObject made for mocking
   * @throws IOException
   */
  private DBObject createMockDBObject(String bsonData) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    @SuppressWarnings("rawtypes")
    Map jsonData = mapper.readValue(bsonData, HashMap.class);
    BasicDBObjectBuilder dbBuilder = BasicDBObjectBuilder.start(jsonData);
    return dbBuilder.get();
  }
}
