package com.krickert.mongodb.oplog;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.bson.types.BSONTimestamp;
import org.joda.time.DateTime;

import com.mongodb.BasicDBObject;
import com.mongodb.Bytes;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

public class OpLogReader implements Runnable {

  private static final Logger log = Logger.getLogger(OpLogReader.class);

  private final Mongo mongoConnection;
  private final BlockingQueue<OplogLine> queue;

  public OpLogReader(Mongo m, BlockingQueue<OplogLine> queue) {
    this.mongoConnection = m;
    this.queue = queue;
  }

  @Override
  public void run() {
    DB local = mongoConnection.getDB("local");

    DBCollection oplog = local.getCollection("oplog.$main");

    DBObject last = null;
    {
      DBCursor lastCursor = oplog.find().sort(new BasicDBObject("$natural", -1)).limit(1);
      if (!lastCursor.hasNext()) {
        log.fatal("no oplog configured for this connection.  Please restart mongo with the --master option.");
        return;
      }
      last = lastCursor.next();
    }

    BSONTimestamp ts = (BSONTimestamp) last.get("ts");
    log.info("starting point: " + ts);

    while (true) {
      log.debug("ts: " + ts);
      DBCursor cursor = oplog.find(new BasicDBObject("ts", new BasicDBObject("$gt", ts)));
      cursor.addOption(Bytes.QUERYOPTION_TAILABLE);
      cursor.addOption(Bytes.QUERYOPTION_AWAITDATA);
      ts = insertOplogInQueue(cursor, ts);
    }
  }

  protected BSONTimestamp insertOplogInQueue(DBCursor cursor, BSONTimestamp ts) {

    while (cursor.hasNext()) {
      DBObject x = cursor.next();
      ts = (BSONTimestamp) x.get("ts");
      OplogLine line = parseLogLine(ts, x);
      if (log.isDebugEnabled()) {
        log.debug(line);
      }
      try {
        if (!queue.offer(line, 10, TimeUnit.SECONDS)) {
          log.error("Failed to insert oplog into queue.  Queue size: [" + queue.size() + "] while trying to insert " + line);
        }
      } catch (InterruptedException e) {
        log.info("oplog offer was interrupted.  exiting oplog reader.", e);
      }
    }
    return ts;
  }

  protected OplogLine parseLogLine(BSONTimestamp ts, DBObject x) {
    DateTime timestamp = new DateTime(ts.getTime() * 1000l);
    MongoOplogOperation operation = MongoOplogOperation.find((String) x.get("op"));
    String nameSpace = (String) x.get("ns");
    BasicDBObject data = (BasicDBObject) x.get("o");
    OplogLine line = new OplogLine(operation, timestamp, nameSpace, data.toString());
    return line;
  }
}
