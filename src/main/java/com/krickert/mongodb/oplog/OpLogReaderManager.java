package com.krickert.mongodb.oplog;

import java.net.UnknownHostException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.mongodb.Mongo;
import com.mongodb.MongoException;

public class OpLogReaderManager {

  private static final Logger log = Logger.getLogger(OpLogReaderManager.class);

  public static void main(String args[]) {
    try {
      Mongo mongo = new Mongo("localhost");
      BlockingQueue<OplogLine> queue = new LinkedBlockingQueue<OplogLine>();
      OpLogReader reader = new OpLogReader(mongo, queue);
      ExecutorService executor = Executors.newSingleThreadExecutor();
      executor.execute(reader);
      executor.shutdown();
      try {
        while (!executor.isTerminated()) {
          OplogLine line = queue.poll(30, TimeUnit.SECONDS);
          log.info("Line info: " + line);
        }
      } catch (InterruptedException e) {
        log.info("time to go.");
        executor.shutdownNow();
      }
    } catch (UnknownHostException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (MongoException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

  }

}
