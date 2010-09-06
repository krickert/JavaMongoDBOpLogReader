/**
 * Simple Example that shows creating a people (id:number, name:string,
 * gender:string) collection and then adding, finding, updating and deleting.
 * You need to download and install MongoDB (www.mongodb.org) and run the
 * server. You’ll also need to have the mongo-1.2.jar in your class path.
 */

package com.krickert.mongodb.oplog;

import java.net.UnknownHostException;
import java.util.regex.Pattern;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.Mongo;
import com.mongodb.MongoException;

/**
 * This is just some simple mongo operations to test the mongo oplog reader on a
 * live mongodb
 * 
 * This code was taken from this website:
 * <link>http://vsbabu.org/mt/archives/2010
 * /03/02/simple_mongodbjava_example.html</link>
 */
public class MongoLocalhostCRUD {

  public static void main(String[] args) {

    try {
      Mongo m = new Mongo("localhost", 27017);
      DB db = m.getDB("sampledb");
      DBCollection coll = db.getCollection("people");

      // clear records if any
      DBCursor cur = coll.find();
      while (cur.hasNext())
        coll.remove(cur.next());

      // create a unique ascending index on id;
      // doesn’t seem to work? it works from python and javascript
      coll.ensureIndex(new BasicDBObject("id", 1).append("unique", true));
      coll.createIndex(new BasicDBObject("name", 1));
      coll.insert(makePersonDocument(6655, "James", "male"));
      coll.insert(makePersonDocument(6797, "Bond", "male"));
      coll.insert(makePersonDocument(6643, "Cheryl", "female"));
      coll.insert(makePersonDocument(7200, "Scarlett", "female"));
      coll.insert(makePersonDocument(6400, "Jacks", "male"));
      System.out.println("Total Records : " + coll.getCount());

      cur = coll.find();
      printResults(cur, "Find All Records");

      cur = coll.find(new BasicDBObject("id", 6655));
      printResults(cur, "Find id = 6655");

      cur = coll.find(new BasicDBObject().append("id", new BasicDBObject("$lte", 6700)));
      printResults(cur, "Find id <= 6700");

      cur = coll.find(new BasicDBObject().append("id", new BasicDBObject("$lte", 6700)).append("gender", "male"));
      printResults(cur, "Find id <= 6700 and gender = male");

      cur = coll.find(new BasicDBObject().append("name", Pattern.compile("^ja.*?s$", Pattern.CASE_INSENSITIVE))).sort(
          new BasicDBObject("name", -1));
      printResults(cur, "Find name like Ja%s and sort reverse by name");

      cur = coll.find(new BasicDBObject().append("gender", "female")).sort(new BasicDBObject("id", -1)).limit(2);
      printResults(cur, "Get top 2 (by id) ladies");

      // let us reduce every body’s phone numbers by 10; add Sir to males, Mme
      // to ladies
      cur = coll.find();
      while (cur.hasNext()) {
        BasicDBObject set = new BasicDBObject("$inc", new BasicDBObject("id", -10));
        if ("male".equals(cur.next().get("gender")))
          set.append("$set", new BasicDBObject("name", "Sir ".concat((String) cur.curr().get("name"))));
        else
          set.append("$set", new BasicDBObject("name", "Mme ".concat((String) cur.curr().get("name"))));
        coll.update(cur.curr(), set);
      }
      cur = coll.find();
      printResults(cur, "All, after id and name update");

    } catch (UnknownHostException ex) {
      ex.printStackTrace();
    } catch (MongoException ex) {
      ex.printStackTrace();
    }

  }

  private static void printResults(DBCursor cur, String message) {
    System.out.println("<<<<<<<<<< " + message + " >>>>>>>>>>>>");
    while (cur.hasNext()) {
      System.out.println(cur.next().get("id") + "," + cur.curr().get("name") + "," + cur.curr().get("gender"));
    }
  }

  private static BasicDBObject makePersonDocument(int id, String name, String gender) {
    BasicDBObject doc = new BasicDBObject();
    doc.put("id", id);
    doc.put("name", name);
    doc.put("gender", gender);
    return doc;
  }

}
/**
 * <pre>
 * Total Records : 5
 * <<<<<<<<<< Find All Records >>>>>>>>>>>>
 * 6655,James,male
 * 6797,Bond,male
 * 6643,Cheryl,female
 * 7200,Scarlett,female
 * 6400,Jacks,male
 * <<<<<<<<<< Find id = 6655 >>>>>>>>>>>>
 * 6655,James,male
 * <<<<<<<<<< Find id <= 6700 >>>>>>>>>>>>
 * 6400,Jacks,male
 * 6643,Cheryl,female
 * 6655,James,male
 * <<<<<<<<<< Find id <= 6700 and gender = male >>>>>>>>>>>>
 * 6400,Jacks,male
 * 6655,James,male
 * <<<<<<<<<< Find name like Ja%s and sort reverse by name >>>>>>>>>>>>
 * 6655,James,male
 * 6400,Jacks,male
 * <<<<<<<<<< Get top 2 (by id) ladies >>>>>>>>>>>>
 * 7200,Scarlett,female
 * 6643,Cheryl,female
 * <<<<<<<<<< All, after id and name update >>>>>>>>>>>>
 * 6645,Sir James,male
 * 6787,Sir Bond,male
 * 6633,Mme Cheryl,female
 * 7190,Mme Scarlett,female
 * 6390,Sir Jacks,male
 * </pre>
 */
