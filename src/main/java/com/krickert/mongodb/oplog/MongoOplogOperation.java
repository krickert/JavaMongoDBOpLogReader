package com.krickert.mongodb.oplog;

public enum MongoOplogOperation {
  NoOp("n"), Insert("i"), Update("u"), Delete("d"), Unknown("!");

  private final char statusCode;

  MongoOplogOperation(String statusCode) {
    this.statusCode = statusCode.charAt(0);
  }

  public char getStatusCode() {
    return statusCode;
  }

  public static MongoOplogOperation find(String oplogCode) {
    if (oplogCode == null || oplogCode.length() == 0) {
      return MongoOplogOperation.Unknown;
    }
    for (MongoOplogOperation value : MongoOplogOperation.values()) {
      if (value.getStatusCode() == oplogCode.charAt(0)) {
        return value;
      }
    }
    return MongoOplogOperation.Unknown;
  }
}
