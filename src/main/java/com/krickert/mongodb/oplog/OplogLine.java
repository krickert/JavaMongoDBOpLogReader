package com.krickert.mongodb.oplog;

import org.joda.time.DateTime;

public class OplogLine {
  private final MongoOplogOperation operation;
  private final DateTime timestamp;
  private final String nameSpace;
  private final String data;

  public OplogLine(MongoOplogOperation operation, DateTime timestamp, String nameSpace, String data) {
    super();
    this.operation = operation;
    this.timestamp = timestamp;
    this.nameSpace = nameSpace;
    this.data = data;
  }

  public MongoOplogOperation getOperation() {
    return operation;
  }

  public DateTime getTimestamp() {
    return timestamp;
  }

  public String getNameSpace() {
    return nameSpace;
  }

  public String getData() {
    return data;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("OplogLine [operation=");
    builder.append(operation);
    builder.append(", timestamp=");
    builder.append(timestamp);
    builder.append(", timestamp milliseconds=");
    builder.append(timestamp.getMillis());
    builder.append(", nameSpace=");
    builder.append(nameSpace);
    builder.append(", data=");
    builder.append(data);
    builder.append("]");
    return builder.toString();
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((data == null) ? 0 : data.hashCode());
    result = prime * result + ((nameSpace == null) ? 0 : nameSpace.hashCode());
    result = prime * result + ((operation == null) ? 0 : operation.hashCode());
    result = prime * result + ((timestamp == null) ? 0 : timestamp.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    OplogLine other = (OplogLine) obj;
    if (data == null) {
      if (other.data != null)
        return false;
    } else if (!data.equals(other.data))
      return false;
    if (nameSpace == null) {
      if (other.nameSpace != null)
        return false;
    } else if (!nameSpace.equals(other.nameSpace))
      return false;
    if (operation != other.operation)
      return false;
    if (timestamp == null) {
      if (other.timestamp != null)
        return false;
    } else if (!timestamp.equals(other.timestamp))
      return false;
    return true;
  }

}
