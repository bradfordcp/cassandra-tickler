import com.datastax.driver.core.*;

import java.util.List;

/**
 * The tickler performs reads at a Consistency Level of ALL across a partition. This forces a read repair across all
 * replicas.
 *
 * Based on the Python cassTicker at https://github.com/ckalantzis/cassTickler/blob/master/tickler.py
 */
public class Tickler {
  public static void main(String[] args) {
    if (args.length != 4) {
      System.err.println("Invalid command");
      System.err.println("java -jar Tickler-1.0-SNAPSHOT.jar keyspace table contact_point delay");
      System.err.println("  keyspace      - Name of the keyspace where the table resides");
      System.err.println("  table         - Name of the table to repair");
      System.err.println("  contact_point - IP address of a node in the cluster");
      System.err.println("  delay         - Delay between repair queries (in milliseconds)");
    } else {
      String keyspace = args[0];
      String table = args[1];
      String contact_point = args[2];
      long delay = Long.parseLong(args[3]);

      Cluster cluster = Cluster.builder().addContactPoint(contact_point).build();
      Session session = cluster.connect();

      try {
        List<ColumnMetadata> partitionKeys = cluster.getMetadata().getKeyspace(keyspace).getTable(table).getPartitionKey();

        StringBuilder getPartitionKeyValuesRawQuery = new StringBuilder();
        getPartitionKeyValuesRawQuery.append("SELECT ");

        StringBuilder repairPartitionRawQuery = new StringBuilder();
        repairPartitionRawQuery.append("SELECT COUNT(1) FROM ").append(keyspace).append(".").append(table).append(" WHERE ");

        for (ColumnMetadata partitionKey : partitionKeys) {
          getPartitionKeyValuesRawQuery.append(partitionKey.getName());
          repairPartitionRawQuery.append(partitionKey.getName()).append(" = :").append(partitionKey.getName());

          if (partitionKey != partitionKeys.get(partitionKeys.size() - 1)) {
            getPartitionKeyValuesRawQuery.append(",");
            repairPartitionRawQuery.append(" AND ");
          }
          getPartitionKeyValuesRawQuery.append(" ");
        }
        getPartitionKeyValuesRawQuery.append("FROM ").append(keyspace).append(".").append(table).append(";");
        repairPartitionRawQuery.append(";");

        PreparedStatement getPartitionKeyValues = session.prepare(getPartitionKeyValuesRawQuery.toString())
            .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
        PreparedStatement repairPartition = session.prepare(repairPartitionRawQuery.toString())
            .setConsistencyLevel(ConsistencyLevel.ALL);

        ResultSet partitionKeyRows = session.execute(getPartitionKeyValues.bind().setFetchSize(1000));
        long rowCount = 0;
        for (Row row : partitionKeyRows) {
          rowCount += 1;

          BoundStatement repairStatement = repairPartition.bind();
          CodecRegistry codecRegistry = cluster.getConfiguration().getCodecRegistry();
          for (ColumnDefinitions.Definition partitionKey : row.getColumnDefinitions()) {
            repairStatement = repairStatement.set(partitionKey.getName(), row.getObject(partitionKey.getName()), codecRegistry.codecFor(partitionKey.getType()));
          }

          session.execute(repairStatement);
          Thread.sleep(delay);
          if (rowCount % 1000 == 0) {
            System.out.println(rowCount);
          }
        }
        System.out.println("Repair of table " + table + " complete");
        System.out.println("" + rowCount + " rows read and repaired");
      } catch (Exception ex) {
        ex.printStackTrace(System.err);
      } finally {
        session.close();
        cluster.close();
      }
    }
  }
}
