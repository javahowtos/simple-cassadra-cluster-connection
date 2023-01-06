import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;

import java.net.InetSocketAddress;

public class CassandraExample {
    public static void main(String[] args) {
        // Connect to the Cassandra cluster
        CqlSession session = CqlSession.builder()
                .withLocalDatacenter("datacenter1")
                .addContactPoint(new InetSocketAddress("127.0.0.1", 9042))
                .build();

        // Create a keyspace and table
        session.execute(
                "CREATE KEYSPACE IF NOT EXISTS example " +
                        "WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}");
        session.execute(
                "CREATE TABLE IF NOT EXISTS example.user (id INT PRIMARY KEY, name TEXT)");

        // Insert some data
        session.execute("INSERT INTO example.user (id, name) VALUES (1, 'Mary')");
        session.execute("INSERT INTO example.user (id, name) VALUES (2, 'Robert')");
        session.execute("INSERT INTO example.user (id, name) VALUES (3, 'Bob')");

        // Read the data back
        ResultSet resultSet = session.execute("SELECT * FROM example.user");
        for (Row row : resultSet) {
            System.out.format("id: %d, name: %s\n", row.getInt("id"), row.getString("name"));
        }

        // Close the session
        session.close();
    }
}
