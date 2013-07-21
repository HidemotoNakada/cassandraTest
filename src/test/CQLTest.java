package test;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class CQLTest {

	static {
    try {
			Class.forName("org.apache.cassandra.cql.jdbc.CassandraDriver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	final String keyspaceName = "testks2";
	final String dropKS = "DROP KEYSPACE " + keyspaceName + ";";
	final String createKS = "CREATE KEYSPACE "+ keyspaceName + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 2};" ;
	final String useKS = "USE " + keyspaceName +";";
  final String createTable =
	"CREATE TABLE simpledata (" + 
	"    id      text PRIMARY KEY, " + 
	"	   content text              " + 
	") with comment = 'simple text data with id';";
  final String insert = "INSERT INTO simpledata (id, content)    VALUES (?, ?);";
  final String select = "SELECT id, content FROM simpledata";

  
  final String testString = ("aaaa bbb\n cccc dddd \n eee\n");
  
  
	public void prepare(Connection con) throws Exception {
    Statement stmt = con.createStatement();
		try {
    	stmt.execute(dropKS);
    } catch (Exception e) {/*ignore*/}
    stmt.execute(createKS);
    stmt.execute(useKS);
    stmt.execute(createTable);;

	}

	public void insert(Connection con, String id, String content) throws Exception {
    PreparedStatement statement = con.prepareStatement(insert);
    statement.setString(1, id);
    statement.setString(2, content);
    statement.execute();
  }

	public void test() throws Exception {
		Connection con = DriverManager.getConnection("jdbc:cassandra://localhost:9160/system");
		prepare(con);
		for (int i = 0; i < 100; i++) {
			insert(con, String.format("%03d", i), testString);
		}
		con.close();
	}

	public void selectTest() throws Exception {
		Connection con = DriverManager.getConnection("jdbc:cassandra://localhost:9160/system");

    Statement stmt = con.createStatement();
    stmt.execute(useKS);
    
    ResultSet rs = stmt.executeQuery(select);
    System.out.println(rs);
    
    while(rs.next()) {
    	String id      = rs.getString(1);
      String content = rs.getString(2);
      System.out.println(id + " " + content);
    }

    rs.close();		
		con.close();
	}
	
	
	/**
	 * @param args
	 * @throws dException 
	 */
	public static void main(String[] args) throws Exception {
		System.out.println(args);
		if (args.length == 1 && args[0].equals("select")) { 
			System.out.println("selecting");
			/* testing */
			(new CQLTest()).selectTest();	
		} else {
			/* prepare */
			(new CQLTest()).test();
		}
	}

}
