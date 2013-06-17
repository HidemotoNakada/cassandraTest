package test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.Map.Entry;
import java.nio.ByteBuffer;
import java.util.Random;
import java.text.DecimalFormat;
import org.apache.cassandra.thrift.AuthenticationException;
import org.apache.cassandra.thrift.AuthorizationException;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import java.io.*;

public class App5 {
	public static final String CHARSET = "UTF-8";

	public static void main(String[] args) throws Exception {
		if (args.length != 5) {
			System.err.println("java App KSNAME CFNAME KEY COL ValuFile");
			System.exit(1);
		}

		String ksName = args[0];
		String cfName = args[1];
		String keyName = args[2];
		String clName = args[3];
		String fileName = args[4];

		TTransport tr = new TFramedTransport(new TSocket("127.0.0.1", 9160));
		TProtocol proto = new TBinaryProtocol(tr);

		Cassandra.Client client = new Cassandra.Client(proto);
		tr.open();
		String keyspace = ksName;
		client.set_keyspace(keyspace);

		// record id
		String key_user_id = keyName;
		String columnFamily = cfName;

		ColumnParent columnParent = new ColumnParent(columnFamily);
		long timestamp = System.currentTimeMillis();

		Column nameColumn = new Column(toByteBuffer("fname"));
		// nameColumn.setValue(toByteBuffer("Chris Goffinet"));
		// nameColumn.setTimestamp(timestamp);
		// client.insert(toByteBuffer(key_user_id), columnParent, nameColumn,
		// ConsistencyLevel.ONE);

		ColumnPath path = new ColumnPath(columnFamily);

		// read entire row
		SlicePredicate predicate = new SlicePredicate();
		SliceRange sliceRange = new SliceRange(toByteBuffer(""),
				toByteBuffer(""), false, 10);
		predicate.setSlice_range(sliceRange);
		//predicate.setColumn_names(Arrays.asList(toByteBuffer(clName)));

		/*
		 * List<ByteBuffer> rowKeys =new ArrayList(Arrays.asList()); String z
		 * ="100"; rowKeys.add(toByteBuffer(z));
		 */

		DecimalFormat dformat = new DecimalFormat("000");
		List<ByteBuffer> rowKeys = new ArrayList<ByteBuffer>();
		for (int i = 1; i <= 70; i++) 
			rowKeys.add(toByteBuffer(dformat.format(i)));

		// Map<ByteBuffer, List<ColumnOrSuperColumn>> results =
		// client.mymultiget_slice(rowKeys, columnParent,
		// predicate,ConsistencyLevel.ONE);

		//String udf = "{\"classname\": \"org.apache.cassandra.db.ExternalProcessUDF\", \"commands\": [\"wc\"]}";
		
		String udf = "{\"classname\": \"test.ExternalProcessUDF\", \"commands\": [\"wc\"], \"jar\": \"/home/nakada/workspace/cassandraTest/udf.jar\" }";
		
		//String udf = null;
		Map<ByteBuffer, List<ColumnOrSuperColumn>> results = client
				.multiget_slice_udf(rowKeys, columnParent, predicate,
						ConsistencyLevel.ONE, udf);
/*
		Map<ByteBuffer, List<ColumnOrSuperColumn>> results = client
				.multiget_slice(rowKeys, columnParent, predicate,
				ConsistencyLevel.ONE);
*/
		
		//System.out.println("----------");
		for (Entry<ByteBuffer, List<ColumnOrSuperColumn>> e : results
				.entrySet()) {
			ByteBuffer key = e.getKey();
			List<ColumnOrSuperColumn> coscs = e.getValue();
			for (ColumnOrSuperColumn cosc : coscs) {
				Column column = cosc.getColumn();
				System.out.printf("%s[%s]=%s%n",
						toString(key), 
						new String(column.getName(),
						CHARSET), new String(column.getValue(), CHARSET));
			}
			//System.out.println("----------");

		}

		/*
		 * List<ColumnOrSuperColumn> results = client.multiget_slice(rowKeys,
		 * columnParent, predicate, ConsistencyLevel.ONE); for
		 * (ColumnOrSuperColumn result : results) { Column columns =
		 * result.column; PrintStream ps = new PrintStream("hoge.txt");
		 * System.setOut(ps); System.out.println(toString(columns.name) + " -> "
		 * + toString(columns.value)); ps.close(); }
		 */

		tr.close();
	}

	public static ByteBuffer toByteBuffer(String value)
			throws UnsupportedEncodingException {
		return ByteBuffer.wrap(value.getBytes("UTF-8"));
	}

	public static String toString(ByteBuffer buffer)
			throws UnsupportedEncodingException {
		byte[] bytes = new byte[buffer.remaining()];
		buffer.get(bytes);
		return new String(bytes, "UTF-8");
	}

}