package test;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Random;
import java.text.DecimalFormat;

import org.apache.cassandra.stress.util.CassandraClient;
import org.apache.cassandra.thrift.AuthenticationException;
import org.apache.cassandra.thrift.AuthorizationException;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.KsDef;
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

public class App6 {

	public static byte[] readAll(String filename) throws IOException {
		File f = new File(filename);

		FileInputStream fis = new FileInputStream(f);
		int length = (int) f.length();
		byte[] buf = new byte[length];
		int read = 0;
		int offset = 0;
		while ((read = fis.read(buf, offset, length - offset)) > 0)
			;
/*
		Random r = new Random();
		Integer itg = r.nextInt(20) + 40;
		if (itg > 45) {
			byte[] buf2 = toBytes(itg);

			byte[] result = new byte[length + 4];
			for (int i = 0; i < length; i++) {
				result[i] = buf[i];
			}
			for (int i = 0; i < 4; i++) {
				result[length + i] = buf2[i];
			}
			return result;
		} else
		*/
			return buf;
	}

  public static byte[] toBytes(int a) {
        byte[] bs = new byte[4];
        bs[3] = (byte) (0x000000ff & (a));
        bs[2] = (byte) (0x000000ff & (a >>> 8));
        bs[1] = (byte) (0x000000ff & (a >>> 16));
        bs[0] = (byte) (0x000000ff & (a >>> 24));
        return bs;
    }

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
		try {
			client.set_keyspace(keyspace);
		} catch (InvalidRequestException e) {

			KsDef ks_def = new KsDef(keyspace, "SimpleStrategy", null);
			CfDef cf_def = new CfDef(keyspace, cfName);
			cf_def.setComparator_type("UTF8Type");
			cf_def.default_validation_class = "UTF8Type";
			cf_def.key_validation_class =  "UTF8Type";
			ks_def.addToCf_defs(cf_def);
			Map strategy_options = new HashMap<String, String>();
			strategy_options.put("replication_factor", "2");
			ks_def.setStrategy_options(strategy_options);

			client.system_add_keyspace(ks_def);
			client.set_keyspace(keyspace);
		}

		// record id
		// String key_user_id = keyName;
		DecimalFormat dformat = new DecimalFormat("000");
		int i;
		for (i = 0; i < 100; i++) {
			int a = i + 1;
			String key_user_id = dformat.format(a);
			System.out.println(key_user_id);
			String columnFamily = cfName;

			Column fileColumn = new Column(ByteBuffer.wrap(clName.getBytes()));
			fileColumn.setValue(ByteBuffer.wrap(readAll(fileName)));
			/*
			 * byte [] Bytes = new byte[1024*1024*50]; Random r = new Random();
			 * r.nextBytes(Bytes); fileColumn.setValue(ByteBuffer.wrap(Bytes));
			 */
			ColumnParent columnParent = new ColumnParent(columnFamily);

			long timestamp = System.currentTimeMillis();
			fileColumn.setTimestamp(timestamp);
			client.insert(ByteBuffer.wrap(key_user_id.getBytes()),
					columnParent, fileColumn, ConsistencyLevel.ALL);
		}
		tr.close();
	}
}