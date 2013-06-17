package test;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

public class App8 {
    public static final String CHARSET = "UTF-8";

  public static void main(String[] args) throws Exception{
	if (args.length != 5) {
	  System.err.println("java App KSNAME CFNAME KEY COL ValuFile");
	  System.exit(1);
	}

	String ksName = args[0];
	String cfName = args[1];
	String keyName = args[2];
	String clName =  args[3];
	String fileName =  args[4];

	TTransport tr = new TFramedTransport(new TSocket("localhost", 9160));
	TProtocol proto = new TBinaryProtocol(tr);
	
	Cassandra.Client client = new Cassandra.Client(proto);
	tr.open();
	String keyspace = ksName;
	client.set_keyspace(keyspace);
	
	String columnFamily = cfName;
	ColumnParent columnParent = new ColumnParent(columnFamily);
	long timestamp = System.currentTimeMillis();
	ColumnPath path = new ColumnPath(columnFamily);
  


	// read entire row
	SlicePredicate predicate = new SlicePredicate();
       	SliceRange sliceRange = new SliceRange(toByteBuffer(""), toByteBuffer(""), false, 10);
	//predicate.setSlice_range(sliceRange);
	predicate.setColumn_names(Arrays.asList(toByteBuffer("comment")));
	
	//Get all keys
	KeyRange keyRange = new KeyRange(2);
	keyRange.setStart_key(new byte[0]);
	keyRange.setEnd_key(new byte[0]);
	
	List<KeySlice> keySlices = client.get_range_slices(columnParent, predicate, keyRange, ConsistencyLevel.ONE);
	System.out.println(keySlices.size());
	System.out.println(keySlices);
	for (KeySlice ks : keySlices) {
	    System.out.println(new String(ks.getKey()));
	}    
	tr.close();
}
    
    public static ByteBuffer toByteBuffer(String value) 
	throws UnsupportedEncodingException
    {
	return ByteBuffer.wrap(value.getBytes("UTF-8"));
    }
    public static String toString(ByteBuffer buffer)
        throws UnsupportedEncodingException
    {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return new String(bytes, "UTF-8");
    }

}