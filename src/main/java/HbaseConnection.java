import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseConnection {
	public void establishConnection() throws IOException {
		Configuration config = HBaseConfiguration.create();		
		InputStream configInputStream = new FileInputStream(new File("hbase-conf.xml"));
		config.addResource(configInputStream); 
		try {
			HBaseAdmin.checkHBaseAvailable(config);
		}catch(Exception e) {
			System.out.println("Hbase is not available");			
		}
		Connection connection = null;
		try {
			connection = ConnectionFactory.createConnection(config);
		} catch (IOException e1) {
			System.out.println("Failed to establish connection");
			return;
		}
		TableName tableName = TableName.valueOf("TABLE_NAME");
		Table table  = null;
		try {
			table = connection.getTable(tableName);
		} catch (IOException e) {
			System.out.println("getTable Operation Failed");
			return;
		}
		Filter nodeIdPrefixFilter = new PrefixFilter(Bytes.toBytes("FILTER_PARAM_NAME"));
		Scan scan = new Scan();
		scan.setFilter(nodeIdPrefixFilter);
		
		ResultScanner scanner = table.getScanner(scan);
		for (Result result = scanner.next(); (result != null); result = scanner.next()) {
		    for(KeyValue keyValue : result.list()) {
		        System.out.println("Qualifier : " + keyValue.getKeyString() + " : Value : " + Bytes.toString(keyValue.getValue()));
		    }
		}
	}
	public static void main(String args[]) throws IOException {
		HbaseConnection h = new HbaseConnection();
		h.establishConnection();
	}

}
