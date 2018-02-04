import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Appender;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.jruby.RubyProcess;

import java.awt.*;
import java.io.IOException;
import java.rmi.server.ExportException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by Administrator on 2017/9/2.
 */
public class ExampleClient {
    private final static byte[] ROW1 = Bytes.toBytes("row1");
    private final static byte[] ROW2 = Bytes.toBytes("row2");
    private final static byte[] COLFAM1 = Bytes.toBytes("colfam1");
    private final static byte[] COLFAM2 = Bytes.toBytes("colfam2");
    private final static byte[] QUAL1 = Bytes.toBytes("qual1");
    private final static byte[] QUAL2 = Bytes.toBytes("qual2");
    private final static byte[] VAL1 = Bytes.toBytes("val1");
    private final static byte[] VAL2 = Bytes.toBytes("val2");
    private final static byte[] VAL3 = Bytes.toBytes("val3");

    /* 线程类*/
    static class UnlockedPut implements Runnable {
        public void run(){
            try{
                //初始化HBase
                Configuration conf = new Configuration();
                conf.set("hbase.zookeeper.quorum","master,node1,node2");  //hbase-site.xml
                conf.set("hbase.zookeeper.property.clientPort", "2181");
                HBaseHelper hbase = new HBaseHelper(conf);
                Table table = hbase.getConnection().getTable(TableName.valueOf("test"));
                Put put = new Put(ROW1);
                put.addColumn(COLFAM1,QUAL1,VAL3);
                long time = System.currentTimeMillis();
                table.put(put);
                System.out.println("wait time:"+(System.currentTimeMillis()-time) + "ms");
            }catch (IOException e){
                System.err.println("Thread error:"+e);
            }
        }
    }

    private static void scan(Table table,int caching,int batch) throws IOException {
        Logger log = Logger.getLogger("org.apache.hadoop");
        final int[] counters = {0, 0};
        Appender appender = new AppenderSkeleton() {
            @Override
            protected void append(LoggingEvent event) {
                String msg = event.getMessage().toString();
                if (msg != null && msg.contains("Call: next")) {
                    counters[0]++;
                }
            }

            @Override
            public void close() {
            }

            @Override
            public boolean requiresLayout () {
                return false;
            }
        };
        log.removeAllAppenders();
        log.setAdditivity(false);
        log.addAppender(appender);
        log.setLevel(Level.DEBUG);

        Scan scan = new Scan();
        scan.setCaching(caching);
        scan.setBatch(batch);
        ResultScanner scanner = table.getScanner(scan);
        for (Result result:scanner){
            counters[1]++;
        }
        scanner.close();
        System.out.println("Caching:"+caching+",Batch:"+batch+
                ",Results:"+counters[1] + ",RPCs:"+counters[0]);
    }

    public static void main(String[] args) throws Exception {
        //初始化HBase
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum","master,node1,node2");  //hbase-site.xml
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        HBaseHelper hbase = new HBaseHelper(conf);
        //创建表
        String tableName = "blog";
        //hbase.deleteTable(tableName);
        String colFamilies[] = {"article","author"};
        //hbase.createTable(tableName,colFamilies);

        /*
        //插入一条记录
        hbase.insertRecord(tableName,"1","article","title","hadoop学习资料");
        hbase.insertRecord(tableName,"1","author","name","bill");
        hbase.insertRecord(tableName,"1","article","content","http://www.hadoop.com");

        //查询一条记录
        Result rs1 = hbase.getOneRecord(tableName,"1");
        for(KeyValue kv:rs1.raw()){
            System.out.println(new String(kv.getRow()));
            System.out.println(new String(kv.getFamily()));
            System.out.println(new String(kv.getQualifier()));
            System.out.println(new String(kv.getValue()));
        }

        //查询整个table
        List<Result> list=null;
        list=hbase.getAllRecord(tableName);
        Iterator<Result> it=list.iterator();
        while(it.hasNext()){
            Result rs2=it.next();
            for(KeyValue kv:rs2.raw()){
                System.out.println("row key is:"+new String(kv.getRow()));
                System.out.print("family is  : " + new String(kv.getFamily()));
                System.out.print("qualifier is:" + new String(kv.getQualifier()));
                System.out.print("timestamp is:" + kv.getTimestamp());
                System.out.println("Value  is  : " + new String(kv.getValue()));
            }
        }*/

        //Table table = hbase.getConnection().getTable(TableName.valueOf(tableName));

        /* put test */
//        Get get = new Get(Bytes.toBytes("4"));
//        get.addColumn(Bytes.toBytes("article"),Bytes.toBytes("author"));
//        //get.addFamily(Bytes.toBytes("article"));
//        Result result = table.get(get);
//        System.out.println(result.toString());
//        byte[] val = result.getValue(Bytes.toBytes("article"),Bytes.toBytes("author"));
//        System.out.println(result.size());
//        System.out.println(Bytes.toString(get.getRow()));  //获取行键
//        System.out.println(get.getTimeRange());
//        System.out.println("article:content:"+ Bytes.toString(val));

        /* delete test */
//        Delete delete = new Delete(Bytes.toBytes("1"));
//        delete.addColumn(Bytes.toBytes("article"),Bytes.toBytes("content"));
//        table.delete(delete);

        /* batch */
        /*
        String colFams[] = {"colfam1","colfam2"};
        hbase.createTable("test",colFams);
        Table tableTest = hbase.getConnection().getTable(TableName.valueOf("test"));

        List<Row> batch =  new ArrayList<Row>();
        Put put = new Put(ROW2);
        put.addColumn(COLFAM2,QUAL1,Bytes.toBytes("val5"));
        batch.add(put);

        Get get1 = new Get(ROW1);
        get1.addColumn(COLFAM1,QUAL1);
        batch.add(get1);

        Delete delete = new Delete(ROW1);
        delete.addColumn(COLFAM1,QUAL2);
        batch.add(delete);

//        Get get2 = new Get(ROW2);
//        get2.addFamily(Bytes.toBytes("faml"));
//        batch.add(get2);

        Object[] results = new Object[batch.size()];
        try{
            tableTest.batch(batch,results);
        }catch (ExportException e){
            System.err.println("Error:"+e);
        }

        for(int i = 0; i < results.length;i++){
            System.out.println("Result[" + i + "]" + results[i]);
        }
        */

        /* 行锁 */
        /*
        System.out.println("Taking out lock...");
        Table table = hbase.getConnection().getTable(TableName.valueOf("test"));
        Region.RowLock rowLock = new Region.RowLock();
        */

        /* scanner */
        /*
        Scan scan = new Scan();
        scan.setBatch(1);
        //scan.addFamily(COLFAM1);
        scan.setStartRow(Bytes.toBytes("row1"));
        scan.setStopRow(Bytes.toBytes("row3"));
        Table table = hbase.getConnection().getTable(TableName.valueOf("test"));
        ResultScanner resultScanner = table.getScanner(scan);
        for (Result result:resultScanner){
            System.out.println(result);
        }
        resultScanner.close();
        */
        Table table = hbase.getConnection().getTable(TableName.valueOf("test"));
        scan(table,1,1);
        scan(table,200,1);
        scan(table,2000,100);
    }
}
