import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;

import java.util.Iterator;
import java.util.List;

/**
 * Created by Administrator on 2017/9/2.
 */
public class ExampleClient {
    public static void main(String[] args) throws Exception {
        //初始化HBase
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum","master,node1,node2");  //hbase-site.xml
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        HBaseHelper hbase = new HBaseHelper(conf);

        //创建表
        String tableName = "blog";
        hbase.deleteTable(tableName);
        String colFamilies[] = {"article","author"};
        hbase.createTable(tableName,colFamilies);

        //插入一条记录
        hbase.insertRecord(tableName,"1","article","title","hadoop学习资料");
        //hbase.insertRecord(tableName,"1","author","name","bill");
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
        }
    }
}
