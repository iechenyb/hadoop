package com.cyb.hbase;

import java.io.IOException;  
import java.util.ArrayList;  
import java.util.List;  
  
import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.hbase.HColumnDescriptor;  
import org.apache.hadoop.hbase.HTableDescriptor;  
import org.apache.hadoop.hbase.KeyValue;  
import org.apache.hadoop.hbase.client.Delete;  
import org.apache.hadoop.hbase.client.Get;  
import org.apache.hadoop.hbase.client.HBaseAdmin;  
import org.apache.hadoop.hbase.client.HTable;  
import org.apache.hadoop.hbase.client.HTableInterface;  
import org.apache.hadoop.hbase.client.HTablePool;  
import org.apache.hadoop.hbase.client.Put;  
import org.apache.hadoop.hbase.client.Result;  
import org.apache.hadoop.hbase.client.ResultScanner;  
import org.apache.hadoop.hbase.client.Scan;  
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;  
import org.apache.hadoop.hbase.filter.RegexStringComparator;  
import org.apache.hadoop.hbase.filter.RowFilter;  
  
 //http://www.yiibai.com/hbase/hbase_scan.html
@SuppressWarnings("deprecation")  
public class HBaseTest {  
      
    static HBaseAdmin admin=null;  
    Configuration conf=null;  
    public HBaseTest(){  
        try {  
        conf = new Configuration();  
        conf.set("hbase.zookeeper.quorum", "192.168.16.211:2181");  
        conf.set("hbase.rootdir", "hdfs://192.168.16.211:9000/hbase");  
        admin = new HBaseAdmin(conf);  
        } catch (Exception e) {  
        e.printStackTrace();  
        }  
        }  
      
    public static void main(String[] args) throws Exception {  
        HBaseTest hBase=new HBaseTest();  
        //������  
        hBase.createTable("stu","name");  
        //������  
        hBase.getAllTable();  
       /* HColumnDescriptor col1 = new HColumnDescriptor("name2");
        HColumnDescriptor col2 = new HColumnDescriptor("name3");
        // Adding column family
        admin.addColumn("stu", col1);
        admin.addColumn("stu", col2);
        admin.deleteColumn();
        */
        Boolean bool=admin.isTableEnabled("stu");
        
        //����һ������  
      hBase.addOneRecord("stu","key1","name1","","jjhk");  
      hBase.addOneRecord("stu","key1","name2","","1286");  
      hBase.addOneRecord("stu","key1","name3","","sf");  
        //�õ�һ������  
      hBase.getKey("stu","key1");  
        //�õ���������  
      hBase.getAllData("stu");  
        //ɾ��һ����¼  
      //hBase.deleteOneRecord("stu","key1");  
        //ɾ����  
      //hBase.deleteTable("stu");  
        //scan��������ʹ��  
      hBase.getScanData("stu","name","");  
    //rowFilter��ʹ��  
      //hBase.getRowFilter("waln_log","^*_201303131459\\d*$");  
          
    }  
    /** 
     * rowFilterʹ�� 
     * @param tableName 
     * @param reg 
     * @throws Exception 
     */  
    private void getRowFilter(String tableName, String reg) throws Exception {  
        @SuppressWarnings("resource")  
        HTable hTable=new HTable(conf, tableName);  
        Scan scan=new Scan();  
        RowFilter rowFilter = new RowFilter(CompareOp.EQUAL, new RegexStringComparator(reg));  
        scan.setFilter(rowFilter);  
        ResultScanner scanner = hTable.getScanner(scan);  
        for (Result result : scanner) {  
            System.out.println(new String(result.getRow()));  
        }  
    }  
    /** 
     * scan������ 
     * @param tableName 
     * @param family 
     * @param qualifier 
     * @throws Exception 
     */  
    private void getScanData(String tableName, String family, String qualifier) throws Exception {  
        @SuppressWarnings("resource")  
        HTable hTable = new HTable(conf, tableName);  
        Scan scan = new Scan();  
        scan.addColumn(family.getBytes(), qualifier.getBytes());  
        ResultScanner scanner = hTable.getScanner(scan);  
        for (Result result : scanner) {  
            if(result.raw().length==0){  
                System.out.println(tableName+" ������Ϊ�գ�");  
            }else{  
                for (KeyValue kv: result.raw()){  
                    System.out.println(new String(kv.getFamily())+"|"+new String(kv.getQualifier())+""+new String(kv.getValue()));  
                }  
            }  
        }  
    }  
    /** 
     * ɾ���� 
     * @param tableName 
     */  
    private void deleteTable(String tableName) {  
        try {  
            if(admin.tableExists(tableName)){  
                admin.disableTable(tableName);  
                admin.deleteTable(tableName);  
                System.out.println("��:"+tableName+" ɾ���ɹ�");  
            }  
        } catch (IOException e) {  
            e.printStackTrace();  
            System.out.println("��"+tableName+"ɾ��ʧ��");  
        }  
    }  
  
    /** 
     * ɾ��һ����¼ 
     * @param tableName 
     * @param rowKey 
     */  
    public void deleteOneRecord(String tableName, String rowKey) {  
        @SuppressWarnings("resource")  
        HTablePool hTablePool = new HTablePool(conf, 1000);  
        HTableInterface table = hTablePool.getTable(tableName);  
        Delete delete = new Delete(rowKey.getBytes());  
        try {  
            table.delete(delete);  
            System.out.println(rowKey+"��¼ɾ���ɹ���");  
        } catch (IOException e) {  
            e.printStackTrace();  
            System.out.println(rowKey+"��¼ɾ��ʧ�ܣ�");  
        }  
    }  
      
    /** 
     * �õ�ȫ������ 
     * @param string 
     * @throws Exception  
     */  
    private void getAllData(String tableName) throws Exception {  
        @SuppressWarnings("resource")  
        HTable hTable = new HTable(conf, tableName);  
        Scan scan = new Scan();  
        ResultScanner scanner = hTable.getScanner(scan);  
        for (Result result : scanner) {  
            if(result.raw().length==0){  
                System.out.println(tableName+"  Ϊ��");  
            }else{  
                for (KeyValue kv : result.raw()) {  
                	System.out.println(new String(kv.getFamily())+"|"+new String(kv.getQualifier())+""+new String(kv.getValue()));  
                }  
            }  
        }  
    }  
  
    /** 
     * �õ�һ������ 
     * @param string 
     * @param string2 
     */  
     private void getKey(String tableName, String rowkey) {  
        @SuppressWarnings("resource")  
        HTablePool hTablePool = new HTablePool(conf, 1000);  
        HTableInterface table = hTablePool.getTable(tableName);  
        Get get = new Get(rowkey.getBytes());  
        try {  
            Result result = table.get(get);  
            if(result.raw().length==0){  
                System.out.println("�м�"+rowkey+"����Ϊ�գ�");  
            }else{  
                for (KeyValue kv : result.raw()) {  
                	System.out.println(new String(kv.getFamily())+"|"+new String(kv.getQualifier())+""+new String(kv.getValue())); 
                }  
            }  
        } catch (IOException e) {  
            e.printStackTrace();  
        }  
    }  
  
    /** 
      * ����һ������ 
      * @param string 
      * @param string2 
      * @param string3 
      * @param string4 
      * @param string5 
      */  
     private void addOneRecord(String tableName, String rowkey, String column,  
            String qua, String value) {  
         @SuppressWarnings({ "resource" })  
        HTablePool hTablePool = new HTablePool(conf,1000);  
         HTableInterface table = hTablePool.getTable(tableName);  
         Put put = new Put(rowkey.getBytes());  
         put.add(column.getBytes(), qua.getBytes(), value.getBytes());  
         try {  
            table.put(put);  
            System.out.println("�м�"+rowkey+" ���ݲ���ɹ���");  
        } catch (IOException e) {  
            e.printStackTrace();  
            System.out.println("�м� "+rowkey+" ��������ʧ�ܣ�");  
        }  
    }  
  
    /** 
      * ������ 
      * @return 
      * @throws Exception 
      */  
    private List<String> getAllTable() throws Exception {  
        ArrayList<String> tables=new ArrayList<String>();  
        if(admin!=null){  
            HTableDescriptor[] listTables = admin.listTables();  
            if(listTables.length>0){  
                for (HTableDescriptor tableDesc : listTables) {  
                    tables.add(tableDesc.getNameAsString());  
                    System.out.println("table:"+tableDesc.getNameAsString());  
                }  
            }  
        }  
        return tables;  
    }  
    //  /hbase/data/default/stu  
  
    /** 
     * ������ 
     * @param tableName 
     * @param column 
     * @throws Exception 
     */  
    private void createTable(String tableName, String column) throws Exception {  
        if(admin.tableExists(tableName)){  
            System.out.println("���Ѿ����ڣ�");  
        }else{  
            HTableDescriptor tableDesc=new HTableDescriptor(tableName);  
            tableDesc.addFamily(new HColumnDescriptor(column.getBytes()));  
            admin.createTable(tableDesc);  
            System.out.println(tableName+"�����ɹ���");  
        }  
    }  
      
} 
