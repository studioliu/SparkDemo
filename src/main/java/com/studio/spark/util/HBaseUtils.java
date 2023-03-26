package com.studio.spark.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 操作Hbase的工具类
 * 静态类式：方法设计为static方法，构造方法设计为private
 */
public final class HBaseUtils {
    private static Connection conn = null;

    static {
        try {
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "master,slave1,slave2");
            conf.set("hbase.zookeeper.property.clientPort", "2181");

            conn = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 私有化构造器，不让其他类创建本类对象
    private HBaseUtils() {
    }

    // 创建命名空间
    public static void createNameSpace(String namespace) throws IOException {
        Admin admin = conn.getAdmin();
        NamespaceDescriptor db = NamespaceDescriptor.create(namespace).build();
        admin.createNamespace(db);
        admin.close();
    }

    // 检查表是否存在（tableName可由命名空间:表名拼接而成）
    public static boolean isTableExist(String tableName) throws IOException {
        Admin admin = conn.getAdmin();
        boolean flag = admin.tableExists(TableName.valueOf(tableName));
        admin.close();
        return flag;
    }

    // 创建表
    public static boolean createTable(String tableName, String... columnFamilys) throws IOException {
        if (columnFamilys.length <= 0) {
            System.out.println("请输入列族");
            return false;
        }
        if (isTableExist(tableName)) {
            System.out.println(tableName + "表已存在！");
            return false;
        }
        Admin admin = conn.getAdmin();
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        for (String columnFamily : columnFamilys) {
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(columnFamily);
            hTableDescriptor.addFamily(hColumnDescriptor);
        }
        admin.createTable(hTableDescriptor);
        admin.close();
        return true;
    }

    // 修改表
    public static boolean modifyTable(String tableName, String columnFamily, int version) throws IOException {
        Admin admin = conn.getAdmin();
        HTableDescriptor tableDescriptor = admin.getTableDescriptor(TableName.valueOf(tableName));
        HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(Bytes.toBytes(columnFamily)).setMaxVersions(version);
        tableDescriptor.modifyFamily(hColumnDescriptor);
        admin.modifyTable(TableName.valueOf(tableName), tableDescriptor);
        admin.close();
        return true;
    }

    // 删除表
    public static boolean deleteTable(String tableName) throws IOException {
        if (!isTableExist(tableName)) {
            System.out.println(tableName + "表不存在！");
            return false;
        }
        Admin admin = conn.getAdmin();
        admin.disableTable(TableName.valueOf(tableName));
        admin.deleteTable(TableName.valueOf(tableName));
        admin.close();
        return true;
    }

    // 添加数据
    public static void putCell(String tableName, String rowKey, String columnFamily, String column, String value) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
        table.put(put);
        table.close();
    }

    // 获取数据
    public static String getCell(String tableName, String rowKey, String columnFamily, String column) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column));
        Result result = table.get(get);
        Cell[] cells = result.rawCells();
        String value = "";
        for (Cell cell : cells) {
            value += Bytes.toString(CellUtil.cloneValue(cell));
        }
        table.close();
        return value;
    }

    // 扫描表的所有数据
    public static void scanTable(String tableName) throws Exception {
        Table table = conn.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        System.out.println("==================================================");
        System.out.println("rowKey" + "\t" + "columnFamily" + "\t" +
                "column" + "\t" + "timestamp" + "\t" + "value" + "\t");
        System.out.println("==================================================");
        for (Result result : scanner) {
            for (Cell cell : result.listCells()) {
                System.out.println(Bytes.toString(result.getRow()) + "\t" +
                        Bytes.toString(CellUtil.cloneFamily(cell)) + "\t" +
                        Bytes.toString(CellUtil.cloneQualifier(cell)) + "\t" +
                        cell.getTimestamp() + "\t" +
                        Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
        scanner.close();
        table.close();
    }

    // 扫描表的指定行的数据（左闭右开）
    public static void scanTable(String tableName, String startRow, String stopRow) throws Exception {
        Table table = conn.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan(Bytes.toBytes(startRow), Bytes.toBytes(stopRow));
        ResultScanner scanner = table.getScanner(scan);
        System.out.println("==================================================");
        System.out.println("rowKey" + "\t" + "columnFamily" + "\t" +
                "column" + "\t" + "timestamp" + "\t" + "value" + "\t");
        System.out.println("==================================================");
        for (Result result : scanner) {
            for (Cell cell : result.listCells()) {
                System.out.println(Bytes.toString(result.getRow()) + "\t" +
                        Bytes.toString(CellUtil.cloneFamily(cell)) + "\t" +
                        Bytes.toString(CellUtil.cloneQualifier(cell)) + "\t" +
                        cell.getTimestamp() + "\t" +
                        Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
        scanner.close();
        table.close();
    }

    // 扫描表获取指定行、指定列的数据集合（左闭右开）
    public static List<String> scanTable(String tableName, String startRow, String stopRow, String columnFamily, String column) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan(Bytes.toBytes(startRow), Bytes.toBytes(stopRow));
        scan.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column));
        ResultScanner scanner = table.getScanner(scan);
        ArrayList<String> arrayList = new ArrayList<>();
        for (Result result : scanner) {
            arrayList.add(Bytes.toString(result.value()));
        }
        scanner.close();
        table.close();
        return arrayList;
    }

    // 删除数据
    public static void deleteCell(String tableName, String rowKey, String columnFamily, String column) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        delete.addColumns(Bytes.toBytes(columnFamily), Bytes.toBytes(column));
        table.delete(delete);
        table.close();
    }

    // 删除一行数据
    public static void deleteRow(String tableName, String rowKey) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        table.delete(delete);
        table.close();
    }

    // 获得表描述
    public static void getTableDesc(String tableName) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        HTableDescriptor tableDescriptor = table.getTableDescriptor();
        HColumnDescriptor[] columnFamilies = tableDescriptor.getColumnFamilies();
        for (HColumnDescriptor columnFamily : columnFamilies) {
            String name = columnFamily.getNameAsString();       //列族名
            int bs = columnFamily.getBlocksize();               //块大小
            int minVers = columnFamily.getMinVersions();        //最小版本号
            int maxVers = columnFamily.getMaxVersions();        //最大版本号
            int defVers = HColumnDescriptor.DEFAULT_VERSIONS;   //默认版本号
            System.out.println("name : " + name +
                    " blocksize : " + bs +
                    " minVers : " + minVers +
                    " maxVers : " + maxVers +
                    " defVers : " + defVers);
        }
        table.close();
    }

    public static void close() {
        if (conn != null) {
            try {
                conn.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}