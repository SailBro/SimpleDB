package simpledb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The Catalog keeps track of all available tables in the database and their
 * associated schemas.
 * For now, this is a stub catalog that must be populated with tables by a
 * user program before it can be used -- eventually, this should be converted
 * to a catalog that reads a catalog table from disk.
 *
 * @Threadsafe
 */
public class Catalog {

    /**
     * Constructor.
     * Creates a new, empty catalog.
     */

    //HashMap存储各个table的信息
    private final ConcurrentHashMap<Integer,Table> hashTable;
    //(key,value)=(Int,Table)

    public Catalog() {
        // some code goes here

        hashTable=new ConcurrentHashMap<Integer,Table>();
    }

    //创建table类
    private static class Table {
        private static final long serialVersionUID = 1L;

        public final DbFile dbFile;
        //存储表的相关信息，含有ID等
        public String tableName;
        public final String pk;

        public Table(DbFile file, String name, String pkeyField) {
            dbFile = file;
            tableName = name;
            pk = pkeyField;
        }
    }

    /**
     * Add a new table to the catalog.
     * This table's contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     * @param name the name of the table -- may be an empty string.  May not be null.  If a name
     * conflict exists, use the last table to be added as the table for a given name.
     * @param pkeyField the name of the primary key field
     */

    public void addTable(DbFile file, String name, String pkeyField) {
        // some code goes here

        //判断是否已经有该名字的表
        int old_key=-1;
        Enumeration<Integer> hashKeys=hashTable.keys();
        while(hashKeys.hasMoreElements()){
            Integer key=hashKeys.nextElement();
            Table temp_table=hashTable.get(key);
            if(temp_table.tableName.equals(name))
                old_key=key;
        }

        Table old_table=null;
        //若有odd_key存在，则给odd_table改名
        if(old_key!=-1) {
            old_table = hashTable.get(old_key);
            old_table.tableName=null;
        }
        Table newTable = new Table(file, name, pkeyField);
        hashTable.put(file.getId(), newTable);
    }

    public void addTable(DbFile file, String name) {
        addTable(file, name, "");
    }

    /**
     * Add a new table to the catalog.
     * This table has tuples formatted using the specified TupleDesc and its
     * contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     */
    public void addTable(DbFile file) {
        addTable(file, (UUID.randomUUID()).toString());
    }

    /**
     * Return the id of the table with a specified name,
     * @throws NoSuchElementException if the table doesn't exist
     */
    public int getTableId(String name) throws NoSuchElementException {
        // some code goes here

        //先判断name是否为空
        if(name==null)
            throw new NoSuchElementException("invalid name!");

        //遍历哈希表的keys，找到对应value(table)，判断tableName是否相同
        //相同则返回table.dbFile.getId()
        Enumeration<Integer> hashKeys=hashTable.keys();
        while(hashKeys.hasMoreElements()){
            Integer key=hashKeys.nextElement();
            Table temp_table=hashTable.get(key);
            if(temp_table.tableName!=null&&temp_table.tableName.equals(name))
                return key;
            //return temp_table.dbFile.getId();
            //key==tableId
        }

        //遍历结束未找到则失败
        throw new NoSuchElementException("There's no such name!");

    }

    /**
     * Returns the tuple descriptor (schema) of the specified table
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     * @throws NoSuchElementException if the table doesn't exist
     */
    public TupleDesc getTupleDesc(int tableid) throws NoSuchElementException {
        // some code goes here

        Table temp_table=hashTable.get(tableid);
        if(temp_table==null)
            throw new NoSuchElementException("Th table doesn't exist!");
        else
            return temp_table.dbFile.getTupleDesc();

    }

    /**
     * Returns the DbFile that can be used to read the contents of the
     * specified table.
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     */
    public DbFile getDatabaseFile(int tableid) throws NoSuchElementException {
        // some code goes here

        Table temp_table=hashTable.get(tableid);
        if(temp_table==null)
            throw new NoSuchElementException("Th table doesn't exist!");
        else
            return temp_table.dbFile;

    }

    public String getPrimaryKey(int tableid) {
        // some code goes here

        Table temp_table=hashTable.get(tableid);
        if(temp_table==null)
            throw new NoSuchElementException("Th table doesn't exist!");
        else
            return temp_table.pk;
    }

    public Iterator<Integer> tableIdIterator() {
        // some code goes here

        return hashTable.keySet().iterator();
    }

    public String getTableName(int id) {
        // some code goes here

        Table temp_table=hashTable.get(id);
        if(temp_table==null)
            throw new NoSuchElementException("Th table doesn't exist!");
        else
            return temp_table.tableName;
    }

    /** Delete all tables from the catalog */
    public void clear() {
        // some code goes here

        hashTable.clear();//直接调用函数
    }

    /**
     * Reads the schema from a file and creates the appropriate tables in the database.
     * @param catalogFile
     */
    public void loadSchema(String catalogFile) {
        String line = "";
        String baseFolder=new File(new File(catalogFile).getAbsolutePath()).getParent();
        try {
            BufferedReader br = new BufferedReader(new FileReader(new File(catalogFile)));

            while ((line = br.readLine()) != null) {
                //assume line is of the format name (field type, field type, ...)
                String name = line.substring(0, line.indexOf("(")).trim();
                //System.out.println("TABLE NAME: " + name);
                String fields = line.substring(line.indexOf("(") + 1, line.indexOf(")")).trim();
                String[] els = fields.split(",");
                ArrayList<String> names = new ArrayList<String>();
                ArrayList<Type> types = new ArrayList<Type>();
                String primaryKey = "";
                for (String e : els) {
                    String[] els2 = e.trim().split(" ");
                    names.add(els2[0].trim());
                    if (els2[1].trim().toLowerCase().equals("int"))
                        types.add(Type.INT_TYPE);
                    else if (els2[1].trim().toLowerCase().equals("string"))
                        types.add(Type.STRING_TYPE);
                    else {
                        System.out.println("Unknown type " + els2[1]);
                        System.exit(0);
                    }
                    if (els2.length == 3) {
                        if (els2[2].trim().equals("pk"))
                            primaryKey = els2[0].trim();
                        else {
                            System.out.println("Unknown annotation " + els2[2]);
                            System.exit(0);
                        }
                    }
                }
                Type[] typeAr = types.toArray(new Type[0]);
                String[] namesAr = names.toArray(new String[0]);
                TupleDesc t = new TupleDesc(typeAr, namesAr);
                HeapFile tabHf = new HeapFile(new File(baseFolder+"/"+name + ".dat"), t);
                addTable(tabHf,name,primaryKey);
                System.out.println("Added table : " + name + " with schema " + t);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        } catch (IndexOutOfBoundsException e) {
            System.out.println ("Invalid catalog entry : " + line);
            System.exit(0);
        }
    }
}

