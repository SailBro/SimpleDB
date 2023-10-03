package simpledb;

import com.sun.deploy.security.SelectableSecurityManager;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {
        //TDItems是字段的类

        private static final long serialVersionUID = 1L;//序列化兼容

        /**
         * The type of the field
         * */
        public final Type fieldType;
        //Type是自定义类，作为字段的类型
        //被final关键字定义的类不能被继承

        /**
         * The name of the field
         * */
        public final String fieldName;
        //字段名字是字符串类型

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }//含参构造函数，field的两个重要属性：name,type

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
        //返回字段名和类型
    }

    private final TDItem[] tdItems;//tdItems作为表头，是字段数组类型

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here

        //迭代器
        //return 类.iterator();
        return (Iterator<TDItem>) Arrays.asList(tdItems).iterator();
        //Arrays.aslist 数组转化成列表
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here

        //typeAr为表头结构的类型数组
        //fieldAr为表头的Name数组
        tdItems = new TDItem[typeAr.length];
        for(int i=0;i<typeAr.length;++i){
            tdItems[i] = new TDItem(typeAr[i],fieldAr[i]);
            //根据typeAr[i]和fieldAr[i],依次生成filed[i]
        }
    }
    //构造函数

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here

        tdItems = new TDItem[typeAr.length];
        for(int i=0;i<typeAr.length;++i){
            tdItems[i] = new TDItem(typeAr[i],"");
        }
        //无fieldAr时，name默认先为空字符串
    }
    //构造函数

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here

        //tdItems是filed构成的数组
        return tdItems.length;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here

        //i is invalid时抛出错误
        if(i<0||i>= tdItems.length)
            throw new NoSuchElementException("invalid position!");
        else
            return tdItems[i].fieldName;

    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here

        //i is invalid时抛出错误
        if(i<0||i>= tdItems.length)
            throw new NoSuchElementException("invalid position!");
        else
            return tdItems[i].fieldType;
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here

        for(int i=0;i< tdItems.length;i++)
            if(tdItems[i].fieldName.equals(name))//name可能为null，必须在后
                return i;

        throw new NoSuchElementException("There's no matching name!");
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here

        int size=0;
        for(int i=0;i< tdItems.length;i++)
            size+=tdItems[i].fieldType.getLen();

        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     *
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here

        //创建field类型数组
        Type[]type_array=new Type[td1.numFields()+ td2.numFields()];
        //创建name类型数组
        String[]name_array=new String[td1.numFields()+ td2.numFields()];

        //添加td1信息
        for(int i=0;i<td1.tdItems.length;i++){
            type_array[i]=td1.tdItems[i].fieldType;
            name_array[i]=td1.tdItems[i].fieldName;

        }
        //添加td2信息
        for(int i=0;i<td2.tdItems.length;i++){
            int index=td1.tdItems.length;//偏移量
            type_array[index+i]=td2.tdItems[i].fieldType;
            name_array[index+i]=td2.tdItems[i].fieldName;

        }

        //创建td3
        TupleDesc td3=new TupleDesc(type_array,name_array);

        return td3;
    }


    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     *
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        // some code goes here

        //先判断类型是否相同
        if(!(o instanceof TupleDesc))
            return false;

        //判断长度是否相同
        if(tdItems.length!=((TupleDesc) o).tdItems.length)
            return false;

        for(int i=0;i<tdItems.length;i++){
            if(((TupleDesc) o).tdItems[i].fieldType==tdItems[i].fieldType)
                continue;
            else
                return false;
        }
        return true;

    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results

        //不实现
        throw new UnsupportedOperationException("unimplemented");

    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     *
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here

        StringBuilder goal=new StringBuilder();//可变型字符串
        for(int i=0;i< tdItems.length;i++){
            goal.append("fieldType[").append(i).append("](fieldName[").append(i).append("]");
            if(i< tdItems.length-1)
                goal.append(",");
        }

        return goal.toString();
    }
}
