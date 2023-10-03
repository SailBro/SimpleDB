package simpledb;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;

    //表头声明
    private TupleDesc tupleDesc;
    private RecordId recordId;
    private final Field[] fields;

    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        // some code goes here

        tupleDesc=td;
        fields = new Field[td.numFields()];//行是字段的数组
        recordId=null;//初始化为null
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        // some code goes here
        return recordId;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        // some code goes here
        recordId=rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
        // some code goes here

        //先检查i是否有效
        if(i<0||i>=tupleDesc.numFields())
            throw new NoSuchElementException("invalid position!");

        fields[i]=f;
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     *
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        // some code goes here

        return fields[i];
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     *
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     *
     * where \t is any whitespace (except a newline)
     */
    public String toString() {
        // some code goes here
        StringBuilder str=new StringBuilder();
        for(int i=0;i<tupleDesc.numFields()-1;i++) {
            str.append((fields[i]).toString()+"\t");
        }
        str.append((fields[tupleDesc.numFields()-1]).toString());
        return str.toString();
        //throw new UnsupportedOperationException("Implement this");
    }

    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
        // some code goes here
        return Arrays.asList(fields).iterator();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Tuple other = (Tuple) obj;
        return Arrays.equals(fields, other.fields) && Objects.equals(recordId, other.recordId)
                && Objects.equals(tupleDesc, other.tupleDesc);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(tupleDesc, recordId);
        result = 31 * result + Arrays.hashCode(fields);
        return result;
    }

    /**
     * reset the TupleDesc of this tuple (only affecting the TupleDesc)
     * */
    public void resetTupleDesc(TupleDesc td)
    {
        // some code goes here

        tupleDesc=td;
    }

    //this与t连接，返回t3
    public Tuple create_link(Tuple t){
        TupleDesc newtd=TupleDesc.merge(tupleDesc,t.tupleDesc);//注意先后顺序
        Tuple t3=new Tuple(newtd);
        int len1=tupleDesc.numFields();
        int len2=t.tupleDesc.numFields();
        for(int i=0;i<len1;i++)
            t3.fields[i]=fields[i];
        for(int i=0;i<len2;i++)
            t3.fields[len1+i]=t.fields[i];
        //recordID默认为null
        return t3;
    }
}
