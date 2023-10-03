package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */

    TransactionId t;
    OpIterator child;
    int tableId;
    boolean isUsed;
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // some code goes here

        this.t=t;
        this.child=child;
        this.tableId=tableId;
        isUsed=false;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here

        return new TupleDesc(new Type[] {Type.INT_TYPE},new String [] {"count"});
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        child.open();
        super.open();
    }

    public void close() {
        // some code goes here
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child.rewind();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if(isUsed==true)
            return null;

        isUsed=true;
        int count=0;
        while(child.hasNext()){
            Tuple tuple = child.next();
            try {
                Database.getBufferPool().insertTuple(t,tableId,tuple);
                count++;
            } catch (IOException e) {

            }
        }
        Tuple res = new Tuple(getTupleDesc());
        res.setField(0,new IntField(count));
        return res;
    }

    @Override
    public OpIterator[] getChildren(){
        // some code goes here
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        children[0]=child;
    }
}
