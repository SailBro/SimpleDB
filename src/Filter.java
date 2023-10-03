package simpledb;

import simpledb.*;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 * 用Predicate类比较单个field，然后过滤tuple
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;

    Predicate p;//用于对指定field判断，从而过滤tuple
    OpIterator child;//迭代器，用于read需要过滤的tuples

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     *
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    public Filter(Predicate p, OpIterator child) {
        // some code goes here

        this.p=p;
        this.child=child;
    }

    public Predicate getPredicate() {
        // some code goes here
        return p;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return child.getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here

        //先执行父类open，在继承的hasNext中会调用判断
        super.open();
        child.open();
    }

    public void close() {
        // some code goes here

        child.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here

        child.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     *
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here

        //返回下一个满足过滤条件的tuple
        //先判断还有没有下一个待过滤的tuple
        if(child==null)
            return null;

        while(child.hasNext()) {
            Tuple temp=child.next();
            //执行.next时迭代器自动往后跳一位
            if(p.filter(temp))
                return temp;
            if(child==null)
                return null;
        }
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here

        OpIterator[] children=new OpIterator[100];
        children[0]=child;

        return children;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here

        //重置迭代器，重置待过滤的元组
        child=children[0];
    }

}
