package simpledb;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 * 对满足过滤条件的两个tuple进行连接操作
 */
public class Join extends Operator {

    private static final long serialVersionUID = 1L;

    JoinPredicate p;
    OpIterator child1;//指向元组1
    OpIterator child2;//指向元组2

    /**
     * Constructor. Accepts two children to join and the predicate to join them
     * on
     *
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    public Join(JoinPredicate p, OpIterator child1, OpIterator child2) {
        // some code goes here

        this.p=p;
        this.child1=child1;
        this.child2=child2;
        this.t1=null;
    }

    public JoinPredicate getJoinPredicate() {
        // some code goes here
        return p;
    }

    /**
     * @return
     *       the field name of join field1. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField1Name() {
        // some code goes here

        //返回tuple1需要比较的给定字段名
        int index=p.field1;
        return child1.getTupleDesc().getFieldName(index);

    }

    /**
     * @return
     *       the field name of join field2. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField2Name() {
        // some code goes here

        int index=p.field2;
        return child1.getTupleDesc().getFieldName(index);
    }

    /**
     * @see simpledb.TupleDesc#merge(TupleDesc, TupleDesc) for possible
     *      implementation logic.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here

        TupleDesc td1=child1.getTupleDesc();
        TupleDesc td2=child2.getTupleDesc();
        //连接到td3
        TupleDesc td3=TupleDesc.merge(td1,td2);
        return td3;
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here

        super.open();
        child1.open();
        child2.open();
    }

    public void close() {
        // some code goes here

        child1.close();
        child2.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here

        child1.rewind();
        child2.rewind();
    }

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     *
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */

    Tuple t1;//记录当前child1的指向
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here

        //返回合并之后的tuple
        //先检查迭代器是否为空
        if(child1==null||child2==null)
            return null;

        //一开始t1为null
        if(t1==null){
            if(!child1.hasNext())
                return null;
            else
                t1=child1.next();
        }

        while(t1!=null) {
            //在当前的t2往下找(t1为Null时child2已重置，不为Null时child2在下一个)
            while (child2 != null && child2.hasNext()) {
                Tuple t2 = child2.next();
                if (p.filter(t1, t2)) {
                    Tuple t3 = t1.create_link(t2);
                    System.out.println(t3.toString());
                    return t3;
                }
            }
            //没有合适的t2,找下一个t1
            if(child1.hasNext()) {
                t1 = child1.next();//为下一个t1寻找
                child2.rewind();//把child2重置
            }
            else
                return null;
        }

        return null;

    }


    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        OpIterator[] children=new OpIterator[100];
        children[0]=child1;
        children[1]=child2;
        return children;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here

        child1=children[0];
        child2=children[1];
    }

}
