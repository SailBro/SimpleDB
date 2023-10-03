package simpledb;

import java.io.Serializable;

/**
 * JoinPredicate compares fields of two tuples using a predicate. JoinPredicate
 * is most likely used by the Join operator.
 */
public class JoinPredicate implements Serializable {

    private static final long serialVersionUID = 1L;

    int field1;
    int field2;
    Predicate.Op op;

    /**
     * Constructor -- create a new predicate over two fields of two tuples.
     *
     * @param field1 The field index into the first tuple in the predicate
     * @param field2 The field index into the second tuple in the predicate
     * @param op     The operation to apply (as defined in Predicate.Op); either
     *               Predicate.Op.GREATER_THAN, Predicate.Op.LESS_THAN,
     *               Predicate.Op.EQUAL, Predicate.Op.GREATER_THAN_OR_EQ, or
     *               Predicate.Op.LESS_THAN_OR_EQ
     * @see Predicate
     */
    public JoinPredicate(int field1, Predicate.Op op, int field2) {
        // some code goes here

        this.field1 = field1;
        this.field2 = field2;
        this.op = op;
    }

    /**
     * Apply the predicate to the two specified tuples. The comparison can be
     * made through Field's compare method.
     *
     * @return true if the tuples satisfy the predicate.
     */
    public boolean filter(Tuple t1, Tuple t2) {
        // some code goes here

        //判断t1与t2能否满足连接的过滤条件
        if (t1 == null) {
            if (t2 == null)
                return true;
            else
                return false;
        } else {
            boolean res = t1.getField(field1).compare(op, t2.getField(field2));
            return res;
        }
    }

    public int getField1() {
        // some code goes here
        return field1;
    }

    public int getField2() {
        // some code goes here
        return field2;
    }

    public Predicate.Op getOperator() {
        // some code goes here
        return op;
    }
}