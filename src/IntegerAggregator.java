package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     *
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    int gbfield;//分组字段的序号
    Type gbfieldtype;//分组字段的类型
    int afield;//聚合操作的字段序号，字段类型为int型
    Op what;//聚合方式
    HashMap<Field,Integer> map;//key为字段，value为聚合结果
    HashMap<Field,Integer> tally;//key为字段，value为对应组的个数
    HashMap<Field,Integer>Sum;//key为字段，value为对应组的求和




    public IntegerAggregator(int gbfizeld, Type gbfieldtype, int afield, Op what) {
        // some code goes here

        this.gbfield=gbfield ;
        this.gbfieldtype=gbfieldtype;
        this.afield=afield;
        this.what=what;
        map=new HashMap<>();
        tally=new HashMap<>();
        Sum=new HashMap<>();
        Iterator<Map.Entry<Field, Integer>>mapIt = map.entrySet().iterator();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */

    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here

        //将tup的gpfield加入分组
        Field f;
        //若无分组
        if(gbfield==NO_GROUPING) {
            f=null;
        }
        //有分组
        else{
            f=tup.getField(gbfield);
        }
        //判断f是否有value值

        //找到当前需要加入的afield
        IntField aField= (IntField) tup.getField(afield);
        int aValue=aField.getValue();

        //更新聚合操作结果
        //判断聚合操作
        switch (what){
            case MIN:
                if(!map.containsKey(f)){
                    map.put(f,aValue);
                    tally.put(f,1);
                }
                else{
                    map.put(f,Math.min(map.get(f),aValue));
                    tally.put(f,tally.get(f)+1);
                }
                break;
            case MAX:
                if(!map.containsKey(f)){
                    map.put(f,aValue);
                    tally.put(f,1);
                }
                else{
                    map.put(f,Math.max(map.get(f),aValue));
                    tally.put(f,tally.get(f)+1);
                }
                break;
            case SUM:
                if(!map.containsKey(f)){
                    map.put(f,aValue);
                    tally.put(f,1);
                }
                else{
                    map.put(f,map.get(f)+aValue);
                    tally.put(f,tally.get(f)+1);
                }
                break;
            case COUNT:
                if(!map.containsKey(f)){
                    map.put(f,1);
                    tally.put(f,1);
                }
                else{
                    map.put(f,map.get(f)+1);
                    tally.put(f,tally.get(f)+1);
                }
                break;
            case AVG:
                if(!map.containsKey(f)){
                    map.put(f,aValue);
                    tally.put(f,1);
                    Sum.put(f,aValue);
                    //System.out.print("1");
                }
                else{
                    tally.put(f,tally.get(f)+1);
                    Sum.put(f,Sum.get(f)+aValue);
                    int count=tally.get(f);
                    int sum=Sum.get(f);
                    map.put(f,sum/count);//迭代返回均值会增大误差
                    //System.out.print("2");
                }
                break;
        }

    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here

//        ///创建表头
//        String[] tdstr=new String[2];
//        Type[] tdtp=new Type[2];
//        //不空时
//        if(gbfield!=NO_GROUPING){
//            tdstr[0]="gp";
//            tdstr[1]="agg";
//            tdtp[0]=gbfieldtype;
//            tdtp[1]=Type.INT_TYPE;
//        }
//        //空时
//        else{
//            tdstr[0]="agg";
//            tdtp[0]=Type.INT_TYPE;
//        }
//        //表头确定
//        TupleDesc td = new TupleDesc(tdtp, tdstr);
//
//        //创建数组及其迭代器
//        ArrayList<Tuple> res=new ArrayList<Tuple>();
//        //遍历哈希表，加入tuple（gp,agg）
//        Iterator<Map.Entry<Field, Integer>>mapIt = map.entrySet().iterator();
//        while (mapIt.hasNext()) {
//            Map.Entry<Field, Integer> entry =mapIt.next();
//            Field gpf= entry.getKey();
//            IntField agg=new IntField(entry.getValue());
//            Tuple tp=new Tuple(td);
//            if(gbfield!=NO_GROUPING){
//                tp.setField(0,gpf);
//                tp.setField(1,agg);
//            }
//            else{
//                tp.setField(0,agg);
//            }
//            res.add(tp);
//        }
//
//        TupleIterator resIt=new TupleIterator(td,res);
//        return resIt;


        return new IntegerIterator();
    }

    //创建内部类
    private final class IntegerIterator implements OpIterator{

        Iterator<Tuple> it;

        public IntegerIterator(){
            it=null;
        }


        @Override
        public void open() throws DbException, TransactionAbortedException {
            //所有结果元组全部装入it中

            //创建数组
            ArrayList<Tuple> res=new ArrayList<Tuple>();
            //遍历哈希表，加入tuple（gp,agg）
            Iterator<Map.Entry<Field, Integer>>mapIt = map.entrySet().iterator();

            while (mapIt.hasNext()) {
                Map.Entry<Field, Integer> entry =mapIt.next();
                Field gpf= entry.getKey();
                IntField agg=new IntField(entry.getValue());
                Tuple tp=new Tuple(getTupleDesc());
                if(gbfield!=NO_GROUPING){
                    tp.setField(0,gpf);
                    tp.setField(1,agg);
                }
                else{
                    tp.setField(0,agg);
                }
                res.add(tp);
            }
            it= res.iterator();
        }

        @Override
        public TupleDesc getTupleDesc() {

            String[] tdstr;
            Type[] tdtp;
            //不空时
            if(gbfield!=NO_GROUPING){
                tdstr=new String[2];
                tdtp=new Type[2];
                tdstr[0]="gp";
                tdstr[1]="agg";
                tdtp[0]=gbfieldtype;
                tdtp[1]=Type.INT_TYPE;
            }
            //空时
            else{
                tdstr=new String[1];
                tdtp=new Type[1];
                tdstr[0]="agg";
                tdtp[0]=Type.INT_TYPE;
            }
            //表头确定
            TupleDesc td = new TupleDesc(tdtp, tdstr);
            return td;
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if(it==null)
                return false;

            if(!it.hasNext())
                return false;

            return true;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            //if(it==null||!hasNext())666666666666
            //return null;
            return it.next();
        }

        @Override
        public void close() {
            it=null;
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }
    }

}



