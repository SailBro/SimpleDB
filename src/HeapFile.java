package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f
     * the file that stores the on-disk backing store for this heap
     * file.
     */

    final File f;
    final TupleDesc td;


    public HeapFile(File f, TupleDesc td) {
        // some code goes here

        this.f = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return this.f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
    }


    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here

        int tableId = pid.getTableId();
        int pgN = pid.getPageNumber();//table中的页数
        HeapPageId h_id = new HeapPageId(tableId, pgN);//确定HeapPageId
        int page_size = BufferPool.getPageSize();//确定每页的大小
        HeapPage res = null;

        try {
            RandomAccessFile myfile = new RandomAccessFile(f, "r");
            //输入流file
            byte[] bytes = new byte[page_size];//每页的字节
            myfile.seek(pid.getPageNumber() * page_size);//定位到对应页的起始
            int test=myfile.read(bytes, 0, page_size);//把页的数据从0~page_size依次读入bytes中
            if(test!=BufferPool.getPageSize())
                throw new IllegalArgumentException("wrong");
            res = new HeapPage(h_id, bytes);
            myfile.close();
            return res;
        } catch (IOException e) {
            e.printStackTrace();
        }

        return res;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1

        // 获取pageNumber
        byte []data=new byte[BufferPool.getPageSize()];
        int pageNo=page.getId().getPageNumber();
        if(pageNo>numPages()) {
            throw new IllegalArgumentException("page wrong！");
        }
        // 写入file
        RandomAccessFile raf=new RandomAccessFile(f,"rw");
        int offset=pageNo*BufferPool.getPageSize();
        raf.seek(offset);

        data=page.getPageData();
        raf.write(data);
        raf.close();

    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here

        //file按页依次存储，没有多余信息
        int num = (int) Math.floor((double)f.length() / BufferPool.getPageSize());
        return num;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1

        //创建返回列表
        ArrayList<Page> res=new ArrayList<Page>();

        //通过BufferPool遍历所有page，若找到空位即插入
        for(int i=0;i<numPages();i++){
            HeapPageId tempId=new HeapPageId(getId(),i);
            HeapPage tempPage=(HeapPage) Database.getBufferPool().getPage(tid,tempId,Permissions.READ_WRITE);
            if(tempPage.getNumEmptySlots()!=0) {//非全满时
                //在当前页插入
                tempPage.insertTuple(t);
                res.add(tempPage);
                return res;
            }
            //无空闲slot时，释放当前事务的锁，让其他事务继续
            else
                Database.getBufferPool().releasePage(tid,tempId);
        }


        //新建一页
        RandomAccessFile raf=new RandomAccessFile(getFile(),"rw");
        int offset=numPages()*BufferPool.getPageSize();// 从尾部追加
        raf.seek(offset);
        byte[] emptyPageData=HeapPage.createEmptyPageData();
        raf.write(emptyPageData);
        raf.close();
        // 拿出新的一页做插入
        HeapPageId pid=new HeapPageId(this.getId(),numPages()-1);
        HeapPage page=(HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        page.insertTuple(t);
        res.add(page);

        return res;
        //若所有page都满，则创建新page，并插入tuple
        //新建页序号


        //确定Page

        //insert元组并返回列表


    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for lab1

        //创建返回列表
        ArrayList<Page> res=new ArrayList<Page>();

//        //通过BufferPool遍历页寻找
//        for(int i=0;i<BufferPool.getPageSize();i++) {
//            HeapPageId tempId=new HeapPageId(getId(),i);
//            HeapPage tempPage=(HeapPage)Database.getBufferPool().getPage(tid,tempId,Permissions.READ_WRITE);
//
//        }
        //直接根据tuple的RecordId找到对应页
        HeapPageId pid=(HeapPageId) t.getRecordId().getPageId();
        HeapPage page=(HeapPage) Database.getBufferPool().getPage(tid,pid,Permissions.READ_WRITE);
        //调用该页的delete函数
        page.deleteTuple(t);

        res.add(page);
        return res;


    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator1(this, tid);//内部类
    }

    private static final class HeapFileIterator1 implements DbFileIterator {
        private final HeapFile heapFile;
        TransactionId tid;//请求的事务id
        Permissions permissions;
        BufferPool bufferPool = Database.getBufferPool();
        Iterator<Tuple> iterator;
        int num = 0;//当前读取的页数

        public HeapFileIterator1(HeapFile hf, TransactionId tid) {
            super();
            this.tid = tid;
            heapFile = hf;
        }

        //open后迭代器默认先读取第0页
        public void open() throws DbException, TransactionAbortedException {
            // 获取第一页的全部元组
            num = 0;
            //iterator = getPageTuple(num);
            HeapPageId temp=new HeapPageId(heapFile.getId(),num);//tableId有，pageNo有
            iterator=((HeapPage)Database.getBufferPool().getPage(tid, temp, Permissions.READ_ONLY)).iterator();

        }

        // 获取当前页的所有行,返回temp_page的iterator
//        private Iterator<Tuple> getPageTuple(int pageNumber) throws TransactionAbortedException, DbException {
//            if(pageNumber<0||pageNumber>= heapFile.numPages())
//                throw new DbException(String.format("heapfile %d does not contain page %d!", pageNumber,heapFile.getId()));
//            //确定当前page
//            int tableId = heapFile.getId();
//            HeapPageId pageId = new HeapPageId(tableId, pageNumber);
//            HeapPage temp_page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_ONLY);
//            //返回该page的数组迭代器
//            return temp_page.iterator();
//            //            if(temp_page!=null) {
////                return temp_page.iterator();
////            }
////            else
////                return null;
//
//        }

        //判断是否还要继续读取
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (iterator == null)
                return false;
            else {
                if (!iterator.hasNext()){
                    //页的元组迭代器表示无下一行时
                    if(num <(heapFile.numPages()-1)){
//                        //若当前页不是最后一页，指向下一页的元组迭代器
//                        num++;
//                        iterator= getPageTuple(num);
//                        return true;
                        num++;
                        HeapPageId temp=new HeapPageId(heapFile.getId(),num);
                        iterator=((HeapPage)Database.getBufferPool().getPage(tid, temp, Permissions.READ_ONLY)).iterator();
                        return iterator.hasNext();// deleteTest的时候报错，NoSuchElement 找到这来，确实需要再判断新开的一页是否还有next


                    }else{
                        return false;
                    }
                }
                else//有下一行时返回true
                    return true;
            }
        }

        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {

            // 返回下一个元组
            if(hasNext())
                return iterator.next();
            throw new NoSuchElementException("no");
        }

        public void rewind() throws DbException, TransactionAbortedException {
//            // 清除上一个迭代器
//            close();
//            // 重新开始
//            open();
            num=0;
            HeapPageId temp=new HeapPageId(heapFile.getId(),num);
            iterator=((HeapPage)Database.getBufferPool().getPage(tid, temp, Permissions.READ_ONLY)).iterator();

        }
        public void close() {
            iterator = null;
            num=0;
        }
    }

}


