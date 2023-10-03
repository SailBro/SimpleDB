package simpledb;

import java.io.*;
import java.util.LinkedList;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 *
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    /** Default number of pages passed to the constructor. This is used by
     other classes. BufferPool should use the numPages argument to the
     constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private final int numPages;//缓存池中的页数
    private final ConcurrentHashMap<Integer,Page> pageStore;
    //哈希表存放key和value(page),同catalog中的hashTable
    private LinkedList<PageId> pageOrder;
    //锁管理
    private LockManager lockManager;


    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here

        this.numPages=numPages;
        pageStore=new ConcurrentHashMap<Integer, Page>();
        pageOrder=new LinkedList<PageId>();
        //新增锁管理器
        lockManager=new LockManager();
    }

    public static int getPageSize() {
        return pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
        BufferPool.pageSize = pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
        BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
        // some code goes here


        //lab4
        //先判断需要获取的锁的类型
        int lockType=perm==Permissions.READ_ONLY?PageLock.SHARE:PageLock.EXCLUSIVE;
        // 计算超时时间（设置为 500 ms）
        long startTime = System.currentTimeMillis();
        //循环判断是否可以加锁
        boolean isAcquired=false;//初始化不能
        while(!isAcquired){
            isAcquired=lockManager.acquiredLock(pid,tid,lockType);
            long now = System.currentTimeMillis();
            // 如果超过 500 ms没有获取就抛出异常
            if(now - startTime > 500){
                // 放弃当前事务
                throw new TransactionAbortedException();
                //break;
            }

        }
        //跳出循环表明可以加锁了，否则一直忙等待


        //lab1~3
        if(!pageStore.containsKey(pid.hashCode())){
            DbFile temp=Database.getCatalog().getDatabaseFile(pid.getTableId());
            Page page=temp.readPage(pid);
            pageStore.put(pid.hashCode(),page);
            pageOrder.add(pid);
        }
        else{
            pageOrder.remove(pid);
            pageOrder.add(pid);
        }
        //只用参数pid即可
        return pageStore.get(pid.hashCode());
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        lockManager.releaseLock(tid,pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        //lockManager.completeTranslation(tid);
        transactionComplete(tid,true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return lockManager.isHoldLock(tid,p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
            throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        //若提交，刷新页面->磁盘
        if(commit) {
            flushPages(tid);
        }
        else {
            //若回滚，还原事务对内存的所有改变
            //遍历所有页面
            for (Page p : pageStore.values()) {
                //找到该事务的脏页
                if (tid.equals(p.isDirty())) {
                    //获取磁盘中的原有状态
                    int tableId = p.getId().getTableId();
                    DbFile table = Database.getCatalog().getDatabaseFile(tableId);
                    Page pageInDisk = table.readPage(p.getId());
                    //写回内存
                    pageStore.put(p.getId().hashCode(), pageInDisk);
                }
            }
        }
        //事务完成
        lockManager.completeTranslation(tid);

    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1

        //确定heapFile
        DbFile heapFile = (DbFile) Database.getCatalog().getDatabaseFile(tableId);
        //调用file.insert
        ArrayList<Page> page = heapFile.insertTuple(tid, t);// 调用heapFile插入，返回修改过的page

        for(Page p:page){
            p.markDirty(true,tid);// 插入了 标记为脏数据
            if(pageStore.size()>=numPages) {// insufficient space
                //满时删除最不常用的页
                evictPage();
            }
            pageStore.put(p.getId().hashCode(),p);

            //从链表中删除
            pageOrder.remove(p.getId());
            //加入链表的最后端
            pageOrder.add(p.getId());

        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1

        //确定heapFile
        DbFile heapFile = (DbFile) Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        //调用file.delete
        ArrayList<Page> page = heapFile.deleteTuple(tid, t);// 调用heapFile删除，返回修改过的page

        for(Page p:page){
            p.markDirty(true,tid);// 插入了 标记为脏数据
            pageStore.put(p.getId().hashCode(),p);

            pageOrder.remove(p.getId());
            pageOrder.add(p.getId());
        }

    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1

        for(Page p:pageStore.values()) {// 调用flushPage去做
            flushPage(p.getId());
        }

    }

    /** Remove the specific page id from the buffer pool.
     Needed by the recovery manager to ensure that the
     buffer pool doesn't keep a rolled back page in its
     cache.

     Also used by B+ tree files to ensure that deleted pages
     are removed from the cache so they can be reused safely
     */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        pageStore.remove(pid.hashCode());
        //删除缓存
        pageOrder.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1

        Page page=pageStore.get(pid.hashCode());
        if(page.isDirty()!=null){
            // 写入脏页
            Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(page);
            // 移除脏页标签和事务标签
            page.markDirty(false, null);
        }
    }
    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2

        //事务提交后，刷新该事务的所有页面（缓存->提交至磁盘）
        //遍历pageStore中所有页
        for(Page p:pageStore.values())
            if(tid.equals(p.isDirty()))
                flushPage((p.getId()));
        //刷新tid==事务tid，且为脏的页
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException, IOException {
        // some code goes here
        // not necessary for lab1

        //取下第一页（最不常用）
        /*PageId pageId=pageOrder.getFirst();
        flushPage(pageId);
        discardPage(pageId);*/

        //脏页代表事务未完成，不能直接丢弃
        //找到一个非脏页丢弃即可
        for(Page p:pageStore.values()){
            if(p.isDirty()==null){
                //不是脏页，丢弃
                discardPage(p.getId());
                return;
            }
            //是脏页则寻找下一个
            else
                continue;
        }
        //到这里没返回说明全都是脏页，没有可以丢弃的
        throw new DbException("All Page Are Dirty Page");
    }

    // 锁
    class PageLock{
        private static final int SHARE = 0;
        private static final int EXCLUSIVE = 1;
        private TransactionId tid;
        private int type;
        public PageLock(TransactionId tid, int type){
            this.tid = tid;
            this.type = type;
        }
        public TransactionId getTid(){
            return tid;
        }
        public int getType(){
            return type;
        }
        public void setType(int type){
            this.type = type;
        }
    }

    //锁管理器
    class LockManager {
        //哈希表记录页和该页上锁的集合
        ConcurrentHashMap<PageId, ConcurrentHashMap<TransactionId, PageLock>> lockMap = new ConcurrentHashMap<>();

            //获取锁
            public synchronized boolean acquiredLock(PageId pageId, TransactionId tid, int requiredType) {
                //先判断该页是否有锁

                //无锁则直接添锁
                if(lockMap.get(pageId)==null) {
                    //创建锁（自定义类）——tid和pageLock对应
                    PageLock pageLock=new PageLock(tid,requiredType);
                    ConcurrentHashMap<TransactionId,PageLock> tpMap=new ConcurrentHashMap<TransactionId,PageLock>();
                    //放入该页的锁集合
                    tpMap.put(tid,pageLock);
                    lockMap.put(pageId,tpMap);
                    return true;
                }

                //该页有锁时
                //先获取该页的锁集合
                ConcurrentHashMap<TransactionId,PageLock> tpMap=lockMap.get(pageId);

                //检查当前事务是否在该页有锁
                //无该事务上的锁时
                if(tpMap.get(tid)==null){
                    //判断当前页的锁个数
                    //个数>1，肯定是共享锁
                    if(tpMap.size()>1){
                        //新建锁并加入哈希表，更新
                        PageLock newPageLock=new PageLock(tid,requiredType);
                        tpMap.put(tid,newPageLock);
                        lockMap.put(pageId,tpMap);
                            return true;
                    }

                    //个数=1
                    else if(tpMap.size()==1){
                        //找到唯一的锁，然后判断这个锁的类型
                        PageLock curLock=null;
                        for(PageLock lock:tpMap.values())
                            curLock=lock;

                        //判断旧锁的类型
                        //旧锁为排他锁时，不行
                        if(curLock.getType()==PageLock.EXCLUSIVE)
                            return false;
                        //旧锁为共享锁时
                        else if(curLock.getType()==PageLock.SHARE){
                            //判断当前锁的类型

                            //当前锁也是共享锁，则增添
                            if(requiredType==PageLock.SHARE){
                                //新建锁
                                PageLock newPageLock=new PageLock(tid,requiredType);
                                //从内向外更新哈希表
                                tpMap.put(tid,newPageLock);
                                lockMap.put(pageId,tpMap);
                                return true;
                            }
                            //当前锁时排他锁，不行
                            else if(requiredType == PageLock.EXCLUSIVE)
                                return false;
                        }
                    }

                }


                //当前事务在该页添加过锁
                else if(tpMap.get(tid)!=null){
                    //找到这个锁
                    PageLock newPageLock=tpMap.get(tid);

                    //判断旧锁类型
                    //旧锁为共享锁时
                    if(newPageLock.getType()==PageLock.SHARE){
                        //判断当前锁的类型
                        //共享直接，则返回
                        if(requiredType==PageLock.SHARE)
                            return true;
                        //当前锁不共享时
                        else if(requiredType==PageLock.EXCLUSIVE){
                            //判断页面的锁数
                            //只有1个锁时，旧锁就是当前锁
                            if(tpMap.size()==1){
                                //锁升级为写锁
                                newPageLock.setType(PageLock.EXCLUSIVE);
                                tpMap.put(tid,newPageLock);
                                return true;
                            }
                            //>1个锁时，自己本身是排他锁，也不能随意更改别的锁的属性
                            return false;
                        }
                    }
                    //旧锁为排他锁时，不用改了，本来就达到写锁要求了
                    else
                        return true;

                }


                return false;
            }//函数结束


        //判断事务是否有锁
        public synchronized boolean isHoldLock(TransactionId tid, PageId pageId){
            //先找到该页的锁集合
            ConcurrentHashMap<TransactionId, PageLock> locks=lockMap.get(pageId);
            //判断是否为空
            if(locks!=null&&locks.get(tid)!=null)
                return true;
            return false;
        }


        //释放锁
        public synchronized boolean releaseLock(TransactionId tid, PageId pageId){
            //先判断该事务有没有这个锁

            //先找到该页的锁集合
            ConcurrentHashMap<TransactionId, PageLock> locks=lockMap.get(pageId);
            //判断锁集合是否为空
            if(locks==null)
                return false;//为空肯定不行
            //判断锁集合中是否有当前事务的
            if(locks.get(tid)==null)
                return false;

            //存在，在哈希表中删除
            locks.remove(tid);
            //若释放锁后此时页没有锁了，需要将该页在哈希表中删除
            if(locks.size()==0)
                lockMap.remove(pageId);

            return true;
        }

        //释放事务在各页上的所有锁
        public synchronized void completeTranslation(TransactionId tid){
                //遍历哈希表中所存在的所有页的Id
                for(PageId pageId:lockMap.keySet())
                    releaseLock(tid,pageId);
        }



    }//锁管理器类
}
