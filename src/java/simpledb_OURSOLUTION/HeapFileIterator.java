package simpledb_OURSOLUTION;

import java.util.Iterator;

import simpledb.AbstractDbFileIterator;
import simpledb.Database;
import simpledb.DbException;
import simpledb.HeapFile;
import simpledb.HeapPage;
import simpledb.HeapPageId;
import simpledb.PageId;
import simpledb.Permissions;
import simpledb.TransactionAbortedException;
import simpledb.TransactionId;
import simpledb.Tuple;

/**
 * HeapFileIterator
 */
public class HeapFileIterator extends AbstractDbFileIterator {

  // Members to access data
  private final HeapFile heapFile;
  private final TransactionId tid;
  private Iterator<Tuple> tupleIterator;
  private int currPageNum;

  HeapFileIterator(HeapFile hf, TransactionId tid) {
    this.heapFile = hf;
    this.tid = tid;
  }

  @Override
  public void open() throws DbException, TransactionAbortedException {
    currPageNum = 0;
    final PageId pid = new HeapPageId(heapFile.getId(), currPageNum);
    final HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
    tupleIterator = page.iterator();
  }

  @Override
  public void close() {
    tupleIterator = null;
    super.close();
    System.out.println("Number of pages in HeapFile: " + heapFile.numPages());
  }

  @Override
  public void rewind() throws DbException, TransactionAbortedException {
    open();
  }

  @Override
  protected Tuple readNext() throws DbException, TransactionAbortedException {
    if (tupleIterator == null) {
      return null;
    }

    if (tupleIterator.hasNext()) {
      return tupleIterator.next();
    } else {
      tupleIterator = moveToNextPage();
    }
    return readNext();
  }

  private Iterator<Tuple> moveToNextPage() throws DbException, TransactionAbortedException {
    if (currPageNum + 1 < heapFile.numPages()) {
      currPageNum++;
      final PageId pid = new HeapPageId(heapFile.getId(), currPageNum);
      final HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
      return page.iterator();
    } else {
      return null;
    }
  }
}