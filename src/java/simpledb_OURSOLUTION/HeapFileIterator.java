package simpledb_OURSOLUTION;

import java.util.Iterator;

import simpledb_OURSOLUTION.AbstractDbFileIterator;
import simpledb_OURSOLUTION.Database;
import simpledb_OURSOLUTION.DbException;
import simpledb_OURSOLUTION.HeapFile;
import simpledb_OURSOLUTION.HeapPage;
import simpledb_OURSOLUTION.HeapPageId;
import simpledb_OURSOLUTION.PageId;
import simpledb_OURSOLUTION.Permissions;
import simpledb_OURSOLUTION.TransactionAbortedException;
import simpledb_OURSOLUTION.TransactionId;
import simpledb_OURSOLUTION.Tuple;

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