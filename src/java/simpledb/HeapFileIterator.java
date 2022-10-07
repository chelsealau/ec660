package simpledb;

import java.util.Iterator;

/**
 * HeapFileIterator
 */
public class HeapFileIterator extends AbstractDbFileIterator {

  // Members to access data
  private HeapFile hf;
  private TransactionId tid;

  // State variables
  private boolean isOpen = false;
  private int currPageNum = 0;

  private HeapPageId currPageId;
  private HeapPage currPage;
  private Iterator<Tuple> tupleIterator;

  HeapFileIterator(HeapFile heapFile, TransactionId tid) {
    this.hf = heapFile;
    this.tid = tid;
  }

  @Override
  public void open() throws DbException, TransactionAbortedException {
    this.isOpen = true;
    this.currPageId = new HeapPageId(hf.getId(), currPageNum);
  }

  @Override
  public void close() {
    this.isOpen = false;
    super.close();
  }

  @Override
  public void rewind() throws DbException, TransactionAbortedException {
    if (!isOpen) {
      return;
    }

    currPageNum = 0;
    currPageId = new HeapPageId(hf.getId(), currPageNum);
    currPage = (HeapPage) Database.getBufferPool().getPage(tid, currPageId, Permissions.READ_ONLY);
    tupleIterator = currPage.iterator();
  }

  @Override
  protected Tuple readNext() throws DbException, TransactionAbortedException {
    if (!isOpen) {
      return null;
    }

    // Initialize currPage if this is the first time calling readNext()
    if (currPage == null) {
      currPage = (HeapPage) Database.getBufferPool().getPage(tid, currPageId, Permissions.READ_ONLY);
      tupleIterator = currPage.iterator();
    }

    if (tupleIterator.hasNext()) {
      System.out.println("tupleIterator.hasNext()");
      return tupleIterator.next();
    }

    // If we reach here, we have exhausted the current page
    System.out.println("EXHAUSTED PAGE... Max Pages: " + hf.numPages() + " | currPageNum: " + currPageNum);
    while (!tupleIterator.hasNext() && currPageNum < hf.numPages() - 1) {
      moveToNextPage();
    }
    if (currPageNum < hf.numPages() - 1) {
      return tupleIterator.next();
    }

    // No more tuples exist since the currPageNum >= hf.numPages() - 1
    return null;
  }

  private void moveToNextPage() throws DbException, TransactionAbortedException {
    currPageNum++;
    System.out.println("MOVING TO NEXT PAGE: " + currPageNum);
    currPageId = new HeapPageId(hf.getId(), currPageNum);
    currPage = (HeapPage) Database.getBufferPool().getPage(tid, currPageId, Permissions.READ_ONLY);
    tupleIterator = currPage.iterator();
  }
}