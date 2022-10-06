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
  private int tuplePos = 0;

  private HeapPageId currPageId;
  private HeapPage currPage;

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
    tuplePos = 0;
    currPageId = new HeapPageId(hf.getId(), currPageNum);
    currPage = (HeapPage) Database.getBufferPool().getPage(tid, currPageId, Permissions.READ_ONLY);
  }

  @Override
  protected Tuple readNext() throws DbException, TransactionAbortedException {
    if (!isOpen) {
      return null;
    }

    // Initialize currPage if this is the first time calling readNext()
    if (currPage == null) {
      currPage = (HeapPage) Database.getBufferPool().getPage(tid, currPageId, Permissions.READ_ONLY);
    }

    if (tuplePos > currPage.tuples.length) {
      currPageNum++;
      if (currPageNum > hf.numPages()) {
        return null;
      }
      currPageId = new HeapPageId(hf.getId(), currPageNum);
      currPage = (HeapPage) Database.getBufferPool().getPage(tid, currPageId, Permissions.READ_ONLY);
      tuplePos = 0;
    }

    return currPage.tuples[tuplePos++];
  }
}