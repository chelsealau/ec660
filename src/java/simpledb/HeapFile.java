package simpledb;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

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

    private File systemFile;
    private TupleDesc fileTupleDesc;
    private ConcurrentHashMap<HeapPageId, HeapPage> pageDirectory;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *          the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        systemFile = f;
        fileTupleDesc = td;
        pageDirectory = new ConcurrentHashMap<HeapPageId, HeapPage>();

        FileInputStream sysFileReader = null;
        byte[] pageBuffer = HeapPage.createEmptyPageData();
        int allocatedPages = 0;
        try {
            sysFileReader = new FileInputStream(f);
            // Ensure the file is not empty
            if (sysFileReader.available() > 0) {
                // Read a page's worth of data into the bucket
                while (sysFileReader.read(pageBuffer) != -1) {
                    HeapPageId newPageId = new HeapPageId(getId(), allocatedPages);
                    HeapPage newPage = new HeapPage(newPageId, pageBuffer);
                    pageDirectory.put(newPageId, newPage);
                    allocatedPages++;
                }
            }
            sysFileReader.close();
        } catch (Exception e) {
            if (sysFileReader != null) {
                try {
                    sysFileReader.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
            }
            e.printStackTrace(); // Should never happen in this simpleDB
        }
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here (__done__)
        return systemFile;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here (__done__)
        return systemFile.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here (__done__)
        return fileTupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) throws IllegalArgumentException {
        // some code goes here (__done__)
        if (!pageDirectory.containsKey(pid)) {
            throw new IllegalArgumentException("PageId " + pid + " does not exist in this HeapFile");
        }
        return pageDirectory.get(pid);
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return pageDirectory.size();
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(this, tid);
    }

}
