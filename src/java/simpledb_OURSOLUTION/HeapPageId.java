package simpledb_OURSOLUTION;

import simpledb_OURSOLUTION.BufferPool;
import simpledb_OURSOLUTION.HeapPageId;
import simpledb_OURSOLUTION.PageId;

/** Unique identifier for HeapPage objects. */
public class HeapPageId implements PageId {

    private int tableId;
    private int pageNum;

    /**
     * Constructor. Create a page id structure for a specific page of a
     * specific table.
     *
     * @param tableId The table that is being referenced
     * @param pgNo    The page number in that table.
     */
    public HeapPageId(int tableId, int pgNo) {
        // some code goes here (__done__)
        this.tableId = tableId;
        this.pageNum = pgNo;
    }

    /** @return the table associated with this PageId */
    public int getTableId() {
        // some code goes here (__done__)
        return tableId;
    }

    /**
     * @return the page number in the table getTableId() associated with
     *         this PageId
     */
    public int pageNumber() {
        // some code goes here (__done__)
        return pageNum;
    }

    /**
     * @return a hash code for this page, represented by the concatenation of
     *         the table number and the page number (needed if a PageId is used as a
     *         key in a hash table in the BufferPool, for example.)
     * @see BufferPool
     */
    public int hashCode() {
        // some code goes here (__done__)
        // Will be string hashCode of "<tableId>_<pageNum>"
        return (Integer.toString(tableId) + "_" + Integer.toString(pageNum)).hashCode();
    }

    /**
     * Compares one PageId to another.
     *
     * @param o The object to compare against (must be a PageId)
     * @return true if the objects are equal (e.g., page numbers and table
     *         ids are the same)
     */
    public boolean equals(Object o) {
        // some code goes here
        if (o == this) {
            return true;
        }
        if (!(o instanceof HeapPageId)) {
            return false;
        }
        HeapPageId hpid = (HeapPageId) o;
        return this.hashCode() == hpid.hashCode();
    }

    /**
     * Return a representation of this object as an array of
     * integers, for writing to disk. Size of returned array must contain
     * number of integers that corresponds to number of args to one of the
     * constructors.
     */
    public int[] serialize() {
        int data[] = new int[2];

        data[0] = getTableId();
        data[1] = pageNumber();

        return data;
    }

}