package simpledb_OURSOLUTION;

import java.util.*;

import javax.xml.crypto.Data;

import simpledb_OURSOLUTION.Database;
import simpledb_OURSOLUTION.DbException;
import simpledb_OURSOLUTION.DbFileIterator;
import simpledb_OURSOLUTION.DbIterator;
import simpledb_OURSOLUTION.HeapFile;
import simpledb_OURSOLUTION.TransactionAbortedException;
import simpledb_OURSOLUTION.TransactionId;
import simpledb_OURSOLUTION.Tuple;
import simpledb_OURSOLUTION.TupleDesc;
import simpledb_OURSOLUTION.Type;
import simpledb_OURSOLUTION.TupleDesc.TDItem;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements DbIterator {

    private static final long serialVersionUID = 1L;

    private TransactionId tid;
    private int tableid;
    private String tableAlias;
    private HeapFile tableFile;
    private DbFileIterator tableIterator;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid
     *                   The transaction this scan is running as a part of.
     * @param tableid
     *                   the table to scan.
     * @param tableAlias
     *                   the alias of this table (needed by the parser); the
     *                   returned
     *                   tupleDesc should have fields with name tableAlias.fieldName
     *                   (note: this class is not responsible for handling a case
     *                   where
     *                   tableAlias or fieldName are null. It shouldn't crash if
     *                   they
     *                   are, but the resulting name can be null.fieldName,
     *                   tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        // some code goes here
        this.tid = tid;
        this.tableid = tableid;
        this.tableAlias = tableAlias;
        this.tableFile = (HeapFile) Database.getCatalog().getDatabaseFile(tableid);
        this.tableIterator = tableFile.iterator(tid);
    }

    /**
     * @return
     *         return the table name of the table the operator scans. This should
     *         be the actual name of the table in the catalog of the database
     */
    public String getTableName() {
        // some code goes here (__done__)
        return Database.getCatalog().getTableName(tableid);
    }

    /**
     * @return Return the alias of the table this operator scans.
     */
    public String getAlias() {
        // some code goes here (__done__)
        return tableAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * 
     * @param tableid
     *                   the table to scan.a
     * @param tableAlias
     *                   the alias of this table (needed by the parser); the
     *                   returned
     *                   tupleDesc should have fields with name tableAlias.fieldName
     *                   (note: this class is not responsible for handling a case
     *                   where
     *                   tableAlias or fieldName are null. It shouldn't crash if
     *                   they
     *                   are, but the resulting name can be null.fieldName,
     *                   tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        this.tableid = tableid;
        this.tableAlias = tableAlias;
        this.tableFile = (HeapFile) Database.getCatalog().getDatabaseFile(tableid);
        this.tableIterator = tableFile.iterator(tid);
    }

    public SeqScan(TransactionId tid, int tableid) {
        this(tid, tableid, Database.getCatalog().getTableName(tableid));
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here (__done__)
        tableIterator.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name. The alias and name should be separated with a "." character
     * (e.g., "alias.fieldName").
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here (__done__)
        TupleDesc original = this.tableFile.getTupleDesc();
        Iterator<TupleDesc.TDItem> fieldIt = original.iterator();
        while (fieldIt.hasNext()) {
            TupleDesc.TDItem currTDItem = fieldIt.next();
            String newFieldName = tableAlias + "." + currTDItem.fieldName;
            Type newFieldType = currTDItem.fieldType;
            TupleDesc.TDItem newTDItem = new TupleDesc.TDItem(newFieldType, newFieldName);
            currTDItem = newTDItem;
        }

        return original;
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        // some code goes here (__done__)
        return tableIterator.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here (__done__)
        return tableIterator.next();
    }

    public void close() {
        // some code goes here (__done__)
        tableIterator.close();
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here (__done__)
        tableIterator.rewind();
    }
}
