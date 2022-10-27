package simpledb_OURSOLUTION;

import java.util.*;

import simpledb_OURSOLUTION.DbException;
import simpledb_OURSOLUTION.DbIterator;
import simpledb_OURSOLUTION.TransactionAbortedException;

/**
 * IndexDBIterator is the interface that index access methods
 * implement in SimpleDb.
 */
public interface IndexDbIterator extends DbIterator {
    /**
     * Open the access method such that when getNext is called, it
     * iterates through the tuples that satisfy ipred.
     * 
     * @param ipred The predicate that is used to scan the index.
     */
    public void open(IndexPredicate ipred)
            throws NoSuchElementException, DbException, TransactionAbortedException;

    /**
     * Begin a new index scan with the specified predicate.
     * 
     * @param ipred The predicate that is used to scan the index.
     */
    public void rewind(IndexPredicate ipred)
            throws DbException, TransactionAbortedException;
}
