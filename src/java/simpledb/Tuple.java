package simpledb;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private Field[] tup;
    private RecordId rid;
    private TupleDesc thisTD;
    private static final long serialVersionUID = 1L;

    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td
     *           the schema of this tuple. It must be a valid TupleDesc
     *           instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        // some code goes here
        thisTD = td;
        tup = new Field[td.numFields()];
        for (int i = 0; i < td.numFields(); i++) {
            Type type = td.getFieldType(i);
            if (type == Type.INT_TYPE) {
                tup[i] = new IntField(0);
            }
            if (type == Type.STRING_TYPE) {
                tup[i] = new StringField("", Type.STRING_LEN);
            }
        }

    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return thisTD;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        // some code goes here (__done__)
        return rid;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        // some code goes here (__done__)
        this.rid = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i
     *          index of the field to change. It must be a valid index.
     * @param f
     *          new value for the field.
     * 
     * @throws IllegalArgumentException
     *                                  If the Field's type being passed doesn't
     *                                  match the TupleDesc's type
     * @throws NoSuchElementException
     *                                  If the index is out of bounds
     */
    public void setField(int i, Field f) throws IllegalArgumentException, NoSuchElementException {
        // some code goes here (__done__)
        Type currFieldType = thisTD.getFieldType(i);
        if (currFieldType != f.getType()) {
            throw new IllegalArgumentException("Field type does not match TupleDesc type");
        }
        tup[i] = f;
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     *
     * @param i
     *          field index to return. Must be a valid index.
     * 
     * @throws NoSuchElementException If the index is out of bounds
     */
    public Field getField(int i) throws NoSuchElementException {
        // some code goes here (__done__)
        if (i < 0 || i >= thisTD.numFields()) {
            throw new NoSuchElementException("Invalid field index");
        }
        return tup[i];
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     *
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     *
     * where \t is any whitespace (except a newline)
     */
    public String toString() {
        // some code goes here (__done__)
        String tupString = "";
        Iterator<Field> itr = this.fields();
        while (itr.hasNext()) {
            tupString += itr.next().toString();
            if (itr.hasNext()) {
                tupString += ' ';
            }
        }
        return tupString;
    }

    /**
     * @return
     *         An iterator which iterates over all the fields of this tuple
     */
    public Iterator<Field> fields() {
        // some code goes here (__done__)
        Iterator<Field> fieldIterator = new Iterator<Field>() {
            private int currentIndex = 0;

            @Override
            public boolean hasNext() {
                return thisTD.numFields() > currentIndex;
            }

            @Override
            public Field next() {
                return tup[currentIndex++];
            }
        };

        return fieldIterator;
    }

    /**
     * reset the TupleDesc of this tuple
     */
    public void resetTupleDesc(TupleDesc td) {
        // some code goes here (__done__)
        this.thisTD = td;
        tup = new Field[td.numFields()];
        for (int i = 0; i < td.numFields(); i++) {
            Type type = td.getFieldType(i);
            if (type == Type.INT_TYPE) {
                tup[i] = new IntField(0);
            }
            if (type == Type.STRING_TYPE) {
                tup[i] = new StringField("", Type.STRING_LEN);
            }
        }
    }
}
