package simpledb_OURSOLUTION;

import java.io.Serializable;
import java.util.*;

import simpledb_OURSOLUTION.TupleDesc;
import simpledb_OURSOLUTION.Type;

import java.lang.instrument.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     */
    // MAKE ARRAY OF TD ITEMS - might have to change access parameters for this
    private TDItem[] tdArray;

    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         */
        public final Type fieldType;

        /**
         * The name of the field
         */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldType + "(" + fieldName + ")";
        }
    }

    /**
     * @return
     *         An iterator which iterates over all the field TDItems
     *         that are included in this TupleDesc
     */
    public Iterator<TDItem> iterator() {
        // some code goes here
        Iterator<TDItem> tdIterator = new Iterator<TDItem>() {
            private int currentIndex = 0;

            @Override
            public boolean hasNext() {
                return numFields() > currentIndex;
            }

            @Override
            public TDItem next() {
                return tdArray[currentIndex++];
            }
        };

        return tdIterator;
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *                array specifying the number of and types of fields in this
     *                TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *                array specifying the names of the fields. Note that names may
     *                be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here

        // initialize array of TDItems to be same size as typeAr array
        this.tdArray = new TDItem[typeAr.length];

        // initialize each TDItem with type and field name
        for (int i = 0; i < typeAr.length; i++) {
            this.tdArray[i] = new TDItem(typeAr[i], fieldAr[i]);
        }

    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *               array specifying the number of and types of fields in this
     *               TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here

        // initialize array of TDItems to be same size as typeAr array
        this.tdArray = new TDItem[typeAr.length];

        // initialize each TDItem with type and empty field name
        for (int i = 0; i < typeAr.length; i++) {
            this.tdArray[i] = new TDItem(typeAr[i], "");
        }

    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        if (this.tdArray == null) {
            return 0;
        } else {
            return this.tdArray.length;
        }
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *          index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *                                if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        if (i < 0 || i >= numFields()) {
            throw new NoSuchElementException("Invalid field index");
        }
        return tdArray[i].fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *          The index of the field to get the type of. It must be a valid
     *          index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *                                if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        if (i < 0 || i >= numFields()) {
            throw new NoSuchElementException("Invalid field index");
        }
        return tdArray[i].fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *             name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *                                if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        // check if searched field name is null
        if (name == null || tdArray == null) {
            throw new NoSuchElementException();
        }
        // search for field name
        for (int i = 0; i < tdArray.length; i++) {
            if (tdArray[i].fieldName.equals(name)) {
                return i;
            }
        }
        // throw exception if nothing is found
        throw new NoSuchElementException();

    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here (__done__)
        int size = 0;
        for (int i = 0; i < tdArray.length; i++) {
            size += tdArray[i].fieldType.getLen();
        }
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        int totalLen = td1.tdArray.length + td2.tdArray.length;
        Type[] emptyTypeArray = new Type[totalLen];
        TupleDesc newTD = new TupleDesc(emptyTypeArray); // WHAT DO I INITIALIZE THIS WITH?????? ////////
        newTD.tdArray = new TDItem[totalLen];

        // insert each TDItem from td1 into new TupleDesc's array
        for (int i = 0; i < td1.tdArray.length; i++) {
            newTD.tdArray[i] = td1.tdArray[i];
        }

        // insert each TDItem from td2 into new TupleDesc's array
        for (int j = td1.tdArray.length; j < totalLen; j++) {
            newTD.tdArray[j] = td2.tdArray[j - td1.tdArray.length];
        }

        return newTD;
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     * 
     * @param o
     *          the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        // some code goes here
        // check if object o is the same object
        if (o == this) {
            return true;
        }
        // check if object o is null or a different class
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TupleDesc other = (TupleDesc) o;
        if (tdArray.length != other.tdArray.length) {
            return false;
        }
        for (int i = 0; i < tdArray.length; i++) {
            if (tdArray[i].fieldName != other.tdArray[i].fieldName ||
                    tdArray[i].fieldType != other.tdArray[i].fieldType) {
                return false;
            }
        }
        // if (Arrays.equals(this.tdArray, other.tdArray)) {
        // return true;
        // }
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        // use iterator to add all tdItems into list or something then print them all?
        String desc = "";
        Iterator<TDItem> itr = this.iterator();
        while (itr.hasNext()) {
            desc += itr.next().toString();
            if (itr.hasNext()) {
                desc += ", ";
            }
        }
        return desc;
    }
}
