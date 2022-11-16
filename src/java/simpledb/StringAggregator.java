package simpledb;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;

    private ConcurrentHashMap<Field, Integer> aggregateMap;

    /**
     * Aggregate constructor
     * 
     * @param gbfield     the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or
     *                    null if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here (__done__)
        if (what != Op.COUNT) {
            throw new IllegalArgumentException("Only COUNT is supported");
        }

        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;

        this.aggregateMap = new ConcurrentHashMap<Field, Integer>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here (__done__)
        final Field groupByField = gbfield == NO_GROUPING ? new StringField("", 1) : tup.getField(gbfield);
        this.aggregateMap.merge(groupByField, 1, Integer::sum);
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *         aggregateVal) if using group, or a single (aggregateVal) if no
     *         grouping. The aggregateVal is determined by the type of
     *         aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        // some code goes here (__done__)
        return new TupleIterator(new TupleDesc(new Type[] { this.gbfieldtype, Type.INT_TYPE }),
                this.aggregateMap.entrySet().stream().map(entry -> {
                    final Tuple tuple = new Tuple(new TupleDesc(
                            new Type[] { this.gbfieldtype, Type.INT_TYPE }));
                    tuple.setField(0, entry.getKey());
                    tuple.setField(1, new IntField(entry.getValue()));
                    return tuple;
                }).collect(java.util.stream.Collectors.toList()));
    }

}
