package simpledb;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;

    private ConcurrentHashMap<Field, Integer[]> aggregateMap;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *                    the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *                    the type of the group by field (e.g., Type.INT_TYPE), or
     *                    null
     *                    if there is no grouping
     * @param afield
     *                    the 0-based index of the aggregate field in the tuple
     * @param what
     *                    the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here (__done__)
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;

        this.aggregateMap = new ConcurrentHashMap<Field, Integer[]>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        final Field groupByField = gbfield == NO_GROUPING ? new IntField(NO_GROUPING) : tup.getField(gbfield);

        switch (this.what) {
            case MIN:
                this.aggregateMap.merge(groupByField, new Integer[] { ((IntField) tup.getField(afield)).getValue(), 1 },
                        (Integer[] oldValue, Integer[] newValue) -> {
                            oldValue[0] = Math.min(oldValue[0], newValue[0]);
                            oldValue[1] += newValue[1];
                            return oldValue;
                        });
                break;
            case MAX:
                this.aggregateMap.merge(groupByField, new Integer[] { ((IntField) tup.getField(afield)).getValue(), 1 },
                        (Integer[] oldValue, Integer[] newValue) -> {
                            oldValue[0] = Math.max(oldValue[0], newValue[0]);
                            oldValue[1] += newValue[1];
                            return oldValue;
                        });
                break;
            case SUM:
            case SUM_COUNT:
            case AVG:
            case SC_AVG:
                this.aggregateMap.merge(groupByField, new Integer[] { ((IntField) tup.getField(afield)).getValue(), 1 },
                        (Integer[] oldValue,
                                Integer[] newValue) -> {
                            oldValue[0] += newValue[0];
                            oldValue[1] += newValue[1];
                            return oldValue;
                        });
                break;
            case COUNT:
                this.aggregateMap.merge(groupByField, new Integer[] { 1, 1 },
                        (Integer[] oldValue, Integer[] newValue) -> {
                            oldValue[0] += newValue[0];
                            oldValue[1] += newValue[1];
                            return oldValue;
                        });
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + this.what);
        }
    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
        // some code goes here (__done__)
        return new TupleIterator(new TupleDesc(new Type[] { this.gbfieldtype, Type.INT_TYPE }),
                this.aggregateMap.entrySet().stream().map(entry -> {
                    final Tuple tuple = new Tuple(new TupleDesc(
                            new Type[] { this.gbfieldtype, Type.INT_TYPE }));
                    tuple.setField(0, entry.getKey());
                    if (this.what == Op.AVG) {
                        tuple.setField(1, new IntField(entry.getValue()[0] / entry.getValue()[1]));
                    } else {
                        tuple.setField(1, new IntField(entry.getValue()[0]));
                    }
                    return tuple;
                }).collect(java.util.stream.Collectors.toList()));
    }
}
