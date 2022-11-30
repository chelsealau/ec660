package simpledb;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {
	private int m_min, m_max;
	private int numB; // tot num of buckets
	private int ntups; // total num of tuples
	private int h_b; // height of bucket
	private int w_b; // width of bucket
	private int[] hist; // stores number of values in each bucket

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
    	this.numB = buckets;
    	this.m_min = min;
    	this.m_max = max;
    	this.ntups = 0;
    	// calculate width of bucket 
    	this.w_b = (int) Math.ceil((double) (max-min+1)/buckets); 
    	this.hist = new int[buckets];
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
    	
    	// determine which bucket value belongs to
    	if ((m_min > v) || (v > m_max)) {
    		return;
    	}
    	int b = (v == m_max) ? (numB - 1) : (v - m_min)/w_b;
        hist[b]++; 
        ntups++;
    }
    
    // helper method to calculate sum of values in buckets starting from start_b to end_b
    private int calcSum(int start_b, int end_b) {
    	int sum = 0;
    	for (int i = start_b; i < end_b; i++) {
    		sum += hist[i];
    	}
    	
    	return sum;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {

    	// some code goes here

    	double selectivity = 0.0;
    	int b = (v == m_max) ? (numB - 1) : (v - m_min)/w_b;
    	
    	h_b = (v >= m_min && v < m_max) ? hist[b] : 0;
    	
    	double b_f = (double) h_b/ntups;
    	int b_right = (b + 1)*w_b + m_min;
    	int b_left = b*w_b + m_min;
    	double b_part = 0.0;
    	
    	switch(op) {
    		case EQUALS:
    			selectivity = (double) (h_b/w_b)/ntups; // CAN REPLACE WITH B_F
    			break;
    		case NOT_EQUALS:
    			selectivity = 1.0 - (double) (h_b/w_b)/ntups; // CAN REPLACE WITH B_F
    			break;
    		case GREATER_THAN_OR_EQ:
    			if (m_min >= v) {
    	    		return 1.0;
    	    	} else if (v >= m_max) {
    	    		return 0.0;
    	    	}
    			b_part = (b_right - v + 1)/w_b;
    			selectivity = b_part*b_f + (double) calcSum(b+1,numB)/ntups;
    			break;
    		case GREATER_THAN:
    			if (m_min > v) {
    	    		return 1.0;
    	    	} else if (v > m_max) {
    	    		return 0.0;
    	    	}
    			
    			b_part = (b_right - v)/w_b;
    			selectivity = b_part*b_f + (double) calcSum(b+1,numB)/ntups;
    			break;
    		case LESS_THAN_OR_EQ:
    			if (m_min >= v) {
    	    		return 0.0;
    	    	} else if (v >= m_max) {
    	    		return 1.0;
    	    	}
    			
    			b_part = (v - b_left + 1)/w_b;
    			selectivity = b_part*b_f + (double) calcSum(0,b)/ntups;
    			break;
    		case LESS_THAN:
    			if (m_min > v) {
    	    		return 0.0;
    	    	} else if (v > m_max) {
    	    		return 1.0;
    	    	}
    			
    			b_part = (v - b_left)/w_b;
    			selectivity = b_part*b_f + (double) calcSum(0,b)/ntups;
    			break;
    		
    	}
        return selectivity;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return null;
    }
}
