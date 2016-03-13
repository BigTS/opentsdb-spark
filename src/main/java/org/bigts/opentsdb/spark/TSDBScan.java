package org.bigts.opentsdb.spark;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.util.StringUtils;

import java.nio.charset.Charset;
import java.util.regex.Pattern;

public class TSDBScan {

    private static final Log LOG = LogFactory.getLog(TSDBInputFormat.class);

    /**
     * The starting timestamp used to filter columns with a specific range of
     * versions.
     */
    public static final String SCAN_TIMERANGE_START = "opentsdb.timerange.start";
    /**
     * The ending timestamp used to filter columns with a specific range of
     * versions.
     */
    public static final String SCAN_TIMERANGE_END = "opentsdb.timerange.end";

    public static final String TSDB_UIDS = "net.opentsdb.tsdb.uid";
    /**
     * The opentsdb metric to be retrived.
     */
    public static final String METRICS = "net.opentsdb.rowkey";
    /**
     * The opentsdb metric to be retrived.
     */
    public static final String TAGKV = "net.opentsdb.tagkv";
    /**
     * The tag keys for the associated metric (space seperated).
     */
    public static final String TSDB_STARTKEY = "net.opentsdb.start";
    /**
     * The tag values for the tag keys (space seperated).
     */
    public static final String TSDB_ENDKEY = "net.opentsdb.end";

    /** Set to false to disable server-side caching of blocks for this scan. */
    public static final String SCAN_CACHEBLOCKS = "hbase.mapreduce.scan.cacheblocks";

    public static Scan createScan(Configuration conf) {
        Scan scan = null;
        try {
            scan = new Scan();
            // Configuration for extracting the UIDs for the user specified
            // metric and tag names.
            if (conf.get(TSDB_UIDS) != null) {
                // We get all uids for all specified column quantifiers
                // (metrics|tagk|tagv)
                String pattern = String.format("^(%s)$", conf.get(TSDB_UIDS));
                RegexStringComparator keyRegEx = new RegexStringComparator(pattern);
                RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, keyRegEx);
                scan.setFilter(rowFilter);
            } else {
                // Configuration for extracting & filtering the required rows
                // from tsdb table.
                if (conf.get(METRICS) != null) {
                    String name;
                    if (conf.get(TAGKV) != null) // If we have to extract based
                        // on a metric and its group
                        // of tags "^%s.{4}.*%s.*$"
                        name = String.format("^%s.*%s.*$", conf.get(METRICS), conf.get(TAGKV));
                    else
                        // If we have to extract based on just the metric
                        name = String.format("^%s.+$", conf.get(METRICS));

                    RegexStringComparator keyRegEx = new RegexStringComparator(name, Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
                    keyRegEx.setCharset(Charset.forName("ISO-8859-1"));
                    RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, keyRegEx);
                    scan.setFilter(rowFilter);
                    //scan.setFilter(rowFilter);
                }
                // Extracts data based on the supplied timerange. If timerange
                // is not provided then all data are extracted
                if (conf.get(SCAN_TIMERANGE_START) != null) {
                    String startRow = conf.get(METRICS) + conf.get(SCAN_TIMERANGE_START) + (conf.get(TAGKV) != null ? conf.get(TAGKV) : "");
                    scan.setStartRow(hexStringToByteArray(startRow));
                }

                if (conf.get(SCAN_TIMERANGE_END) != null) {
                    String endRow = conf.get(METRICS) + conf.get(SCAN_TIMERANGE_END) + (conf.get(TAGKV) != null ? conf.get(TAGKV) : "");
                    scan.setStopRow(hexStringToByteArray(endRow));
                }
            }
            // false by default, full table scans generate too much BC churn
            scan.setCacheBlocks((conf.getBoolean(SCAN_CACHEBLOCKS, false)));
        } catch (Exception e) {
            LOG.error(StringUtils.stringifyException(e));
        }

        return scan;
    }

    public static byte[] hexStringToByteArray(String s) {
        s = s.replace("\\x", "");
        byte[] b = new byte[s.length() / 2];
        for (int i = 0; i < b.length; i++) {
            int index = i * 2;
            int v = Integer.parseInt(s.substring(index, index + 2), 16);
            b[i] = (byte) v;
        }
        return b;
    }


}
