package uis.cipsi.rdd.opentsdb;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.util.StringUtils;

/**
 * Convert HBase tabular data into a format that is consumable by Map/Reduce.
 */
public class TSDBInputFormat extends TableInputFormat implements Configurable {
    private final Log LOG = LogFactory.getLog(TSDBInputFormat.class);

    /**
     * Returns the current configuration.
     *
     * @return The current configuration.
     * @see org.apache.hadoop.conf.Configurable#getConf()
     */
    @Override
    public Configuration getConf() {
        return super.getConf();
    }

    /**
     * Specific configuration for the OPENTSDB tables {tsdb, tsdb-uid}
     *
     * @param configuration The configuration to set.
     * @see org.apache.hadoop.conf.Configurable#setConf(org.apache.hadoop.conf.Configuration)
     */
    @Override
    public void setConf(Configuration configuration) {
        super.setConf(configuration);
        Configuration conf = configuration;

        String tableName = conf.get(INPUT_TABLE);
        try {
            //	setHTable(new HTable(new Configuration(conf), tableName));
        } catch (Exception e) {
            LOG.error(StringUtils.stringifyException(e));
        }

        Scan scan = TSDBScan.createScan(conf);
        setScan(scan);
    }
}