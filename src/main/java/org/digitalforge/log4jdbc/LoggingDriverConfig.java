package org.digitalforge.log4jdbc;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.digitalforge.log4jdbc.formatter.ParameterFormatter;

public class LoggingDriverConfig {
    
    private static final Logger log = LoggerFactory.getLogger(LoggingDriverConfig.class);

    private static final ParameterFormatter DEFAULT_PARAMETER_FORMATTER = new ParameterFormatter();

    /**
     * Maps driver class names to ParameterFormatter objects for each kind of
     * database.
     */
    private Map<String, ParameterFormatter> parameterFormatters = new HashMap<>();

    /**
     * Optional package prefix to use for finding application generating point of
     * SQL.
     */
    private String debugStackPrefix;

    /**
     * Flag to indicate debug trace info should be from the calling application
     * point of view (true if DebugStackPrefix is set.)
     */
    private boolean traceFromApplication;

    /**
     * Flag to indicate if a warning should be shown if SQL takes more than
     * SqlTimingWarnThresholdNanoSec nanoseconds to run. See below.
     */
    private boolean sqlTimingWarnThresholdEnabled;

    /**
     * An amount of time in nanoseconds for which SQL that executed taking this
     * long or more to run shall cause a warning message to be generated on the
     * SQL timing logger.
     *
     * This threshold will <i>ONLY</i> be used if SqlTimingWarnThresholdEnabled is
     * true.
     */
    private long sqlTimingWarnThresholdNanoSec;

    /**
     * Flag to indicate if an error should be shown if SQL takes more than
     * SqlTimingErrorThresholdNanoSec nanoseconds to run. See below.
     */
    private boolean sqlTimingErrorThresholdEnabled;

    /**
     * An amount of time in nanoseconds for which SQL that executed taking this
     * long or more to run shall cause an error message to be generated on the SQL
     * timing logger.
     *
     * This threshold will <i>ONLY</i> be used if SqlTimingErrorThresholdEnabled
     * is true.
     */
    private long sqlTimingErrorThresholdNanoSec;

    /**
     * Options to more finely control which types of SQL statements will be
     * dumped, when dumping SQL. By default all 5 of the following will be true.
     * If any one is set to false, then that particular type of SQL will not be
     * dumped.
     */
    private boolean dumpSqlSelect;
    private boolean dumpSqlInsert;
    private boolean dumpSqlUpdate;
    private boolean dumpSqlDelete;
    private boolean dumpSqlCreate;

    private boolean reportOriginalSql;
    private boolean shouldUseMarkersForTimingReports;

    // only true if one ore more of the above 4 flags are false.
    private boolean dumpSqlFilteringOn;

    /**
     * If dumping in debug mode, dump the full stack trace. This will result in a
     * VERY voluminous output, but can be very useful under some circumstances.
     */
    private boolean dumpFullDebugStackTrace;

    /**
     * Attempt to Automatically load a set of popular JDBC drivers?
     */
    private boolean autoLoadPopularDrivers;

    /**
     * Trim SQL before logging it?
     */
    private boolean trimSql;

    private List<String> drivers;

    LoggingDriverConfig() {

        Properties props = new Properties(System.getProperties());

        try(InputStream in = LoggingDriver.class.getResourceAsStream("/log4jdbc.properties")) {

            props.load(in);

            log.debug("log4jdbc.properties loaded from classpath");

        }
        catch(IOException ex) {
            log.debug("Exception loading log4jdbc.properties from classpath: " + ex.getMessage());
        }
        catch(NullPointerException ex) {
            log.debug("log4jdbc.properties not found on classpath");
        }

        // look for additional driver specified in properties
        debugStackPrefix = getStringOption(props, "log4jdbc.debug.stack.prefix");
        traceFromApplication = debugStackPrefix != null;

        Long thresh = getLongOption(props, "log4jdbc.sqltiming.warn.threshold");
        sqlTimingWarnThresholdEnabled = (thresh != null);
        if(sqlTimingWarnThresholdEnabled) {
            sqlTimingWarnThresholdNanoSec = thresh.longValue();
        }

        thresh = getLongOption(props, "log4jdbc.sqltiming.error.threshold");
        sqlTimingErrorThresholdEnabled = (thresh != null);
        if(sqlTimingErrorThresholdEnabled) {
            sqlTimingErrorThresholdNanoSec = thresh.longValue();
        }

        shouldUseMarkersForTimingReports = getBooleanOption(props, "log4jdbc.sqltiming.usemarkersfortimingreports", false);

        dumpFullDebugStackTrace = getBooleanOption(props, "log4jdbc.dump.fulldebugstacktrace", false);

        dumpSqlSelect = getBooleanOption(props, "log4jdbc.dump.sql.select", true);
        dumpSqlInsert = getBooleanOption(props, "log4jdbc.dump.sql.insert", true);
        dumpSqlUpdate = getBooleanOption(props, "log4jdbc.dump.sql.update", true);
        dumpSqlDelete = getBooleanOption(props, "log4jdbc.dump.sql.delete", true);
        dumpSqlCreate = getBooleanOption(props, "log4jdbc.dump.sql.create", true);
        reportOriginalSql = getBooleanOption(props, "log4jdbc.dump.sql.reportoriginal", false);

        dumpSqlFilteringOn = !(dumpSqlSelect && dumpSqlInsert && dumpSqlUpdate && dumpSqlDelete && dumpSqlCreate);

        autoLoadPopularDrivers = getBooleanOption(props, "log4jdbc.auto.load.popular.drivers", true);

        trimSql = getBooleanOption(props, "log4jdbc.trim.sql", true);

        String str = getStringOption(props, "log4jdbc.drivers");
        drivers = (str != null) ? List.of(str.split(",")) : List.of();
        
    }

    Map<String,ParameterFormatter> getParameterFormatters() {
        return parameterFormatters;
    }

    ParameterFormatter getParameterFormatter(String driver) {

        if((driver == null) || driver.isEmpty()) {
            return DEFAULT_PARAMETER_FORMATTER;
        }

        return parameterFormatters.getOrDefault(driver, DEFAULT_PARAMETER_FORMATTER);

    }

    public String getDebugStackPrefix() {
        return debugStackPrefix;
    }

    public boolean isTraceFromApplication() {
        return traceFromApplication;
    }

    public boolean isSqlTimingWarnThresholdEnabled() {
        return sqlTimingWarnThresholdEnabled;
    }

    public long getSqlTimingWarnThresholdNanoSec() {
        return sqlTimingWarnThresholdNanoSec;
    }

    public boolean isSqlTimingErrorThresholdEnabled() {
        return sqlTimingErrorThresholdEnabled;
    }

    public long getSqlTimingErrorThresholdNanoSec() {
        return sqlTimingErrorThresholdNanoSec;
    }

    public boolean isDumpSqlSelect() {
        return dumpSqlSelect;
    }

    public boolean isDumpSqlInsert() {
        return dumpSqlInsert;
    }

    public boolean isDumpSqlUpdate() {
        return dumpSqlUpdate;
    }

    public boolean isDumpSqlDelete() {
        return dumpSqlDelete;
    }

    public boolean isDumpSqlCreate() {
        return dumpSqlCreate;
    }

    public boolean isReportOriginalSql() {
        return reportOriginalSql;
    }

    public boolean isShouldUseMarkersForTimingReports() {
        return shouldUseMarkersForTimingReports;
    }

    public boolean isDumpSqlFilteringOn() {
        return dumpSqlFilteringOn;
    }

    public boolean isDumpFullDebugStackTrace() {
        return dumpFullDebugStackTrace;
    }

    public boolean isAutoLoadPopularDrivers() {
        return autoLoadPopularDrivers;
    }

    public boolean isTrimSql() {
        return trimSql;
    }

    public List<String> getDrivers() {
        return drivers;
    }

    /**
     * Get a Long option from a property and log a debug message about this.
     *
     * @param props Properties to get option from.
     * @param propName property key.
     *
     * @return the value of that property key, converted to a Long. Or null if not
     *         defined or is invalid.
     */
    private static Long getLongOption(Properties props, String propName) {

        String propValue = props.getProperty(propName);
        Long longPropValue = null;

        if(propValue != null) {

            try {
                longPropValue = Long.parseLong(propValue);
                log.debug("  " + propName + " = " + longPropValue);
            }
            catch(NumberFormatException ex) {
                log.debug("x " + propName + " \"" + propValue + "\" is not a valid number");
            }

        }
        else {
            log.debug("x " + propName + " is not defined");
        }

        return longPropValue;

    }

    /**
     * Get a Long option from a property and log a debug message about this.
     *
     * @param props Properties to get option from.
     * @param propName property key.
     *
     * @return the value of that property key, converted to a Long. Or null if not
     *         defined or is invalid.
     */
    private static Long getLongOption(Properties props, String propName, long defaultValue) {

        String propValue = props.getProperty(propName);
        Long longPropValue;

        if(propValue == null) {
            log.debug("x " + propName + " is not defined (using default of " + defaultValue + ")");
            return defaultValue;
        }

        try {
            longPropValue = Long.parseLong(propValue);
            log.debug("  " + propName + " = " + longPropValue);
        }
        catch(NumberFormatException n) {
            log.debug("x " + propName + " \"" + propValue + "\" is not a valid number (using default of " + defaultValue + ")");
            longPropValue = defaultValue;
        }

        return longPropValue;

    }

    /**
     * Get a String option from a property and log a debug message about this.
     *
     * @param props Properties to get option from.
     * @param propName property key.
     * @return the value of that property key.
     */
    private static String getStringOption(Properties props, String propName) {

        String propValue = props.getProperty(propName);

        if((propValue == null) || propValue.isEmpty()) {
            log.debug("x " + propName + " is not defined");
            propValue = null; // force to null, even if empty String
        }
        else {
            log.debug("  " + propName + " = " + propValue);
        }

        return propValue;

    }

    /**
     * Get a boolean option from a property and log a debug message about this.
     *
     * @param props Properties to get option from.
     * @param propName property name to get.
     * @param defaultValue default value to use if undefined.
     *
     * @return boolean value found in property, or defaultValue if no property
     *         found.
     */
    private static boolean getBooleanOption(Properties props, String propName, boolean defaultValue) {

        String propValue = props.getProperty(propName);
        boolean val;

        if(propValue == null) {
            log.debug("x " + propName + " is not defined (using default value " + defaultValue + ")");
            return defaultValue;
        }

        propValue = propValue.trim().toLowerCase();

        if(propValue.isEmpty()) {
            val = defaultValue;
        }
        else {
            val = "true".equals(propValue) || "yes".equals(propValue) || "on".equals(propValue);
        }

        log.debug("  " + propName + " = " + val);

        return val;

    }

}
