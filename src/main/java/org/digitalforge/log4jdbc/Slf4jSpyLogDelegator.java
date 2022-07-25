package org.digitalforge.log4jdbc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

/**
 * Delegates JDBC spy logging events to the Simple Logging Facade for Java
 * (slf4j).
 */
public class Slf4jSpyLogDelegator implements SpyLogDelegator {

    public static ExecutionTimeMarkerFactory markerFactory = new ExecutionTimeMarkerFactory() {
            @Override
            public Marker create(String sql, long executionTimeNanoSec) {
                return MarkerFactory.getMarker("{\"sql\"=\"" + sql + "\",\"executedInNanoSec\"=" + executionTimeNanoSec + "}");
            }
        };

    /**
     * Create a SpyLogDelegator specific to the Simple Logging Facade for Java
     * (slf4j).
     */
    public Slf4jSpyLogDelegator() {

    }

    // logs for sql and jdbc

    /**
     * Logger that shows all JDBC calls on INFO level (exception ResultSet calls)
     */
    private final Logger jdbcLogger = LoggerFactory.getLogger("jdbc.audit");

    /**
     * Logger that shows JDBC calls for ResultSet operations
     */
    private final Logger resultSetLogger = LoggerFactory.getLogger("jdbc.resultset");

    /**
     * Logger that shows only the SQL that is occuring
     */
    private final Logger sqlOnlyLogger = LoggerFactory.getLogger("jdbc.sqlonly");

    /**
     * Logger that shows the SQL timing, post execution
     */
    private final Logger sqlTimingLogger = LoggerFactory.getLogger("jdbc.sqltiming");

    /**
     * Logger that shows connection open and close events as well as current
     * number of open connections.
     */
    private final Logger connectionLogger = LoggerFactory.getLogger("jdbc.connection");

    // admin/setup logging for log4jdbc.

    /**
     * Logger just for debugging things within log4jdbc itself (admin, setup,
     * etc.)
     */
    private final Logger debugLogger = LoggerFactory.getLogger("log4jdbc.debug");

    /**
     * Determine if any of the 5 log4jdbc spy loggers are turned on (jdbc.audit |
     * jdbc.resultset | jdbc.sqlonly | jdbc.sqltiming | jdbc.connection)
     *
     * @return true if any of the 5 spy jdbc/sql loggers are enabled at debug info
     * or error level.
     */
    public boolean isJdbcLoggingEnabled() {
        return jdbcLogger.isErrorEnabled()
            || resultSetLogger.isErrorEnabled()
            || sqlOnlyLogger.isErrorEnabled()
            || sqlTimingLogger.isErrorEnabled()
            || connectionLogger.isErrorEnabled();
    }

    /**
     * Called when a jdbc method throws an Exception.
     *
     * @param spy             the JdbcSpy wrapping the class that threw an Exception.
     * @param methodCall      a description of the name and call parameters of the
     *                        method generated the Exception.
     * @param ex               the Exception that was thrown.
     * @param sql             optional sql that occurred just before the exception occured.
     * @param execTimeNanoSec optional amount of time that passed before an exception was
     *                        thrown when sql was being executed. caller should pass -1 if not
     *                        used
     */
    public void exceptionOccured(JdbcSpy spy, String methodCall, Exception ex, String sql, long execTimeNanoSec) {

        if(true) {
            return;
        }

        sql = LoggingDriver.sqlPrettifier.prettifySql(sql);

        String classType = spy.getClassType();
        Integer spyNo = spy.getConnectionNumber();
        String header = spyNo + ". " + classType + "." + methodCall;
        if(sql == null) {
            jdbcLogger.error(header, ex);
            sqlOnlyLogger.error(header, ex);
            sqlTimingLogger.error(header, ex);
        }
        else {
            sql = processSql(sql);
            jdbcLogger.error(header + " " + sql, ex);

            // if at debug level, display debug info to error log
            if(sqlOnlyLogger.isDebugEnabled()) {
                sqlOnlyLogger.error(getDebugInfo() + NEWLINE + spyNo + ". " + sql, ex);
            }
            else {
                sqlOnlyLogger.error(header + " " + sql, ex);
            }

            // if at debug level, display debug info to error log
            if(sqlTimingLogger.isDebugEnabled()) {
                sqlTimingLogger.error(getDebugInfo() + NEWLINE + spyNo + ". " + sql + " {FAILED after " + execTimeNanoSec + " nanoSec}", ex);
            }
            else {
                sqlTimingLogger.error(header + " FAILED! " + sql + " {FAILED after " + execTimeNanoSec + " nanoSec}", ex);
            }
        }
    }

    /**
     * Called when a JDBC method from a Connection, Statement, PreparedStatement,
     * CallableStatement or ResultSet returns.
     *
     * @param spy        the JdbcSpy wrapping the class that called the method that returned.
     * @param methodCall a description of the name and call parameters of the
     *                   method that returned.
     * @param returnMsg  return value converted to a String for integral types, or
     *                   String representation for Object. Return types this will be null for
     *                   void return types.
     */
    public void methodReturned(JdbcSpy spy, String methodCall, String returnMsg) {
        String classType = spy.getClassType();
        Logger logger = "ResultSet".equals(classType) ? resultSetLogger : jdbcLogger;
        if(logger.isInfoEnabled()) {
            String header = spy.getConnectionNumber() + ". " + classType + "." + methodCall + " returned " + returnMsg;
            if(logger.isDebugEnabled()) {
                logger.debug(header + " " + getDebugInfo());
            }
            else {
                logger.info(header);
            }
        }
    }

    /**
     * Called when a spied upon object is constructed.
     *
     * @param spy              the JdbcSpy wrapping the class that called the method that returned.
     * @param constructionInfo information about the object construction
     */
    public void constructorReturned(JdbcSpy spy, String constructionInfo) {
        // not used in this implementation -- yet
    }

    private static final String NEWLINE = System.getProperty("line.separator");

    /**
     * Determine if the given sql should be logged or not based on the various
     * DumpSqlXXXXXX flags.
     *
     * @param sql SQL to test.
     *
     * @return true if the SQL should be logged, false if not.
     */
    private boolean shouldSqlBeLogged(String sql) {
        if(sql == null) {
            return false;
        }
        sql = sql.trim();

        if(sql.length() < 6) {
            return false;
        }
        sql = sql.substring(0, 6).toLowerCase();
        return (LoggingDriver.config.isDumpSqlSelect() && "select".equals(sql)) || (LoggingDriver.config.isDumpSqlInsert() && "insert".equals(sql)) || (LoggingDriver.config.isDumpSqlUpdate() && "update"
            .equals(sql)) || (LoggingDriver.config.isDumpSqlDelete() && "delete".equals(sql)) || (LoggingDriver.config.isDumpSqlCreate() && "create".equals(sql));
    }

    public boolean shouldUseMarkersForTimingReports() {
        return LoggingDriver.config.isShouldUseMarkersForTimingReports();
    }

    /**
     * Special call that is called only for JDBC method calls that contain SQL.
     *
     * @param spy        the JdbcSpy wrapping the class where the SQL occured.
     * @param methodCall a description of the name and call parameters of the
     *                   method that generated the SQL.
     * @param sql        sql that occured.
     */
    public void sqlOccured(JdbcSpy spy, String methodCall, String sql) {
        sql = LoggingDriver.sqlPrettifier.prettifySql(sql);

        if(!LoggingDriver.config.isDumpSqlFilteringOn() || shouldSqlBeLogged(sql)) {
            if(sqlOnlyLogger.isDebugEnabled()) {
                sqlOnlyLogger.debug(getDebugInfo() + NEWLINE + spy.getConnectionNumber() + ". " + processSql(sql));
            }
            else if(sqlOnlyLogger.isInfoEnabled()) {
                sqlOnlyLogger.info(processSql(sql));
            }
        }
    }

    /**
     * Break an SQL statement up into multiple lines in an attempt to make it more
     * readable
     *
     * @param sql SQL to break up.
     *
     * @return SQL broken up into multiple lines
     */
    private String processSql(String sql) {
        if(sql == null) {
            return null;
        }

        if(LoggingDriver.config.isTrimSql()) {
            sql = sql.trim();
        }

        return sql;

    }

    /**
     * Special call that is called only for JDBC method calls that contain SQL.
     *
     * @param spy             the JdbcSpy wrapping the class where the SQL occurred.
     * @param execTimeNanoSec how long it took the SQL to run, in nanoseconds.
     * @param methodCall      a description of the name and call parameters of the
     *                        method that generated the SQL.
     * @param sql             SQL that occurred.
     */
    public void sqlTimingOccured(JdbcSpy spy, long execTimeNanoSec, String methodCall, String sql) {
        sql = LoggingDriver.sqlPrettifier.prettifySql(sql);

        if(!shouldReportTimingOccured(sql)) {
            return;
        }

        String message = buildSqlTimingDump(spy, execTimeNanoSec, methodCall, sql, sqlTimingLogger.isDebugEnabled());
        Marker marker = Slf4jSpyLogDelegator.markerFactory.create(sql, execTimeNanoSec);

        if(LoggingDriver.config.isSqlTimingErrorThresholdEnabled() && execTimeNanoSec >= LoggingDriver.config.getSqlTimingErrorThresholdNanoSec()) {
            if(shouldUseMarkersForTimingReports()) {
                sqlTimingLogger.error(marker, message);
            }
            else {
                sqlTimingLogger.error(message);
            }

            return;
        }

        if(!sqlTimingLogger.isWarnEnabled()) {
            return;
        }

        if(LoggingDriver.config.isSqlTimingWarnThresholdEnabled() && execTimeNanoSec >= LoggingDriver.config.getSqlTimingWarnThresholdNanoSec()) {
            if(shouldUseMarkersForTimingReports()) {
                sqlTimingLogger.warn(marker, message);
            }
            else {
                sqlTimingLogger.warn(message);
            }
        }
        else if(sqlTimingLogger.isDebugEnabled()) {
            if(shouldUseMarkersForTimingReports()) {
                sqlTimingLogger.debug(marker, message);
            }
            else {
                sqlTimingLogger.debug(message);
            }
        }
        else if(sqlTimingLogger.isInfoEnabled()) {
            if(shouldUseMarkersForTimingReports()) {
                sqlTimingLogger.info(marker, message);
            }
            else {
                sqlTimingLogger.info(message);
            }
        }
    }

    private boolean shouldReportTimingOccured(String sql) {
        return sqlTimingLogger.isErrorEnabled() && (!LoggingDriver.config.isDumpSqlFilteringOn() || shouldSqlBeLogged(sql));
    }

    /**
     * Helper method to quickly build a SQL timing dump output String for logging.
     *
     * @param spy             the JdbcSpy wrapping the class where the SQL occurred.
     * @param execTimeNanoSec how long it took the SQL to run, in nanoseconds.
     * @param methodCall      a description of the name and call parameters of the
     *                        method that generated the SQL.
     * @param sql             SQL that occurred.
     * @param debugInfo       if true, include debug info at the front of the output.
     *
     * @return a SQL timing dump String for logging.
     */
    private String buildSqlTimingDump(JdbcSpy spy, long execTimeNanoSec, String methodCall, String sql, boolean debugInfo) {
        StringBuffer out = new StringBuffer();

        if(debugInfo) {
            out.append(getDebugInfo());
            out.append(NEWLINE);
            out.append(spy.getConnectionNumber());
            out.append(". ");
        }

        // NOTE: if both sql dump and sql timing dump are on, the processSql
        // algorithm will run TWICE once at the beginning and once at the end
        // this is not very efficient but usually
        // only one or the other dump should be on and not both.

        sql = processSql(sql);

        out.append(sql);

        if(!shouldUseMarkersForTimingReports()) {
            out.append(" {executed in ");
            out.append(execTimeNanoSec);
            out.append(" nanoSec}");
        }

        return out.toString();
    }

    /**
     * Get debugging info - the module and line number that called the logger
     * version that prints the stack trace information from the point just before
     * we got it (net.sf.log4jdbc)
     * <p>
     * if the optional log4jdbc.debug.stack.prefix system property is defined then
     * the last call point from an application is shown in the debug trace output,
     * instead of the last direct caller into log4jdbc
     *
     * @return debugging info for whoever called into JDBC from within the
     * application.
     */
    private static String getDebugInfo() {
        Throwable t = new Throwable();
        t.fillInStackTrace();

        StackTraceElement[] stackTrace = t.getStackTrace();

        if(stackTrace != null) {
            String className;

            StringBuffer dump = new StringBuffer();

            /**
             * The DumpFullDebugStackTrace option is useful in some situations when we
             * want to see the full stack trace in the debug info- watch out though as
             * this will make the logs HUGE!
             */
            if(LoggingDriver.config.isDumpFullDebugStackTrace()) {
                boolean first = true;
                for(int i = 0; i < stackTrace.length; i++) {
                    className = stackTrace[i].getClassName();
                    if(className.startsWith("net.sf.log4jdbc") || className.startsWith("com.orderlyhealth.log4jdbc")) {
                        continue;
                    }

                    if(first) {
                        first = false;
                    }
                    else {
                        dump.append("  ");
                    }

                    dump.append("at ");
                    dump.append(stackTrace[i]);
                    dump.append(NEWLINE);

                }
            }
            else {
                dump.append(" ");
                int firstLog4jdbcCall = 0;
                int lastApplicationCall = 0;

                for(int i = 0; i < stackTrace.length; i++) {
                    className = stackTrace[i].getClassName();
                    if(className.startsWith("net.sf.log4jdbc") || className.startsWith("com.orderlyhealth.log4jdbc")) {
                        firstLog4jdbcCall = i;
                    }
                    else if(LoggingDriver.config.isTraceFromApplication() && className.startsWith(LoggingDriver.config.getDebugStackPrefix())) {
                        lastApplicationCall = i;
                        break;
                    }
                }
                int j = lastApplicationCall;

                // if app not found, then use whoever was the last guy that called a log4jdbc class
                if(j == 0) {
                    j = 1 + firstLog4jdbcCall;
                }

                dump.append(stackTrace[j].getClassName()).append(".")
                    .append(stackTrace[j].getMethodName())
                    .append("(")
                    .append(stackTrace[j].getFileName())
                    .append(":")
                    .append(stackTrace[j].getLineNumber())
                    .append(")");
            }

            return dump.toString();
        }
        else {
            return null;
        }
    }

    /**
     * Log a Setup and/or administrative log message for log4jdbc.
     *
     * @param msg message to log.
     */
    public void debug(String msg) {
        debugLogger.debug(msg);
    }

    /**
     * Called whenever a new connection spy is created.
     *
     * @param spy ConnectionSpy that was created.
     */
    public void connectionOpened(JdbcSpy spy) {
        if(connectionLogger.isDebugEnabled()) {
            connectionLogger.info(spy.getConnectionNumber() + ". Connection opened " + getDebugInfo());
            connectionLogger.debug(LoggingConnection.getOpenConnectionsDump());
        }
        else {
            connectionLogger.info(spy.getConnectionNumber() + ". Connection opened");
        }
    }

    /**
     * Called whenever a connection spy is closed.
     *
     * @param spy ConnectionSpy that was closed.
     */
    public void connectionClosed(JdbcSpy spy) {
        if(connectionLogger.isDebugEnabled()) {
            connectionLogger.info(spy.getConnectionNumber() + ". Connection closed " + getDebugInfo());
            connectionLogger.debug(LoggingConnection.getOpenConnectionsDump());
        }
        else {
            connectionLogger.info(spy.getConnectionNumber() + ". Connection closed");
        }
    }

    public interface ExecutionTimeMarkerFactory {

        Marker create(String sql, long executionTimeNanoSec);

    }

}
