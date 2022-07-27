package org.digitalforge.log4jdbc;

import org.digitalforge.log4jdbc.JdbcSpy;

/**
 * Delegates JdbcSpy events to a logger.
 * This interface is used for all logging activity used by Log4JDBC and hides the specific implementation
 * of any given logging system from log4jdbc.
 *
 */
public interface SpyLogDelegator {

    /**
     * Determine if any of the jdbc or sql loggers are turned on.
     *
     * @return true if any of the jdbc or sql loggers are enabled at error level or higher.
     */
    boolean isJdbcLoggingEnabled();

    /**
     * Called when a spied upon method throws an Exception.
     *
     * @param spy        the JdbcSpy wrapping the class that threw an Exception.
     * @param methodCall a description of the name and call parameters of the method generated the Exception.
     * @param e          the Exception that was thrown.
     * @param sql        optional sql that occured just before the exception occured.
     * @param execTimeNanoSec   optional amount of time that passed before an exception was thrown when sql was being executed.
     *                   caller should pass -1 if not used
     */
    void exceptionOccured(JdbcSpy spy, String methodCall, Exception e, String sql, long execTimeNanoSec);

    /**
     * Called when spied upon method call returns.
     *
     * @param spy        the JdbcSpy wrapping the class that called the method that returned.
     * @param methodCall a description of the name and call parameters of the method that returned.
     * @param returnMsg  return value converted to a String for integral types, or String representation for Object
     *                   return types this will be null for void return types.
     */
    void methodReturned(JdbcSpy spy, String methodCall, String returnMsg);

    /**
     * Called when a spied upon object is constructed.
     *
     * @param spy              the JdbcSpy wrapping the class that called the method that returned.
     * @param constructionInfo information about the object construction
     */
    void constructorReturned(JdbcSpy spy, String constructionInfo);

    /**
     * Special call that is called only for JDBC method calls that contain SQL.
     *
     * @param spy        the JdbcSpy wrapping the class where the SQL occured.
     * @param methodCall a description of the name and call parameters of the method that generated the SQL.
     * @param sql        sql that occured.
     */
    void sqlOccured(JdbcSpy spy, String methodCall, String sql);

    /**
     * Similar to sqlOccured, but reported after SQL executes and used to report timing stats on the SQL
     *
     * @param spy the    JdbcSpy wrapping the class where the SQL occured.
     * @param execTimeNanoSec   how long it took the sql to run, in nanoseconds.
     * @param methodCall a description of the name and call parameters of the method that generated the SQL.
     * @param sql        sql that occured.
     */
    void sqlTimingOccured(JdbcSpy spy, long execTimeNanoSec, String methodCall, String sql);

    /**
     * Called whenever a new connection spy is created.
     *
     * @param spy ConnectionSpy that was created.
     */
    void connectionOpened(JdbcSpy spy);

    /**
     * Called whenever a connection spy is closed.
     *
     * @param spy ConnectionSpy that was closed.
     */
    void connectionClosed(JdbcSpy spy);

    /**
     * Log a Setup and/or administrative log message for log4jdbc.
     *
     * @param msg message to log.
     */
    void debug(String msg);

}