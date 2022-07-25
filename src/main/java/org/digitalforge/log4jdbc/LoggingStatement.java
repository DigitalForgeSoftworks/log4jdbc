package org.digitalforge.log4jdbc;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import org.digitalforge.log4jdbc.formatter.ParameterFormatter;
import org.digitalforge.log4jdbc.util.Utilities;

/**
 * Wraps a Statement and reports method calls, returns and exceptions.
 *
 * JDBC 4.2
 */
public class LoggingStatement<S extends Statement> implements Statement, JdbcSpy {

    private static final SpyLogDelegator log = SpyLogFactory.getSpyLogDelegator();

    /**
     * The Connection that created this Statement.
     */
    protected LoggingConnection connection;

    /**
     * ParameterFormatter for formatting SQL for the given RDBMS.
     */
    protected ParameterFormatter parameterFormatter;

    /**
     * The real statement that this LoggingStatement wraps.
     */
    protected S delegate;

    /**
     * Get the delegate Statement that this LoggingStatement wraps.
     *
     * @return the real Statement that this LoggingStatement wraps.
     */
    public S getDelegate() {
        return delegate;
    }

    /**
     * Create a LoggingStatement that wraps another Statement
     * for the purpose of logging all method calls, sql, exceptions and return values.
     *
     * @param connection Connection that created this Statement.
     * @param delegate real underlying Statement that this LoggingStatement wraps.
     */
    public LoggingStatement(LoggingConnection connection, S delegate) {
        if(delegate == null) {
            throw new IllegalArgumentException("Must pass in a non null real Statement");
        }
        if(connection == null) {
            throw new IllegalArgumentException("Must pass in a non null ConnectionSpy");
        }
        this.delegate = delegate;
        this.connection = connection;
        this.parameterFormatter = connection.getParameterFormatter();

        if(delegate instanceof CallableStatement) {
            reportReturn("new CallableStatement");
        }
        else if(delegate instanceof PreparedStatement) {
            reportReturn("new PreparedStatement");
        }
        else {
            reportReturn("new Statement");
        }
    }

    @Override
    public String getClassType() {
        return "Statement";
    }

    @Override
    public Integer getConnectionNumber() {
        return connection.getConnectionNumber();
    }

    /**
     * Report an exception to be logged which includes timing data on a sql failure.
     * @param methodCall description of method call and arguments passed to it that generated the exception.
     * @param exception exception that was generated
     * @param sql SQL associated with the call.
     * @param execTimeNanoSec amount of time that the jdbc driver was chugging on the SQL before it threw an exception.
     */
    protected void reportException(String methodCall, SQLException exception, String sql, long execTimeNanoSec) {
        log.exceptionOccured(this, methodCall, exception, sql, execTimeNanoSec);
    }

    /**
     * Report an exception to be logged.
     * @param methodCall description of method call and arguments passed to it that generated the exception.
     * @param exception exception that was generated
     * @param sql SQL associated with the call.
     */
    protected void reportException(String methodCall, SQLException exception, String sql) {
        log.exceptionOccured(this, methodCall, exception, sql, -1L);
    }

    /**
     * Report an exception to be logged.
     *
     * @param methodCall description of method call and arguments passed to it that generated the exception.
     * @param exception exception that was generated
     */
    protected void reportException(String methodCall, SQLException exception) {
        log.exceptionOccured(this, methodCall, exception, null, -1L);
    }

    /**
     * Report (for logging) that a method returned.  All the other reportReturn methods are conveniance methods that call this method.
     *
     * @param methodCall description of method call and arguments passed to it that returned.
     * @param msg description of what the return value that was returned.  may be an empty String for void return types.
     */
    protected void reportAllReturns(String methodCall, String msg) {
        log.methodReturned(this, methodCall, msg);
    }

    /**
     * Conveniance method to report (for logging) that a method returned a boolean value.
     *
     * @param methodCall description of method call and arguments passed to it that returned.
     * @param value boolean return value.
     * @return the boolean return value as passed in.
     */
    protected boolean reportReturn(String methodCall, boolean value) {
        reportAllReturns(methodCall, "" + value);
        return value;
    }

    /**
     * Conveniance method to report (for logging) that a method returned a byte value.
     *
     * @param methodCall description of method call and arguments passed to it that returned.
     * @param value byte return value.
     * @return the byte return value as passed in.
     */
    protected byte reportReturn(String methodCall, byte value) {
        reportAllReturns(methodCall, "" + value);
        return value;
    }

    /**
     * Conveniance method to report (for logging) that a method returned a int value.
     *
     * @param methodCall description of method call and arguments passed to it that returned.
     * @param value int return value.
     * @return the int return value as passed in.
     */
    protected int reportReturn(String methodCall, int value) {
        reportAllReturns(methodCall, "" + value);
        return value;
    }

    /**
     * Conveniance method to report (for logging) that a method returned a double value.
     *
     * @param methodCall description of method call and arguments passed to it that returned.
     * @param value double return value.
     * @return the double return value as passed in.
     */
    protected double reportReturn(String methodCall, double value) {
        reportAllReturns(methodCall, "" + value);
        return value;
    }

    /**
     * Conveniance method to report (for logging) that a method returned a short value.
     *
     * @param methodCall description of method call and arguments passed to it that returned.
     * @param value short return value.
     * @return the short return value as passed in.
     */
    protected short reportReturn(String methodCall, short value) {
        reportAllReturns(methodCall, "" + value);
        return value;
    }

    /**
     * Conveniance method to report (for logging) that a method returned a long value.
     *
     * @param methodCall description of method call and arguments passed to it that returned.
     * @param value long return value.
     * @return the long return value as passed in.
     */
    protected long reportReturn(String methodCall, long value) {
        reportAllReturns(methodCall, "" + value);
        return value;
    }

    /**
     * Conveniance method to report (for logging) that a method returned a float value.
     *
     * @param methodCall description of method call and arguments passed to it that returned.
     * @param value float return value.
     * @return the float return value as passed in.
     */
    protected float reportReturn(String methodCall, float value) {
        reportAllReturns(methodCall, "" + value);
        return value;
    }

    /**
     * Conveniance method to report (for logging) that a method returned an Object.
     *
     * @param methodCall description of method call and arguments passed to it that returned.
     * @param value return Object.
     * @return the return Object as passed in.
     */
    protected Object reportReturn(String methodCall, Object value) {
        reportAllReturns(methodCall, "" + value);
        return value;
    }

    /**
     * Conveniance method to report (for logging) that a method returned (void return type).
     *
     * @param methodCall description of method call and arguments passed to it that returned.
     */
    protected void reportReturn(String methodCall) {
        reportAllReturns(methodCall, "");
    }

    /**
     * Running one-off statement sql is generally inefficient and a bad idea for various reasons,
     * so give a warning when this is done.
     */
    private static final String StatementSqlWarning = "{WARNING: Statement used to run SQL} ";

    /**
     * Report SQL for logging with a warning that it was generated from a statement.
     *
     * @param sql        the SQL being run
     * @param methodCall the name of the method that was running the SQL
     */
    protected void reportStatementSql(String sql, String methodCall) {
        // redirect to one more method call ONLY so that stack trace search is consistent
        // with the reportReturn calls
        reportSql2(sql, methodCall);
    }

    /**
     * Report SQL for logging with a warning that it was generated from a statement.
     *
     * @param execTimeNanoSec   execution time in nanoseconds.
     * @param sql        the SQL being run
     * @param methodCall the name of the method that was running the SQL
     */
    protected void reportStatementSqlTiming(long execTimeNanoSec, String sql, String methodCall) {
        // redirect to one more method call ONLY so that stack trace search is consistent
        // with the reportReturn calls
        reportSqlTiming2(execTimeNanoSec, sql, methodCall);
    }

    /**
     * Report SQL for logging.
     *
     * @param execTimeNanoSec   execution time in nanoseconds.
     * @param sql        the SQL being run
     * @param methodCall the name of the method that was running the SQL
     */
    protected void reportSqlTiming(long execTimeNanoSec, String sql, String methodCall) {
        // redirect to one more method call ONLY so that stack trace search is consistent
        // with the reportReturn calls
        reportSqlTiming2(execTimeNanoSec, sql, methodCall);
    }

    /**
     * Report SQL for logging.
     *
     * @param sql        the SQL being run
     * @param methodCall the name of the method that was running the SQL
     */
    protected void reportSql(String sql, String methodCall) {
        // redirect to one more method call ONLY so that stack trace search is consistent
        // with the reportReturn calls
        reportSql2(sql, methodCall);
    }

    private void reportSql2(String sql, String methodCall) {
        log.sqlOccured(this, methodCall, sql);
    }

    private void reportSqlTiming2(long execTimeNanoSec, String sql, String methodCall) {
        log.sqlTimingOccured(this, execTimeNanoSec, methodCall, sql);
    }

    // implementation of interface methods
    public SQLWarning getWarnings() throws SQLException {
        String methodCall = "getWarnings()";
        try {
            return (SQLWarning)reportReturn(methodCall, delegate.getWarnings());
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        String methodCall = "executeUpdate(" + sql + ", " + columnNames + ")";
        reportStatementSql(sql, methodCall);
        long tstartNano = System.nanoTime();
        try {
            int result = delegate.executeUpdate(sql, columnNames);
            reportStatementSqlTiming(System.nanoTime() - tstartNano, sql, methodCall);
            return reportReturn(methodCall, result);
        }
        catch(SQLException s) {
            reportException(methodCall, s, sql, System.nanoTime() - tstartNano);
            throw s;
        }
    }

    public boolean execute(String sql, String[] columnNames) throws SQLException {
        String methodCall = "execute(" + sql + ", " + columnNames + ")";
        reportStatementSql(sql, methodCall);
        long tstartNano = System.nanoTime();
        try {
            boolean result = delegate.execute(sql, columnNames);
            reportStatementSqlTiming(System.nanoTime() - tstartNano, sql, methodCall);
            return reportReturn(methodCall, result);
        }
        catch(SQLException s) {
            reportException(methodCall, s, sql, System.nanoTime() - tstartNano);
            throw s;
        }
    }

    public void setMaxRows(int max) throws SQLException {
        String methodCall = "setMaxRows(" + max + ")";
        try {
            delegate.setMaxRows(max);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    public boolean getMoreResults() throws SQLException {
        String methodCall = "getMoreResults()";

        try {
            return reportReturn(methodCall, delegate.getMoreResults());
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    public void clearWarnings() throws SQLException {
        String methodCall = "clearWarnings()";
        try {
            delegate.clearWarnings();
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    /**
     * Tracking of current batch (see addBatch, clearBatch and executeBatch)
     * //todo: should access to this List be synchronized?
     */
    protected List<String> currentBatch = new ArrayList<>();

    public void addBatch(String sql) throws SQLException {
        String methodCall = "addBatch(" + sql + ")";

        currentBatch.add(StatementSqlWarning + sql);
        try {
            delegate.addBatch(sql);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    public int getResultSetType() throws SQLException {
        String methodCall = "getResultSetType()";
        try {
            return reportReturn(methodCall, delegate.getResultSetType());
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    public void clearBatch() throws SQLException {
        String methodCall = "clearBatch()";
        try {
            delegate.clearBatch();
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        currentBatch.clear();
        reportReturn(methodCall);
    }

    public void setFetchDirection(int direction) throws SQLException {
        String methodCall = "setFetchDirection(" + direction + ")";
        try {
            delegate.setFetchDirection(direction);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    public int[] executeBatch() throws SQLException {
        String methodCall = "executeBatch()";

        int j = currentBatch.size();
        String sql;

        if(j > 1) {
            StringBuffer batchReport = new StringBuffer("batching " + j + " statements:");

            int fieldSize = ("" + j).length();

            for(int i = 0; i < j; ) {
                sql = (String)currentBatch.get(i);
                batchReport.append("\n");
                batchReport.append(Utilities.rightJustify(fieldSize, "" + (++i)));
                batchReport.append(":  ");
                batchReport.append(sql);
            }

            sql = batchReport.toString();

        } else {
            sql = currentBatch.get(0);
        }

        reportSql(sql, methodCall);
        long tstartNano = System.nanoTime();

        int[] updateResults;
        try {
            updateResults = delegate.executeBatch();
            reportSqlTiming(System.nanoTime() - tstartNano, sql, methodCall);
        }
        catch(SQLException s) {
            reportException(methodCall, s, sql, System.nanoTime() - tstartNano);
            throw s;
        }
        currentBatch.clear();
        return (int[])reportReturn(methodCall, updateResults);
    }

    public void setFetchSize(int rows) throws SQLException {
        String methodCall = "setFetchSize(" + rows + ")";
        try {
            delegate.setFetchSize(rows);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    public int getQueryTimeout() throws SQLException {
        String methodCall = "getQueryTimeout()";
        try {
            return reportReturn(methodCall, delegate.getQueryTimeout());
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    public Connection getConnection() throws SQLException {
        String methodCall = "getConnection()";
        return (Connection)reportReturn(methodCall, connection);
    }

    public ResultSet getGeneratedKeys() throws SQLException {
        String methodCall = "getGeneratedKeys()";
        try {
            ResultSet r = delegate.getGeneratedKeys();
            if(r == null) {
                return (ResultSet)reportReturn(methodCall, r);
            }
            else {
                return (ResultSet)reportReturn(methodCall, new LoggingResultSet(this, r));
            }
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    public void setEscapeProcessing(boolean enable) throws SQLException {
        String methodCall = "setEscapeProcessing(" + enable + ")";
        try {
            delegate.setEscapeProcessing(enable);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    public int getFetchDirection() throws SQLException {
        String methodCall = "getFetchDirection()";
        try {
            return reportReturn(methodCall, delegate.getFetchDirection());
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    public void setQueryTimeout(int seconds) throws SQLException {
        String methodCall = "setQueryTimeout(" + seconds + ")";
        try {
            delegate.setQueryTimeout(seconds);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    public boolean getMoreResults(int current) throws SQLException {
        String methodCall = "getMoreResults(" + current + ")";

        try {
            return reportReturn(methodCall, delegate.getMoreResults(current));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    public ResultSet executeQuery(String sql) throws SQLException {
        String methodCall = "executeQuery(" + sql + ")";
        reportStatementSql(sql, methodCall);
        long tstartNano = System.nanoTime();
        try {
            ResultSet result = delegate.executeQuery(sql);
            reportStatementSqlTiming(System.nanoTime() - tstartNano, sql, methodCall);
            LoggingResultSet r = new LoggingResultSet(this, result);
            return (ResultSet)reportReturn(methodCall, r);
        }
        catch(SQLException s) {
            reportException(methodCall, s, sql, System.nanoTime() - tstartNano);
            throw s;
        }
    }

    public int getMaxFieldSize() throws SQLException {
        String methodCall = "getMaxFieldSize()";
        try {
            return reportReturn(methodCall, delegate.getMaxFieldSize());
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    public int executeUpdate(String sql) throws SQLException {
        String methodCall = "executeUpdate(" + sql + ")";
        reportStatementSql(sql, methodCall);
        long tstartNano = System.nanoTime();
        try {
            int result = delegate.executeUpdate(sql);
            reportStatementSqlTiming(System.nanoTime() - tstartNano, sql, methodCall);
            return reportReturn(methodCall, result);
        }
        catch(SQLException s) {
            reportException(methodCall, s, sql, System.nanoTime() - tstartNano);
            throw s;
        }
    }

    public void cancel() throws SQLException {
        String methodCall = "cancel()";
        try {
            delegate.cancel();
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    public void setCursorName(String name) throws SQLException {
        String methodCall = "setCursorName(" + name + ")";
        try {
            delegate.setCursorName(name);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    public int getFetchSize() throws SQLException {
        String methodCall = "getFetchSize()";
        try {
            return reportReturn(methodCall, delegate.getFetchSize());
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    public int getResultSetConcurrency() throws SQLException {
        String methodCall = "getResultSetConcurrency()";
        try {
            return reportReturn(methodCall, delegate.getResultSetConcurrency());
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    public int getResultSetHoldability() throws SQLException {
        String methodCall = "getResultSetHoldability()";
        try {
            return reportReturn(methodCall, delegate.getResultSetHoldability());
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    public boolean isClosed() throws SQLException {
        String methodCall = "isClosed()";
        try {
            return reportReturn(methodCall, delegate.isClosed());
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    public void setPoolable(boolean poolable) throws SQLException {
        String methodCall = "setPoolable(" + poolable + ")";
        try {
            delegate.setPoolable(poolable);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    public boolean isPoolable() throws SQLException {
        String methodCall = "isPoolable()";
        try {
            return reportReturn(methodCall, delegate.isPoolable());
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        String methodCall = "closeOnCompletion()";
        try {
            delegate.closeOnCompletion();
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        String methodCall = "isCloseOnCompletion()";
        try {
            return reportReturn(methodCall, delegate.isCloseOnCompletion());
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    @Override
    public long getLargeUpdateCount() throws SQLException {
        String methodCall = "getLargeUpdateCount()";
        try {
            return reportReturn(methodCall, delegate.getLargeUpdateCount());
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    @Override
    public void setLargeMaxRows(long max) throws SQLException {
        String methodCall = "setLargeMaxRows(" + max + ")";
        try {
            delegate.setLargeMaxRows(max);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public long getLargeMaxRows() throws SQLException {
        String methodCall = "getLargeMaxRows()";
        try {
            return reportReturn(methodCall, delegate.getLargeMaxRows());
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    @Override
    public long[] executeLargeBatch() throws SQLException {
        String methodCall = "executeLargeBatch()";

        int j = currentBatch.size();
        StringBuffer batchReport = new StringBuffer("batching " + j + " statements:");

        int fieldSize = ("" + j).length();

        String sql;
        for(int i = 0; i < j; ) {
            sql = (String)currentBatch.get(i);
            batchReport.append("\n");
            batchReport.append(Utilities.rightJustify(fieldSize, "" + (++i)));
            batchReport.append(":  ");
            batchReport.append(sql);
        }

        sql = batchReport.toString();
        reportSql(sql, methodCall);
        long tstartNano = System.nanoTime();

        int[] updateResults;
        try {
            updateResults = delegate.executeBatch();
            reportSqlTiming(System.nanoTime() - tstartNano, sql, methodCall);
        }
        catch(SQLException s) {
            reportException(methodCall, s, sql, System.nanoTime() - tstartNano);
            throw s;
        }
        currentBatch.clear();
        return (long[])reportReturn(methodCall, updateResults);
    }

    @Override
    public long executeLargeUpdate(String sql) throws SQLException {
        String methodCall = "executeLargeUpdate(" + sql + ")";
        reportStatementSql(sql, methodCall);
        long tstartNano = System.nanoTime();
        try {
            long result = delegate.executeLargeUpdate(sql);
            reportStatementSqlTiming(System.nanoTime() - tstartNano, sql, methodCall);
            return reportReturn(methodCall, result);
        }
        catch(SQLException s) {
            reportException(methodCall, s, sql, System.nanoTime() - tstartNano);
            throw s;
        }
    }

    @Override
    public long executeLargeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        String methodCall = "executeLargeUpdate(" + sql + ", " + autoGeneratedKeys + ")";
        reportStatementSql(sql, methodCall);
        long tstartNano = System.nanoTime();

        try {
            long result = delegate.executeLargeUpdate(sql, autoGeneratedKeys);
            reportStatementSqlTiming(System.nanoTime() - tstartNano, sql, methodCall);
            return reportReturn(methodCall, result);
        }
        catch(SQLException s) {
            reportException(methodCall, s, sql, System.nanoTime() - tstartNano);
            throw s;
        }
    }

    @Override
    public long executeLargeUpdate(String sql, int[] columnIndexes) throws SQLException {
        String methodCall = "executeLargeUpdate(" + sql + ", " + columnIndexes + ")";
        reportStatementSql(sql, methodCall);
        long tstartNano = System.nanoTime();

        try {
            long result = delegate.executeLargeUpdate(sql, columnIndexes);
            reportStatementSqlTiming(System.nanoTime() - tstartNano, sql, methodCall);
            return reportReturn(methodCall, result);
        }
        catch(SQLException s) {
            reportException(methodCall, s, sql, System.nanoTime() - tstartNano);
            throw s;
        }
    }

    @Override
    public long executeLargeUpdate(String sql, String[] columnNames) throws SQLException {
        String methodCall = "executeLargeUpdate(" + sql + ", " + columnNames + ")";
        reportStatementSql(sql, methodCall);
        long tstartNano = System.nanoTime();
        try {
            long result = delegate.executeLargeUpdate(sql, columnNames);
            reportStatementSqlTiming(System.nanoTime() - tstartNano, sql, methodCall);
            return reportReturn(methodCall, result);
        }
        catch(SQLException s) {
            reportException(methodCall, s, sql, System.nanoTime() - tstartNano);
            throw s;
        }
    }

    public void setMaxFieldSize(int max) throws SQLException {
        String methodCall = "setMaxFieldSize(" + max + ")";
        try {
            delegate.setMaxFieldSize(max);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    public boolean execute(String sql) throws SQLException {
        String methodCall = "execute(" + sql + ")";
        reportStatementSql(sql, methodCall);
        long tstartNano = System.nanoTime();

        try {
            boolean result = delegate.execute(sql);
            reportStatementSqlTiming(System.nanoTime() - tstartNano, sql, methodCall);
            return reportReturn(methodCall, result);
        }
        catch(SQLException s) {
            reportException(methodCall, s, sql, System.nanoTime() - tstartNano);
            throw s;
        }
    }

    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        String methodCall = "executeUpdate(" + sql + ", " + autoGeneratedKeys + ")";
        reportStatementSql(sql, methodCall);
        long tstartNano = System.nanoTime();

        try {
            int result = delegate.executeUpdate(sql, autoGeneratedKeys);
            reportStatementSqlTiming(System.nanoTime() - tstartNano, sql, methodCall);
            return reportReturn(methodCall, result);
        }
        catch(SQLException s) {
            reportException(methodCall, s, sql, System.nanoTime() - tstartNano);
            throw s;
        }
    }

    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        String methodCall = "execute(" + sql + ", " + autoGeneratedKeys + ")";
        reportStatementSql(sql, methodCall);
        long tstartNano = System.nanoTime();

        try {
            boolean result = delegate.execute(sql, autoGeneratedKeys);
            reportStatementSqlTiming(System.nanoTime() - tstartNano, sql, methodCall);
            return reportReturn(methodCall, result);
        }
        catch(SQLException s) {
            reportException(methodCall, s, sql, System.nanoTime() - tstartNano);
            throw s;
        }
    }

    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        String methodCall = "executeUpdate(" + sql + ", " + columnIndexes + ")";
        reportStatementSql(sql, methodCall);
        long tstartNano = System.nanoTime();

        try {
            int result = delegate.executeUpdate(sql, columnIndexes);
            reportStatementSqlTiming(System.nanoTime() - tstartNano, sql, methodCall);
            return reportReturn(methodCall, result);
        }
        catch(SQLException s) {
            reportException(methodCall, s, sql, System.nanoTime() - tstartNano);
            throw s;
        }
    }

    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        String methodCall = "execute(" + sql + ", " + columnIndexes + ")";
        reportStatementSql(sql, methodCall);
        long tstartNano = System.nanoTime();

        try {
            boolean result = delegate.execute(sql, columnIndexes);
            reportStatementSqlTiming(System.nanoTime() - tstartNano, sql, methodCall);
            return reportReturn(methodCall, result);
        }
        catch(SQLException s) {
            reportException(methodCall, s, sql, System.nanoTime() - tstartNano);
            throw s;
        }
    }

    public ResultSet getResultSet() throws SQLException {
        String methodCall = "getResultSet()";
        try {
            ResultSet r = delegate.getResultSet();
            if(r == null) {
                return (ResultSet)reportReturn(methodCall, r);
            }
            else {
                return (ResultSet)reportReturn(methodCall, new LoggingResultSet(this, r));
            }
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    public int getMaxRows() throws SQLException {
        String methodCall = "getMaxRows()";
        try {
            return reportReturn(methodCall, delegate.getMaxRows());
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    public void close() throws SQLException {
        String methodCall = "close()";
        try {
            delegate.close();
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    public int getUpdateCount() throws SQLException {
        String methodCall = "getUpdateCount()";
        try {
            return reportReturn(methodCall, delegate.getUpdateCount());
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        String methodCall = "unwrap(" + (iface == null ? "null" : iface.getName()) + ")";
        try {
            //todo: double check this logic
            return (T)reportReturn(methodCall, (iface != null && (iface == Connection.class || iface == JdbcSpy.class)) ? (T)this : delegate
                .unwrap(iface));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        String methodCall = "isWrapperFor(" + (iface == null ? "null" : iface.getName()) + ")";
        try {
            return reportReturn(methodCall, (iface != null && (iface == Statement.class || iface == JdbcSpy.class)) || delegate.isWrapperFor(iface));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

}
