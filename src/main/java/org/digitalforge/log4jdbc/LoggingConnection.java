package org.digitalforge.log4jdbc;

import java.sql.*;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import org.digitalforge.log4jdbc.formatter.ParameterFormatter;
import org.digitalforge.log4jdbc.util.ConnectionTracker;

/**
 * Wraps a JDBC Connection and reports method calls, returns and exceptions.
 *
 * JDBC 4.2
 */
public class LoggingConnection implements Connection, JdbcSpy {

    private static final SpyLogDelegator log = SpyLogFactory.getSpyLogDelegator();

    private static final AtomicInteger connectionCounter = new AtomicInteger();

    /**
     * Contains a mapping of connectionNumber to open LoggingConnection objects.
     */
    private static final ConnectionTracker connectionTracker = new ConnectionTracker();

    private final int connectionNumber;

    private Connection delegate;
    private ParameterFormatter parameterFormatter;

    public static ConnectionTracker getConnectionTracker() {
        return connectionTracker;
    }

    /**
     * Create a new LoggingConnection that wraps a given Connection.
     *
     * @param delegate &quot;real&quot; Connection that this LoggingConnection wraps.
     */
    public LoggingConnection(final Connection delegate) {
        this(delegate, LoggingDriver.defaultParameterFormatter);
    }

    /**
     * Create a new LoggingConnection that wraps a given Connection.
     *
     * @param delegate &quot;real&quot; Connection that this LoggingConnection wraps.
     * @param parameterFormatter the ParameterFormatter object for formatting logging appropriate for the Rdbms used.
     */
    public LoggingConnection(final Connection delegate, final ParameterFormatter parameterFormatter) {

        if(delegate == null) {
            throw new IllegalArgumentException("Must pass in a non-null real Connection");
        }

        this.delegate = delegate;
        this.parameterFormatter = (parameterFormatter != null) ? parameterFormatter : LoggingDriver.defaultParameterFormatter;

        this.connectionNumber = connectionCounter.incrementAndGet();

        connectionTracker.track(this.connectionNumber, this);

        //log.info("Connection " + this.connectionNumber + " opened");
        log.connectionOpened(this);

        reportReturn("New connection");

    }

    @Override
    public Integer getConnectionNumber() {
        return connectionNumber;
    }

    @Override
    public String getClassType() {
        return "Connection";
    }

    /**
     * Get the real underlying Connection that this LoggingConnection wraps.
     *
     * @return the real underlying Connection.
     */
    public Connection getDelegate() {
        return delegate;
    }

    /**
     * Get the ParameterFormatter object for formatting logging appropriate for the Rdbms used on this connection.
     *
     * @return the ParameterFormatter object for formatting logging appropriate for the Rdbms used.
     */
    ParameterFormatter getParameterFormatter() {
        return parameterFormatter;
    }

    /**
     * Set the ParameterFormatter object for formatting logging appropriate for the Rdbms used on this connection.
     *
     * @param parameterFormatter the ParameterFormatter object for formatting logging appropriate for the Rdbms used.
     */
    void setParameterFormatter(final ParameterFormatter parameterFormatter) {
        this.parameterFormatter = parameterFormatter;
    }

    protected void reportException(String methodCall, SQLException exception, String sql) {
        log.exceptionOccured(this, methodCall, exception, sql, -1L);
    }

    protected void reportException(String methodCall, SQLException exception) {
        log.exceptionOccured(this, methodCall, exception, null, -1L);
    }

    protected void reportAllReturns(String methodCall, String returnValue) {
        log.methodReturned(this, methodCall, returnValue);
    }

    private boolean reportReturn(String methodCall, boolean value) {
        reportAllReturns(methodCall, "" + value);
        return value;
    }

    private int reportReturn(String methodCall, int value) {
        reportAllReturns(methodCall, "" + value);
        return value;
    }

    private <T> T reportReturn(String methodCall, T value) {
        reportAllReturns(methodCall, "" + value);
        return value;
    }

    private void reportReturn(String methodCall) {
        reportAllReturns(methodCall, "");
    }

    // forwarding methods

    @Override
    public boolean isClosed() throws SQLException {
        String methodCall = "isClosed()";
        try {
            return reportReturn(methodCall, (delegate.isClosed()));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        String methodCall = "getWarnings()";
        try {
            return reportReturn(methodCall, delegate.getWarnings());
        }
        catch(SQLException ex) {
            reportException(methodCall, ex);
            throw ex;
        }
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        String methodCall = "setSavepoint()";
        try {
            return reportReturn(methodCall, delegate.setSavepoint());
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        String methodCall = "releaseSavepoint(" + savepoint + ")";
        try {
            delegate.releaseSavepoint(savepoint);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        String methodCall = "rollback(" + savepoint + ")";
        try {
            delegate.rollback(savepoint);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        String methodCall = "getMetaData()";
        try {
            return reportReturn(methodCall, delegate.getMetaData());
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    @Override
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

    @Override
    public Statement createStatement() throws SQLException {
        String methodCall = "createStatement()";
        try {
            Statement statement = delegate.createStatement();
            LoggingStatement lstatement = reportReturn(methodCall, new LoggingStatement(this, statement));
            connectionTracker.track(lstatement);
            return lstatement;
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        String methodCall = "createStatement(" + resultSetType + ", " + resultSetConcurrency + ")";
        try {
            Statement statement = delegate.createStatement(resultSetType, resultSetConcurrency);
            LoggingStatement lstatement = reportReturn(methodCall, new LoggingStatement(this, statement));
            connectionTracker.track(lstatement);
            return lstatement;
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        String methodCall = "createStatement(" + resultSetType + ", " + resultSetConcurrency + ", " + resultSetHoldability + ")";
        try {
            Statement statement = delegate.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
            LoggingStatement lstatement = reportReturn(methodCall, new LoggingStatement(this, statement));
            connectionTracker.track(lstatement);
            return lstatement;
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        String methodCall = "setReadOnly(" + readOnly + ")";
        try {
            delegate.setReadOnly(readOnly);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        String methodCall = "prepareStatement(" + sql + ")";
        try {
            PreparedStatement statement = delegate.prepareStatement(sql);
            LoggingPreparedStatement lstatement = reportReturn(methodCall, new LoggingPreparedStatement(sql, this, statement));
            connectionTracker.track(lstatement);
            return lstatement;

        }
        catch(SQLException s) {
            reportException(methodCall, s, sql);
            throw s;
        }
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        String methodCall = "prepareStatement(" + sql + ", " + autoGeneratedKeys + ")";
        try {
            PreparedStatement statement = delegate.prepareStatement(sql, autoGeneratedKeys);
            LoggingPreparedStatement lstatement = reportReturn(methodCall, new LoggingPreparedStatement(sql, this, statement));
            connectionTracker.track(lstatement);
            return lstatement;
        }
        catch(SQLException s) {
            reportException(methodCall, s, sql);
            throw s;
        }
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        String methodCall = "prepareStatement(" + sql + ", " + resultSetType + ", " + resultSetConcurrency + ")";
        try {
            PreparedStatement statement = delegate.prepareStatement(sql, resultSetType, resultSetConcurrency);
            LoggingPreparedStatement lstatement = reportReturn(methodCall, new LoggingPreparedStatement(sql, this, statement));
            connectionTracker.track(lstatement);
            return lstatement;
        }
        catch(SQLException s) {
            reportException(methodCall, s, sql);
            throw s;
        }
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        String methodCall = "prepareStatement(" + sql + ", " + resultSetType + ", " + resultSetConcurrency + ", " + resultSetHoldability + ")";
        try {
            PreparedStatement statement = delegate.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
            LoggingPreparedStatement lstatement = reportReturn(methodCall, new LoggingPreparedStatement(sql, this, statement));
            connectionTracker.track(lstatement);
            return lstatement;
        }
        catch(SQLException s) {
            reportException(methodCall, s, sql);
            throw s;
        }
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int columnIndexes[]) throws SQLException {
        //todo: dump the array here?
        String methodCall = "prepareStatement(" + sql + ", " + columnIndexes + ")";
        try {
            PreparedStatement statement = delegate.prepareStatement(sql, columnIndexes);
            LoggingPreparedStatement lstatement = reportReturn(methodCall, new LoggingPreparedStatement(sql, this, statement));
            connectionTracker.track(lstatement);
            return lstatement;
        }
        catch(SQLException s) {
            reportException(methodCall, s, sql);
            throw s;
        }
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        String methodCall = "setSavepoint(" + name + ")";
        try {
            return reportReturn(methodCall, delegate.setSavepoint(name));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String columnNames[]) throws SQLException {
        //todo: dump the array here?
        String methodCall = "prepareStatement(" + sql + ", " + columnNames + ")";
        try {
            PreparedStatement statement = delegate.prepareStatement(sql, columnNames);
            LoggingPreparedStatement lstatement = reportReturn(methodCall, new LoggingPreparedStatement(sql, this, statement));
            connectionTracker.track(lstatement);
            return lstatement;
        }
        catch(SQLException s) {
            reportException(methodCall, s, sql);
            throw s;
        }
    }

    @Override
    public Clob createClob() throws SQLException {
        String methodCall = "createClob()";
        try {
            return reportReturn(methodCall, delegate.createClob());
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    @Override
    public Blob createBlob() throws SQLException {
        String methodCall = "createBlob()";
        try {
            return reportReturn(methodCall, delegate.createBlob());
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    @Override
    public NClob createNClob() throws SQLException {
        String methodCall = "createNClob()";
        try {
            return reportReturn(methodCall, delegate.createNClob());
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        String methodCall = "createSQLXML()";
        try {
            return reportReturn(methodCall, delegate.createSQLXML());
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        String methodCall = "isValid(" + timeout + ")";
        try {
            return reportReturn(methodCall, delegate.isValid(timeout));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        String methodCall = "setClientInfo(" + name + ", " + value + ")";
        try {
            delegate.setClientInfo(name, value);
        }
        catch(SQLClientInfoException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        // todo: dump properties?
        String methodCall = "setClientInfo(" + properties + ")";
        try {
            delegate.setClientInfo(properties);
        }
        catch(SQLClientInfoException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        String methodCall = "getClientInfo(" + name + ")";
        try {
            return reportReturn(methodCall, delegate.getClientInfo(name));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        String methodCall = "getClientInfo()";
        try {
            return reportReturn(methodCall, delegate.getClientInfo());
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        //todo: dump elements?
        String methodCall = "createArrayOf(" + typeName + ", " + elements + ")";
        try {
            return reportReturn(methodCall, delegate.createArrayOf(typeName, elements));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        //todo: dump attributes?
        String methodCall = "createStruct(" + typeName + ", " + attributes + ")";
        try {
            return reportReturn(methodCall, delegate.createStruct(typeName, attributes));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        String methodCall = "setSchema(" + schema + ")";
        try {
            delegate.setSchema(schema);
        }
        catch(SQLClientInfoException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public String getSchema() throws SQLException {
        String methodCall = "getSchema()";
        try {
            return reportReturn(methodCall, delegate.getSchema());
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        String methodCall = "abort(" + executor + ")";
        try {
            delegate.abort(executor);
        }
        catch(SQLClientInfoException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        String methodCall = "setNetworkTimeout(" + executor + ", " + milliseconds + ")";
        try {
            delegate.setNetworkTimeout(executor, milliseconds);
        }
        catch(SQLClientInfoException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        String methodCall = "getNetworkTimeout()";
        try {
            return reportReturn(methodCall, delegate.getNetworkTimeout());
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        String methodCall = "isReadOnly()";
        try {
            return reportReturn(methodCall, delegate.isReadOnly());
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        String methodCall = "setHoldability(" + holdability + ")";
        try {
            delegate.setHoldability(holdability);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        String methodCall = "prepareCall(" + sql + ")";
        try {
            CallableStatement statement = delegate.prepareCall(sql);
            LoggingCallableStatement lstatement = reportReturn(methodCall, new LoggingCallableStatement(sql, this, statement));
            connectionTracker.track(lstatement);
            return lstatement;
        }
        catch(SQLException s) {
            reportException(methodCall, s, sql);
            throw s;
        }
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        String methodCall = "prepareCall(" + sql + ", " + resultSetType + ", " + resultSetConcurrency + ")";
        try {
            CallableStatement statement = delegate.prepareCall(sql, resultSetType, resultSetConcurrency);
            LoggingCallableStatement lstatement = reportReturn(methodCall, new LoggingCallableStatement(sql, this, statement));
            connectionTracker.track(lstatement);
            return lstatement;
        }
        catch(SQLException s) {
            reportException(methodCall, s, sql);
            throw s;
        }
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        String methodCall = "prepareCall(" + sql + ", " + resultSetType + ", " + resultSetConcurrency + ", " + resultSetHoldability + ")";
        try {
            CallableStatement statement = delegate.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
            LoggingCallableStatement lstatement = reportReturn(methodCall, new LoggingCallableStatement(sql, this, statement));
            connectionTracker.track(lstatement);
            return lstatement;
        }
        catch(SQLException s) {
            reportException(methodCall, s, sql);
            throw s;
        }
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        String methodCall = "setCatalog(" + catalog + ")";
        try {
            delegate.setCatalog(catalog);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        String methodCall = "nativeSQL(" + sql + ")";
        try {
            return reportReturn(methodCall, delegate.nativeSQL(sql));
        }
        catch(SQLException s) {
            reportException(methodCall, s, sql);
            throw s;
        }
    }

    @Override
    public Map<String,Class<?>> getTypeMap() throws SQLException {
        String methodCall = "getTypeMap()";
        try {
            return reportReturn(methodCall, delegate.getTypeMap());
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        String methodCall = "setAutoCommit(" + autoCommit + ")";
        try {
            delegate.setAutoCommit(autoCommit);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public String getCatalog() throws SQLException {
        String methodCall = "getCatalog()";
        try {
            return reportReturn(methodCall, delegate.getCatalog());
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    @Override
    public void setTypeMap(java.util.Map<String,Class<?>> map) throws SQLException {
        //todo: dump map??
        String methodCall = "setTypeMap(" + map + ")";
        try {
            delegate.setTypeMap(map);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        String methodCall = "setTransactionIsolation(" + level + ")";
        try {
            delegate.setTransactionIsolation(level);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        String methodCall = "getAutoCommit()";
        try {
            return reportReturn(methodCall, delegate.getAutoCommit());
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    @Override
    public int getHoldability() throws SQLException {
        String methodCall = "getHoldability()";
        try {
            return reportReturn(methodCall, delegate.getHoldability());
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        String methodCall = "getTransactionIsolation()";
        try {
            return reportReturn(methodCall, delegate.getTransactionIsolation());
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    @Override
    public void commit() throws SQLException {
        String methodCall = "commit()";
        try {
            delegate.commit();
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public void rollback() throws SQLException {
        String methodCall = "rollback()";
        try {
            delegate.rollback();
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public void close() throws SQLException {
        String methodCall = "close()";
        try {
            delegate.close();
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        finally {
            connectionTracker.untrack(connectionNumber);
            log.connectionClosed(this);
        }
        reportReturn(methodCall);
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        String methodCall = "unwrap(" + (iface == null ? "null" : iface.getName()) + ")";
        try {
            //todo: double check this logic
            return reportReturn(methodCall, (iface != null && (iface == Connection.class || iface == JdbcSpy.class)) ? (T)this : delegate.unwrap(iface));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        String methodCall = "isWrapperFor(" + (iface == null ? "null" : iface.getName()) + ")";
        try {
            return reportReturn(methodCall, (iface != null && (iface == Connection.class || iface == JdbcSpy.class)) || delegate.isWrapperFor(iface));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

}