package org.digitalforge.log4jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;
import java.util.Map;

/**
 * Wraps a ResultSet and reports method calls, returns and exceptions.
 *
 * JDBC 4.2
 */
public class LoggingResultSet implements ResultSet, JdbcSpy {

    private final SpyLogDelegator log;

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
     * Report (for logging) that a method returned.  All the other reportReturn methods are conveniance methods that call
     * this method.
     *
     * @param methodCall description of method call and arguments passed to it that returned.
     * @param msg description of what the return value that was returned.  may be an empty String for void return types.
     */
    protected void reportAllReturns(String methodCall, String msg) {
        log.methodReturned(this, methodCall, msg);
    }

    private ResultSet delegate;

    /**
     * Get the real ResultSet that this ResultSetSpy wraps.
     *
     * @return the real ResultSet that this ResultSetSpy wraps.
     */
    public ResultSet getDelegate() {
        return delegate;
    }

    private LoggingStatement parent;

    /**
     * Create a new ResultSetSpy that wraps another ResultSet object, that logs all method calls, expceptions, etc.
     *
     * @param parent Statement that generated this ResultSet.
     * @param delegate real underlying ResultSet that is being wrapped.
     */
    public LoggingResultSet(LoggingStatement parent, ResultSet delegate) {
        if(delegate == null) {
            throw new IllegalArgumentException("Must provide a non null real ResultSet");
        }
        this.delegate = delegate;
        this.parent = parent;
        log = SpyLogFactory.getSpyLogDelegator();
        reportReturn("new ResultSet");
    }

    public String getClassType() {
        return "ResultSet";
    }

    public Integer getConnectionNumber() {
        return parent.getConnectionNumber();
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

    // forwarding methods

    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        String methodCall = "updateAsciiStream(" + columnIndex + ", " + x + ", " + length + ")";
        try {
            delegate.updateAsciiStream(columnIndex, x, length);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    public void updateAsciiStream(String columnName, InputStream x, int length) throws SQLException {
        String methodCall = "updateAsciiStream(" + columnName + ", " + x + ", " + length + ")";
        try {
            delegate.updateAsciiStream(columnName, x, length);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    public int getRow() throws SQLException {
        String methodCall = "getRow()";
        try {
            return reportReturn(methodCall, delegate.getRow());
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    public void cancelRowUpdates() throws SQLException {
        String methodCall = "cancelRowUpdates()";
        try {
            delegate.cancelRowUpdates();
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    public Time getTime(int columnIndex) throws SQLException {
        String methodCall = "getTime(" + columnIndex + ")";
        try {
            return (Time)reportReturn(methodCall, delegate.getTime(columnIndex));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    public Time getTime(String columnName) throws SQLException {
        String methodCall = "getTime(" + columnName + ")";
        try {
            return (Time)reportReturn(methodCall, delegate.getTime(columnName));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        String methodCall = "getTime(" + columnIndex + ", " + cal + ")";
        try {
            return (Time)reportReturn(methodCall, delegate.getTime(columnIndex, cal));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    public Time getTime(String columnName, Calendar cal) throws SQLException {
        String methodCall = "getTime(" + columnName + ", " + cal + ")";
        try {
            return (Time)reportReturn(methodCall, delegate.getTime(columnName, cal));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    public boolean absolute(int row) throws SQLException {
        String methodCall = "absolute(" + row + ")";
        try {
            return reportReturn(methodCall, delegate.absolute(row));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        String methodCall = "getTimestamp(" + columnIndex + ")";
        try {
            return (Timestamp)reportReturn(methodCall, delegate.getTimestamp(columnIndex));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    public Timestamp getTimestamp(String columnName) throws SQLException {
        String methodCall = "getTimestamp(" + columnName + ")";
        try {
            return (Timestamp)reportReturn(methodCall, delegate.getTimestamp(columnName));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }

    }

    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        String methodCall = "getTimestamp(" + columnIndex + ", " + cal + ")";
        try {
            return (Timestamp)reportReturn(methodCall, delegate.getTimestamp(columnIndex, cal));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }

    }

    public Timestamp getTimestamp(String columnName, Calendar cal) throws SQLException {
        String methodCall = "getTimestamp(" + columnName + ", " + cal + ")";
        try {
            return (Timestamp)reportReturn(methodCall, delegate.getTimestamp(columnName, cal));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    public void moveToInsertRow() throws SQLException {
        String methodCall = "moveToInsertRow()";
        try {
            delegate.moveToInsertRow();
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    public boolean relative(int rows) throws SQLException {
        String methodCall = "relative(" + rows + ")";
        try {
            return reportReturn(methodCall, delegate.relative(rows));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    public boolean previous() throws SQLException {
        String methodCall = "previous()";
        try {
            return reportReturn(methodCall, delegate.previous());
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    public void moveToCurrentRow() throws SQLException {
        String methodCall = "moveToCurrentRow()";
        try {
            delegate.moveToCurrentRow();
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    public Ref getRef(int i) throws SQLException {
        String methodCall = "getRef(" + i + ")";
        try {
            return (Ref)reportReturn(methodCall, delegate.getRef(i));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    public void updateRef(int columnIndex, Ref x) throws SQLException {
        String methodCall = "updateRef(" + columnIndex + ", " + x + ")";
        try {
            delegate.updateRef(columnIndex, x);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    public Ref getRef(String colName) throws SQLException {
        String methodCall = "getRef(" + colName + ")";
        try {
            return (Ref)reportReturn(methodCall, delegate.getRef(colName));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    public void updateRef(String columnName, Ref x) throws SQLException {
        String methodCall = "updateRef(" + columnName + ", " + x + ")";
        try {
            delegate.updateRef(columnName, x);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    public Blob getBlob(int i) throws SQLException {
        String methodCall = "getBlob(" + i + ")";
        try {
            return (Blob)reportReturn(methodCall, delegate.getBlob(i));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        String methodCall = "updateBlob(" + columnIndex + ", " + x + ")";
        try {
            delegate.updateBlob(columnIndex, x);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    public Blob getBlob(String colName) throws SQLException {
        String methodCall = "getBlob(" + colName + ")";
        try {
            return (Blob)reportReturn(methodCall, delegate.getBlob(colName));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    public void updateBlob(String columnName, Blob x) throws SQLException {
        String methodCall = "updateBlob(" + columnName + ", " + x + ")";
        try {
            delegate.updateBlob(columnName, x);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    public Clob getClob(int i) throws SQLException {
        String methodCall = "getClob(" + i + ")";
        try {
            return (Clob)reportReturn(methodCall, delegate.getClob(i));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    public void updateClob(int columnIndex, Clob x) throws SQLException {
        String methodCall = "updateClob(" + columnIndex + ", " + x + ")";
        try {
            delegate.updateClob(columnIndex, x);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    public Clob getClob(String colName) throws SQLException {
        String methodCall = "getClob(" + colName + ")";
        try {
            return (Clob)reportReturn(methodCall, delegate.getClob(colName));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    public void updateClob(String columnName, Clob x) throws SQLException {
        String methodCall = "updateClob(" + columnName + ", " + x + ")";
        try {
            delegate.updateClob(columnName, x);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    public boolean getBoolean(int columnIndex) throws SQLException {
        String methodCall = "getBoolean(" + columnIndex + ")";
        try {
            return reportReturn(methodCall, delegate.getBoolean(columnIndex));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    public boolean getBoolean(String columnName) throws SQLException {
        String methodCall = "getBoolean(" + columnName + ")";
        try {
            return reportReturn(methodCall, delegate.getBoolean(columnName));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    public Array getArray(int i) throws SQLException {
        String methodCall = "getArray(" + i + ")";
        try {
            return (Array)reportReturn(methodCall, delegate.getArray(i));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    public void updateArray(int columnIndex, Array x) throws SQLException {
        String methodCall = "updateArray(" + columnIndex + ", " + x + ")";
        try {
            delegate.updateArray(columnIndex, x);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    public Array getArray(String colName) throws SQLException {
        String methodCall = "getArray(" + colName + ")";
        try {
            return (Array)reportReturn(methodCall, delegate.getArray(colName));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    public void updateArray(String columnName, Array x) throws SQLException {
        String methodCall = "updateArray(" + columnName + ", " + x + ")";
        try {
            delegate.updateArray(columnName, x);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    public RowId getRowId(int columnIndex) throws SQLException {
        String methodCall = "getRowId(" + columnIndex + ")";
        try {
            return (RowId)reportReturn(methodCall, delegate.getRowId(columnIndex));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    public RowId getRowId(String columnLabel) throws SQLException {
        String methodCall = "getRowId(" + columnLabel + ")";
        try {
            return (RowId)reportReturn(methodCall, delegate.getRowId(columnLabel));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        String methodCall = "updateRowId(" + columnIndex + ", " + x + ")";
        try {
            delegate.updateRowId(columnIndex, x);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        String methodCall = "updateRowId(" + columnLabel + ", " + x + ")";
        try {
            delegate.updateRowId(columnLabel, x);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

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

    public void updateNString(int columnIndex, String nString) throws SQLException {
        String methodCall = "updateNString(" + columnIndex + ", " + nString + ")";
        try {
            delegate.updateNString(columnIndex, nString);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    public void updateNString(String columnLabel, String nString) throws SQLException {
        String methodCall = "updateNString(" + columnLabel + ", " + nString + ")";
        try {
            delegate.updateNString(columnLabel, nString);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        String methodCall = "updateNClob(" + columnIndex + ", " + nClob + ")";
        try {
            delegate.updateNClob(columnIndex, nClob);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
        String methodCall = "updateNClob(" + columnLabel + ", " + nClob + ")";
        try {
            delegate.updateNClob(columnLabel, nClob);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    public NClob getNClob(int columnIndex) throws SQLException {
        String methodCall = "getNClob(" + columnIndex + ")";
        try {
            return (NClob)reportReturn(methodCall, delegate.getNClob(columnIndex));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    public NClob getNClob(String columnLabel) throws SQLException {
        String methodCall = "getNClob(" + columnLabel + ")";
        try {
            return (NClob)reportReturn(methodCall, delegate.getNClob(columnLabel));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        String methodCall = "getSQLXML(" + columnIndex + ")";
        try {
            return (SQLXML)reportReturn(methodCall, delegate.getSQLXML(columnIndex));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        String methodCall = "getSQLXML(" + columnLabel + ")";
        try {
            return (SQLXML)reportReturn(methodCall, delegate.getSQLXML(columnLabel));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        String methodCall = "updateSQLXML(" + columnIndex + ", " + xmlObject + ")";
        try {
            delegate.updateSQLXML(columnIndex, xmlObject);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        String methodCall = "updateSQLXML(" + columnLabel + ", " + xmlObject + ")";
        try {
            delegate.updateSQLXML(columnLabel, xmlObject);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    public String getNString(int columnIndex) throws SQLException {
        String methodCall = "getNString(" + columnIndex + ")";
        try {
            return (String)reportReturn(methodCall, delegate.getNString(columnIndex));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    public String getNString(String columnLabel) throws SQLException {
        String methodCall = "getNString(" + columnLabel + ")";
        try {
            return (String)reportReturn(methodCall, delegate.getNString(columnLabel));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        String methodCall = "getNCharacterStream(" + columnIndex + ")";
        try {
            return (Reader)reportReturn(methodCall, delegate.getNCharacterStream(columnIndex));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        String methodCall = "getNCharacterStream(" + columnLabel + ")";
        try {
            return (Reader)reportReturn(methodCall, delegate.getNCharacterStream(columnLabel));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        String methodCall = "updateNCharacterStream(" + columnIndex + ", " + x + ", " + length + ")";
        try {
            delegate.updateNCharacterStream(columnIndex, x, length);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        String methodCall = "updateNCharacterStream(" + columnLabel + ", " + reader + ", " + length + ")";
        try {
            delegate.updateNCharacterStream(columnLabel, reader, length);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        String methodCall = "updateAsciiStream(" + columnIndex + ", " + x + ", " + length + ")";
        try {
            delegate.updateAsciiStream(columnIndex, x, length);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        String methodCall = "updateBinaryStream(" + columnIndex + ", " + x + ", " + length + ")";
        try {
            delegate.updateBinaryStream(columnIndex, x, length);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        String methodCall = "updateCharacterStream(" + columnIndex + ", " + x + ", " + length + ")";
        try {
            delegate.updateCharacterStream(columnIndex, x, length);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
        String methodCall = "updateAsciiStream(" + columnLabel + ", " + x + ", " + length + ")";
        try {
            delegate.updateAsciiStream(columnLabel, x, length);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
        String methodCall = "updateBinaryStream(" + columnLabel + ", " + x + ", " + length + ")";
        try {
            delegate.updateBinaryStream(columnLabel, x, length);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        String methodCall = "updateCharacterStream(" + columnLabel + ", " + reader + ", " + length + ")";
        try {
            delegate.updateCharacterStream(columnLabel, reader, length);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
        String methodCall = "updateBlob(" + columnIndex + ", " + inputStream + ", " + length + ")";
        try {
            delegate.updateBlob(columnIndex, inputStream, length);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        String methodCall = "updateBlob(" + columnLabel + ", " + inputStream + ", " + length + ")";
        try {
            delegate.updateBlob(columnLabel, inputStream, length);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        String methodCall = "updateClob(" + columnIndex + ", " + reader + ", " + length + ")";
        try {
            delegate.updateClob(columnIndex, reader, length);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        String methodCall = "updateClob(" + columnLabel + ", " + reader + ", " + length + ")";
        try {
            delegate.updateClob(columnLabel, reader, length);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        String methodCall = "updateNClob(" + columnIndex + ", " + reader + ", " + length + ")";
        try {
            delegate.updateNClob(columnIndex, reader, length);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        String methodCall = "updateNClob(" + columnLabel + ", " + reader + ", " + length + ")";
        try {
            delegate.updateNClob(columnLabel, reader, length);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    public void updateNCharacterStream(int columnIndex, Reader reader) throws SQLException {
        String methodCall = "updateNCharacterStream(" + columnIndex + ", " + reader + ")";
        try {
            delegate.updateNCharacterStream(columnIndex, reader);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        String methodCall = "updateNCharacterStream(" + columnLabel + ", " + reader + ")";
        try {
            delegate.updateNCharacterStream(columnLabel, reader);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        String methodCall = "updateAsciiStream(" + columnIndex + ", " + x + ")";
        try {
            delegate.updateAsciiStream(columnIndex, x);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        String methodCall = "updateBinaryStream(" + columnIndex + ", " + x + ")";
        try {
            delegate.updateBinaryStream(columnIndex, x);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        String methodCall = "updateCharacterStream(" + columnIndex + ", " + x + ")";
        try {
            delegate.updateCharacterStream(columnIndex, x);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        String methodCall = "updateAsciiStream(" + columnLabel + ", " + x + ")";
        try {
            delegate.updateAsciiStream(columnLabel, x);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        String methodCall = "updateBinaryStream(" + columnLabel + ", " + x + ")";
        try {
            delegate.updateBinaryStream(columnLabel, x);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        String methodCall = "updateCharacterStream(" + columnLabel + ", " + reader + ")";
        try {
            delegate.updateCharacterStream(columnLabel, reader);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        String methodCall = "updateBlob(" + columnIndex + ", " + inputStream + ")";
        try {
            delegate.updateBlob(columnIndex, inputStream);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        String methodCall = "updateBlob(" + columnLabel + ", " + inputStream + ")";
        try {
            delegate.updateBlob(columnLabel, inputStream);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        String methodCall = "updateClob(" + columnIndex + ", " + reader + ")";
        try {
            delegate.updateClob(columnIndex, reader);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        String methodCall = "updateClob(" + columnLabel + ", " + reader + ")";
        try {
            delegate.updateClob(columnLabel, reader);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        String methodCall = "updateNClob(" + columnIndex + ", " + reader + ")";
        try {
            delegate.updateNClob(columnIndex, reader);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        String methodCall = "updateNClob(" + columnLabel + ", " + reader + ")";
        try {
            delegate.updateNClob(columnLabel, reader);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        String methodCall = "getObject(" + columnIndex + ", " + type.getName() + ")";
        try {
            return (T)reportReturn(methodCall, delegate.getObject(columnIndex, type));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        String methodCall = "getObject(" + columnLabel + ", " + type.getName() + ")";
        try {
            return (T)reportReturn(methodCall, delegate.getObject(columnLabel, type));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    @Override
    public void updateObject(int columnIndex, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
        String methodCall = "updateObject(" + columnIndex + ", " + x + ", " + targetSqlType.getName() + ", " + scaleOrLength + ")";
        try {
            delegate.updateObject(columnIndex, x, targetSqlType, scaleOrLength);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public void updateObject(String columnLabel, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
        String methodCall = "updateObject(" + columnLabel + ", " + x + ", " + targetSqlType.getName() + ", " + scaleOrLength + ")";
        try {
            delegate.updateObject(columnLabel, x, targetSqlType, scaleOrLength);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public void updateObject(int columnIndex, Object x, SQLType targetSqlType) throws SQLException {
        String methodCall = "updateObject(" + columnIndex + ", " + x + ", " + targetSqlType.getName() + ")";
        try {
            delegate.updateObject(columnIndex, x, targetSqlType);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public void updateObject(String columnLabel, Object x, SQLType targetSqlType) throws SQLException {
        String methodCall = "updateObject(" + columnLabel + ", " + x + ", " + targetSqlType.getName() + ")";
        try {
            delegate.updateObject(columnLabel, x, targetSqlType);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    public boolean isBeforeFirst() throws SQLException {
        String methodCall = "isBeforeFirst()";
        try {
            return reportReturn(methodCall, delegate.isBeforeFirst());
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    public short getShort(int columnIndex) throws SQLException {
        String methodCall = "getShort(" + columnIndex + ")";
        try {
            return reportReturn(methodCall, delegate.getShort(columnIndex));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    public short getShort(String columnName) throws SQLException {
        String methodCall = "getShort(" + columnName + ")";
        try {
            return reportReturn(methodCall, delegate.getShort(columnName));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    public int getInt(int columnIndex) throws SQLException {
        String methodCall = "getInt(" + columnIndex + ")";
        try {
            return reportReturn(methodCall, delegate.getInt(columnIndex));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    public int getInt(String columnName) throws SQLException {
        String methodCall = "getInt(" + columnName + ")";
        try {
            return reportReturn(methodCall, delegate.getInt(columnName));
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

    public ResultSetMetaData getMetaData() throws SQLException {
        String methodCall = "getMetaData()";
        try {
            return (ResultSetMetaData)reportReturn(methodCall, delegate.getMetaData());
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    public int getType() throws SQLException {
        String methodCall = "getType()";
        try {
            return reportReturn(methodCall, delegate.getType());
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    public double getDouble(int columnIndex) throws SQLException {
        String methodCall = "getDouble(" + columnIndex + ")";
        try {
            return reportReturn(methodCall, delegate.getDouble(columnIndex));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    public double getDouble(String columnName) throws SQLException {
        String methodCall = "getDouble(" + columnName + ")";
        try {
            return reportReturn(methodCall, delegate.getDouble(columnName));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    public void deleteRow() throws SQLException {
        String methodCall = "deleteRow()";
        try {
            delegate.deleteRow();
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    public int getConcurrency() throws SQLException {
        String methodCall = "getConcurrency()";
        try {
            return reportReturn(methodCall, delegate.getConcurrency());
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    public boolean rowUpdated() throws SQLException {
        String methodCall = "rowUpdated()";
        try {
            return reportReturn(methodCall, delegate.rowUpdated());
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    public Date getDate(int columnIndex) throws SQLException {
        String methodCall = "getDate(" + columnIndex + ")";
        try {
            return (Date)reportReturn(methodCall, delegate.getDate(columnIndex));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    public Date getDate(String columnName) throws SQLException {
        String methodCall = "getDate(" + columnName + ")";
        try {
            return (Date)reportReturn(methodCall, delegate.getDate(columnName));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        String methodCall = "getDate(" + columnIndex + ", " + cal + ")";
        try {
            return (Date)reportReturn(methodCall, delegate.getDate(columnIndex, cal));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }

    }

    public Date getDate(String columnName, Calendar cal) throws SQLException {
        String methodCall = "getDate(" + columnName + ", " + cal + ")";
        try {
            return (Date)reportReturn(methodCall, delegate.getDate(columnName, cal));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    public boolean last() throws SQLException {
        String methodCall = "last()";
        try {
            return reportReturn(methodCall, delegate.last());
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    public boolean rowInserted() throws SQLException {
        String methodCall = "rowInserted()";
        try {
            return reportReturn(methodCall, delegate.rowInserted());
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    public boolean rowDeleted() throws SQLException {
        String methodCall = "rowDeleted()";
        try {
            return reportReturn(methodCall, delegate.rowDeleted());
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    public void updateNull(int columnIndex) throws SQLException {
        String methodCall = "updateNull(" + columnIndex + ")";
        try {
            delegate.updateNull(columnIndex);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    public void updateNull(String columnName) throws SQLException {
        String methodCall = "updateNull(" + columnName + ")";
        try {
            delegate.updateNull(columnName);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    public void updateShort(int columnIndex, short x) throws SQLException {
        String methodCall = "updateShort(" + columnIndex + ", " + x + ")";
        try {
            delegate.updateShort(columnIndex, x);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    public void updateShort(String columnName, short x) throws SQLException {
        String methodCall = "updateShort(" + columnName + ", " + x + ")";
        try {
            delegate.updateShort(columnName, x);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        String methodCall = "updateBoolean(" + columnIndex + ", " + x + ")";
        try {
            delegate.updateBoolean(columnIndex, x);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    public void updateBoolean(String columnName, boolean x) throws SQLException {
        String methodCall = "updateBoolean(" + columnName + ", " + x + ")";
        try {
            delegate.updateBoolean(columnName, x);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    public void updateByte(int columnIndex, byte x) throws SQLException {
        String methodCall = "updateByte(" + columnIndex + ", " + x + ")";
        try {
            delegate.updateByte(columnIndex, x);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    public void updateByte(String columnName, byte x) throws SQLException {
        String methodCall = "updateByte(" + columnName + ", " + x + ")";
        try {
            delegate.updateByte(columnName, x);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    public void updateInt(int columnIndex, int x) throws SQLException {
        String methodCall = "updateInt(" + columnIndex + ", " + x + ")";
        try {
            delegate.updateInt(columnIndex, x);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    public void updateInt(String columnName, int x) throws SQLException {
        String methodCall = "updateInt(" + columnName + ", " + x + ")";
        try {
            delegate.updateInt(columnName, x);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    public Object getObject(int columnIndex) throws SQLException {
        String methodCall = "getObject(" + columnIndex + ")";
        try {
            return reportReturn(methodCall, delegate.getObject(columnIndex));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    public Object getObject(String columnName) throws SQLException {
        String methodCall = "getObject(" + columnName + ")";
        try {
            return reportReturn(methodCall, delegate.getObject(columnName));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    public Object getObject(String colName, Map map) throws SQLException {
        String methodCall = "getObject(" + colName + ", " + map + ")";
        try {
            return reportReturn(methodCall, delegate.getObject(colName, map));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    public boolean next() throws SQLException {
        String methodCall = "next()";
        try {
            return reportReturn(methodCall, delegate.next());
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    public void updateLong(int columnIndex, long x) throws SQLException {
        String methodCall = "updateLong(" + columnIndex + ", " + x + ")";
        try {
            delegate.updateLong(columnIndex, x);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    public void updateLong(String columnName, long x) throws SQLException {
        String methodCall = "updateLong(" + columnName + ", " + x + ")";
        try {
            delegate.updateLong(columnName, x);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    public void updateFloat(int columnIndex, float x) throws SQLException {
        String methodCall = "updateFloat(" + columnIndex + ", " + x + ")";
        try {
            delegate.updateFloat(columnIndex, x);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);

    }

    public void updateFloat(String columnName, float x) throws SQLException {
        String methodCall = "updateFloat(" + columnName + ", " + x + ")";
        try {
            delegate.updateFloat(columnName, x);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    public void updateDouble(int columnIndex, double x) throws SQLException {
        String methodCall = "updateDouble(" + columnIndex + ", " + x + ")";
        try {
            delegate.updateDouble(columnIndex, x);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    public void updateDouble(String columnName, double x) throws SQLException {
        String methodCall = "updateDouble(" + columnName + ", " + x + ")";
        try {
            delegate.updateDouble(columnName, x);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    public Statement getStatement() throws SQLException {
        String methodCall = "getStatement()";
        return (Statement)reportReturn(methodCall, parent);
    }

    public Object getObject(int columnIndex, Map<String,Class<?>> map) throws SQLException {
        String methodCall = "getObject(" + columnIndex + ", " + map + ")";
        try {
            return reportReturn(methodCall, delegate.getObject(columnIndex, map));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    public void updateString(int columnIndex, String x) throws SQLException {
        String methodCall = "updateString(" + columnIndex + ", " + x + ")";
        try {
            delegate.updateString(columnIndex, x);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    public void updateString(String columnName, String x) throws SQLException {
        String methodCall = "updateString(" + columnName + ", " + x + ")";
        try {
            delegate.updateString(columnName, x);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        String methodCall = "getAsciiStream(" + columnIndex + ")";
        try {
            return (InputStream)reportReturn(methodCall, delegate.getAsciiStream(columnIndex));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    public InputStream getAsciiStream(String columnName) throws SQLException {
        String methodCall = "getAsciiStream(" + columnName + ")";
        try {
            return (InputStream)reportReturn(methodCall, delegate.getAsciiStream(columnName));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        String methodCall = "updateBigDecimal(" + columnIndex + ", " + x + ")";
        try {
            delegate.updateBigDecimal(columnIndex, x);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    public URL getURL(int columnIndex) throws SQLException {
        String methodCall = "getURL(" + columnIndex + ")";
        try {
            return (URL)reportReturn(methodCall, delegate.getURL(columnIndex));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    public void updateBigDecimal(String columnName, BigDecimal x) throws SQLException {
        String methodCall = "updateBigDecimal(" + columnName + ", " + x + ")";
        try {
            delegate.updateBigDecimal(columnName, x);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    public URL getURL(String columnName) throws SQLException {
        String methodCall = "getURL(" + columnName + ")";
        try {
            return (URL)reportReturn(methodCall, delegate.getURL(columnName));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        // todo: dump array?
        String methodCall = "updateBytes(" + columnIndex + ", " + x + ")";
        try {
            delegate.updateBytes(columnIndex, x);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    public void updateBytes(String columnName, byte[] x) throws SQLException {
        // todo: dump array?
        String methodCall = "updateBytes(" + columnName + ", " + x + ")";
        try {
            delegate.updateBytes(columnName, x);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    /**
     * @deprecated
     */
    @Deprecated
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        String methodCall = "getUnicodeStream(" + columnIndex + ")";
        try {
            return (InputStream)reportReturn(methodCall, delegate.getUnicodeStream(columnIndex));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    /**
     * @deprecated
     */
    @Deprecated
    public InputStream getUnicodeStream(String columnName) throws SQLException {
        String methodCall = "getUnicodeStream(" + columnName + ")";
        try {
            return (InputStream)reportReturn(methodCall, delegate.getUnicodeStream(columnName));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    public void updateDate(int columnIndex, Date x) throws SQLException {
        String methodCall = "updateDate(" + columnIndex + ", " + x + ")";
        try {
            delegate.updateDate(columnIndex, x);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    public void updateDate(String columnName, Date x) throws SQLException {
        String methodCall = "updateDate(" + columnName + ", " + x + ")";
        try {
            delegate.updateDate(columnName, x);
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

    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        String methodCall = "getBinaryStream(" + columnIndex + ")";
        try {
            return (InputStream)reportReturn(methodCall, delegate.getBinaryStream(columnIndex));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    public InputStream getBinaryStream(String columnName) throws SQLException {
        String methodCall = "getBinaryStream(" + columnName + ")";
        try {
            return (InputStream)reportReturn(methodCall, delegate.getBinaryStream(columnName));
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

    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        String methodCall = "updateTimestamp(" + columnIndex + ", " + x + ")";
        try {
            delegate.updateTimestamp(columnIndex, x);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    public void updateTimestamp(String columnName, Timestamp x) throws SQLException {
        String methodCall = "updateTimestamp(" + columnName + ", " + x + ")";
        try {
            delegate.updateTimestamp(columnName, x);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    public boolean first() throws SQLException {
        String methodCall = "first()";
        try {
            return reportReturn(methodCall, delegate.first());
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    public String getCursorName() throws SQLException {
        String methodCall = "getCursorName()";
        try {
            return (String)reportReturn(methodCall, delegate.getCursorName());
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    public int findColumn(String columnName) throws SQLException {
        String methodCall = "findColumn(" + columnName + ")";
        try {
            return reportReturn(methodCall, delegate.findColumn(columnName));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    public boolean wasNull() throws SQLException {
        String methodCall = "wasNull()";
        try {
            return reportReturn(methodCall, delegate.wasNull());
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        String methodCall = "updateBinaryStream(" + columnIndex + ", " + x + ", " + length + ")";
        try {
            delegate.updateBinaryStream(columnIndex, x, length);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    public void updateBinaryStream(String columnName, InputStream x, int length) throws SQLException {
        String methodCall = "updateBinaryStream(" + columnName + ", " + x + ", " + length + ")";
        try {
            delegate.updateBinaryStream(columnName, x, length);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    public String getString(int columnIndex) throws SQLException {
        String methodCall = "getString(" + columnIndex + ")";
        try {
            return (String)reportReturn(methodCall, delegate.getString(columnIndex));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    public String getString(String columnName) throws SQLException {
        String methodCall = "getString(" + columnName + ")";
        try {
            return (String)reportReturn(methodCall, delegate.getString(columnName));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    public Reader getCharacterStream(int columnIndex) throws SQLException {
        String methodCall = "getCharacterStream(" + columnIndex + ")";
        try {
            return (Reader)reportReturn(methodCall, delegate.getCharacterStream(columnIndex));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    public Reader getCharacterStream(String columnName) throws SQLException {
        String methodCall = "getCharacterStream(" + columnName + ")";
        try {
            return (Reader)reportReturn(methodCall, delegate.getCharacterStream(columnName));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
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

    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        String methodCall = "updateCharacterStream(" + columnIndex + ", " + x + ", " + length + ")";
        try {
            delegate.updateCharacterStream(columnIndex, x, length);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    public void updateCharacterStream(String columnName, Reader reader, int length) throws SQLException {
        String methodCall = "updateCharacterStream(" + columnName + ", " + reader + ", " + length + ")";
        try {
            delegate.updateCharacterStream(columnName, reader, length);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    public byte getByte(int columnIndex) throws SQLException {
        String methodCall = "getByte(" + columnIndex + ")";
        try {
            return reportReturn(methodCall, delegate.getByte(columnIndex));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    public byte getByte(String columnName) throws SQLException {
        String methodCall = "getByte(" + columnName + ")";
        try {
            return reportReturn(methodCall, delegate.getByte(columnName));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    public void updateTime(int columnIndex, Time x) throws SQLException {
        String methodCall = "updateTime(" + columnIndex + ", " + x + ")";
        try {
            delegate.updateTime(columnIndex, x);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    public void updateTime(String columnName, Time x) throws SQLException {
        String methodCall = "updateTime(" + columnName + ", " + x + ")";
        try {
            delegate.updateTime(columnName, x);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    public byte[] getBytes(int columnIndex) throws SQLException {
        String methodCall = "getBytes(" + columnIndex + ")";
        try {
            return (byte[])reportReturn(methodCall, delegate.getBytes(columnIndex));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    public byte[] getBytes(String columnName) throws SQLException {
        String methodCall = "getBytes(" + columnName + ")";
        try {
            return (byte[])reportReturn(methodCall, delegate.getBytes(columnName));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    public boolean isAfterLast() throws SQLException {
        String methodCall = "isAfterLast()";
        try {
            return reportReturn(methodCall, delegate.isAfterLast());
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    public void updateObject(int columnIndex, Object x, int scale) throws SQLException {
        String methodCall = "updateObject(" + columnIndex + ", " + x + ", " + scale + ")";
        try {
            delegate.updateObject(columnIndex, x, scale);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    public void updateObject(int columnIndex, Object x) throws SQLException {
        String methodCall = "updateObject(" + columnIndex + ", " + x + ")";
        try {
            delegate.updateObject(columnIndex, x);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    public void updateObject(String columnName, Object x, int scale) throws SQLException {
        String methodCall = "updateObject(" + columnName + ", " + x + ", " + scale + ")";
        try {
            delegate.updateObject(columnName, x, scale);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    public void updateObject(String columnName, Object x) throws SQLException {
        String methodCall = "updateObject(" + columnName + ", " + x + ")";
        try {
            delegate.updateObject(columnName, x);
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

    public long getLong(int columnIndex) throws SQLException {
        String methodCall = "getLong(" + columnIndex + ")";
        try {
            return reportReturn(methodCall, delegate.getLong(columnIndex));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    public long getLong(String columnName) throws SQLException {
        String methodCall = "getLong(" + columnName + ")";
        try {
            return reportReturn(methodCall, delegate.getLong(columnName));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    public boolean isFirst() throws SQLException {
        String methodCall = "isFirst()";
        try {
            return reportReturn(methodCall, delegate.isFirst());
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    public void insertRow() throws SQLException {
        String methodCall = "insertRow()";
        try {
            delegate.insertRow();
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    public float getFloat(int columnIndex) throws SQLException {
        String methodCall = "getFloat(" + columnIndex + ")";
        try {
            return reportReturn(methodCall, delegate.getFloat(columnIndex));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    public float getFloat(String columnName) throws SQLException {
        String methodCall = "getFloat(" + columnName + ")";
        try {
            return reportReturn(methodCall, delegate.getFloat(columnName));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    public boolean isLast() throws SQLException {
        String methodCall = "isLast()";
        try {
            return reportReturn(methodCall, delegate.isLast());
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
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

    public void updateRow() throws SQLException {
        String methodCall = "updateRow()";
        try {
            delegate.updateRow();
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    public void beforeFirst() throws SQLException {
        String methodCall = "beforeFirst()";
        try {
            delegate.beforeFirst();
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    /**
     * @deprecated
     */
    @Deprecated
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        String methodCall = "getBigDecimal(" + columnIndex + ", " + scale + ")";
        try {
            return (BigDecimal)reportReturn(methodCall, delegate.getBigDecimal(columnIndex, scale));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    /**
     * @deprecated
     */
    @Deprecated
    public BigDecimal getBigDecimal(String columnName, int scale) throws SQLException {
        String methodCall = "getBigDecimal(" + columnName + ", " + scale + ")";
        try {
            return (BigDecimal)reportReturn(methodCall, delegate.getBigDecimal(columnName, scale));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        String methodCall = "getBigDecimal(" + columnIndex + ")";
        try {
            return (BigDecimal)reportReturn(methodCall, delegate.getBigDecimal(columnIndex));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    public BigDecimal getBigDecimal(String columnName) throws SQLException {
        String methodCall = "getBigDecimal(" + columnName + ")";
        try {
            return (BigDecimal)reportReturn(methodCall, delegate.getBigDecimal(columnName));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    public void afterLast() throws SQLException {
        String methodCall = "afterLast()";
        try {
            delegate.afterLast();
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    public void refreshRow() throws SQLException {
        String methodCall = "refreshRow()";
        try {
            delegate.refreshRow();
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
            return (T)reportReturn(methodCall, (iface != null && (iface == ResultSet.class || iface == JdbcSpy.class)) ? (T)this : delegate.unwrap(iface));
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
            return reportReturn(methodCall, (iface != null && (iface == ResultSet.class || iface == JdbcSpy.class)) || delegate.isWrapperFor(iface));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

}