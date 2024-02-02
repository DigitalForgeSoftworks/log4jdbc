package org.digitalforge.log4jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import org.digitalforge.log4jdbc.util.Utilities;

/**
 * Wraps a PreparedStatement and reports method calls, returns and exceptions.
 *
 * JDBC 4.2
 */
public class LoggingPreparedStatement<S extends PreparedStatement> extends LoggingStatement<S> implements PreparedStatement {

    private static final SpyLogDelegator log = SpyLogFactory.getSpyLogDelegator();

    /**
     * holds list of bind variables for tracing
     */
    protected final List<String> argTrace = new ArrayList<>();

    // a way to turn on and off type help...
    // todo:  make this a configurable parameter
    // todo, debug arrays and streams in a more useful manner.... if possible
    private static final boolean showTypeHelp = false;

    /**
     * Store an argument (bind variable) into the argTrace list (above) for later dumping.
     *
     * @param i          index of argument being set.
     * @param typeHelper optional additional info about the type that is being set in the arg
     * @param arg        argument being bound.
     */
    protected void argTraceSet(int i, String typeHelper, Object arg) {
        String tracedArg;
        try {
            tracedArg = parameterFormatter.formatParameterObject(arg);
        }
        catch(Throwable t) {
            // rdbmsSpecifics should NEVER EVER throw an exception!!
            // but just in case it does, we trap it.
            log.debug("rdbmsSpecifics threw an exception while trying to format a " + "parameter object [" + arg + "] this is very bad!!! (" + t.getMessage() + ")");

            // backup - so that at least we won't harm the application using us
            tracedArg = (arg == null) ? "null" : arg.toString();
        }

        i--;  // make the index 0 based
        synchronized(argTrace) {
            // if an object is being inserted out of sequence, fill up missing values with null...
            while(i >= argTrace.size()) {
                argTrace.add(argTrace.size(), null);
            }
            if(!showTypeHelp || typeHelper == null) {
                argTrace.set(i, tracedArg);
            }
            else {
                argTrace.set(i, typeHelper + tracedArg);
            }
        }
    }

    private String sql;

    public String getCurrentSql() {
        return sql;
    }

    protected String dumpedSql() {
        if(LoggingDriver.config.isReportOriginalSql()) {
            return sql;
        }

        StringBuffer dumpSql = new StringBuffer();
        int lastPos = 0;
        int qPos = sql.indexOf('?', lastPos);  // find position of first question mark
        int argIdx = 0;
        String arg;

        while(qPos != -1) {
            // get stored argument
            synchronized(argTrace) {
                try {
                    arg = argTrace.get(argIdx);
                }
                catch(IndexOutOfBoundsException e) {
                    arg = "?";
                }
            }
            if(arg == null) {
                arg = "?";
            }

            argIdx++;

            dumpSql.append(sql.substring(lastPos, qPos));  // dump segment of sql up to question mark.
            lastPos = qPos + 1;
            qPos = sql.indexOf('?', lastPos);
            dumpSql.append(arg);
        }
        if(lastPos < sql.length()) {
            dumpSql.append(sql.substring(lastPos, sql.length()));  // dump last segment
        }

        return dumpSql.toString();
    }

    protected void reportAllReturns(String methodCall, String msg) {
        log.methodReturned(this, methodCall, msg);
    }



    /**
     * Create a PreparedStatementSpy (JDBC 4 version) for logging activity of another PreparedStatement.
     *
     * @param sql                   SQL for the prepared statement that is being spied upon.
     * @param connection         ConnectionSpy that was called to produce this PreparedStatement.
     * @param delegate The actual PreparedStatement that is being spied upon.
     */
    public LoggingPreparedStatement(final String sql, final LoggingConnection connection, final S delegate) {

        super(connection, delegate); // does null check for us

        this.sql = sql;
        this.parameterFormatter = connection.getParameterFormatter();

    }

    @Override
    public String getClassType() {
        return "PreparedStatement";
    }

    // forwarding methods

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        String methodCall = "setTime(" + parameterIndex + ", " + x + ")";
        argTraceSet(parameterIndex, "/*<Time>*/", x);
        try {
            delegate.setTime(parameterIndex, x);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        String methodCall = "setTime(" + parameterIndex + ", " + x + ", " + cal + ")";
        argTraceSet(parameterIndex, "/*<Time>*/", x);
        try {
            delegate.setTime(parameterIndex, x, cal);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        String methodCall = "setCharacterStream(" + parameterIndex + ", " + reader + ", " + length + ")";
        argTraceSet(parameterIndex, "/*<Reader>*/", "<Reader of length " + length + ">");
        try {
            delegate.setCharacterStream(parameterIndex, reader, length);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        String methodCall = "setNull(" + parameterIndex + ", " + sqlType + ")";
        argTraceSet(parameterIndex, null, null);
        try {
            delegate.setNull(parameterIndex, sqlType);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public void setNull(int paramIndex, int sqlType, String typeName) throws SQLException {
        String methodCall = "setNull(" + paramIndex + ", " + sqlType + ", " + typeName + ")";
        argTraceSet(paramIndex, null, null);
        try {
            delegate.setNull(paramIndex, sqlType, typeName);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public void setRef(int i, Ref x) throws SQLException {
        String methodCall = "setRef(" + i + ", " + x + ")";
        argTraceSet(i, "/*<Ref>*/", x);
        try {
            delegate.setRef(i, x);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        String methodCall = "setBoolean(" + parameterIndex + ", " + x + ")";
        argTraceSet(parameterIndex, "/*<boolean>*/", x);
        try {
            delegate.setBoolean(parameterIndex, x);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public void setBlob(int i, Blob x) throws SQLException {
        String methodCall = "setBlob(" + i + ", " + x + ")";
        argTraceSet(i, "/*<Blob>*/", x == null ? null : ("<Blob of size " + x.length() + ">"));
        try {
            delegate.setBlob(i, x);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public void setClob(int i, Clob x) throws SQLException {
        String methodCall = "setClob(" + i + ", " + x + ")";
        argTraceSet(i, "/*<Clob>*/", x == null ? null : ("<Clob of size " + x.length() + ">"));
        try {
            delegate.setClob(i, x);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public void setArray(int i, Array x) throws SQLException {
        String methodCall = "setArray(" + i + ", " + x + ")";
        argTraceSet(i, "/*<Array>*/", "<Array>");
        try {
            delegate.setArray(i, x);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        String methodCall = "setByte(" + parameterIndex + ", " + x + ")";
        argTraceSet(parameterIndex, "/*<byte>*/", x);
        try {
            delegate.setByte(parameterIndex, x);
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
    @Override
    @Deprecated
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        String methodCall = "setUnicodeStream(" + parameterIndex + ", " + x + ", " + length + ")";
        argTraceSet(parameterIndex, "/*<Unicode InputStream>*/", "<Unicode InputStream of length " + length + ">");
        try {
            delegate.setUnicodeStream(parameterIndex, x, length);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        String methodCall = "setShort(" + parameterIndex + ", " + x + ")";
        argTraceSet(parameterIndex, "/*<short>*/", x);
        try {
            delegate.setShort(parameterIndex, x);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public boolean execute() throws SQLException {
        String methodCall = "execute()";
        String dumpedSql = dumpedSql();
        reportSql(dumpedSql, methodCall);
        long tstartNano = System.nanoTime();
        try {
            boolean result = delegate.execute();
            reportSqlTiming(System.nanoTime() - tstartNano, dumpedSql, methodCall);
            return reportReturn(methodCall, result);
        }
        catch(SQLException s) {
            reportException(methodCall, s, dumpedSql, System.nanoTime() - tstartNano);
            throw s;
        }
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        String methodCall = "setInt(" + parameterIndex + ", " + x + ")";
        argTraceSet(parameterIndex, "/*<int>*/", x);
        try {
            delegate.setInt(parameterIndex, x);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        String methodCall = "setLong(" + parameterIndex + ", " + x + ")";
        argTraceSet(parameterIndex, "/*<long>*/", x);
        try {
            delegate.setLong(parameterIndex, x);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        String methodCall = "setFloat(" + parameterIndex + ", " + x + ")";
        argTraceSet(parameterIndex, "/*<float>*/", x);
        try {
            delegate.setFloat(parameterIndex, x);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        String methodCall = "setDouble(" + parameterIndex + ", " + x + ")";
        argTraceSet(parameterIndex, "/*<double>*/", new Double(x));
        try {
            delegate.setDouble(parameterIndex, x);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        String methodCall = "setBigDecimal(" + parameterIndex + ", " + x + ")";
        argTraceSet(parameterIndex, "/*<BigDecimal>*/", x);
        try {
            delegate.setBigDecimal(parameterIndex, x);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        String methodCall = "setURL(" + parameterIndex + ", " + x + ")";
        argTraceSet(parameterIndex, "/*<URL>*/", x);

        try {
            delegate.setURL(parameterIndex, x);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        String methodCall = "setString(" + parameterIndex + ", \"" + x + "\")";
        argTraceSet(parameterIndex, "/*<String>*/", x);

        try {
            delegate.setString(parameterIndex, x);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        String methodCall = "setBytes(" + parameterIndex + ", " + x + ")";
        String argVal = (x.length <= 32) ? ("0x" + Utilities.hex(x)) : ("<byte[" + x.length + "]>");
        argTraceSet(parameterIndex, "/*<byte[]>*/", argVal);
        try {
            delegate.setBytes(parameterIndex, x);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        String methodCall = "setDate(" + parameterIndex + ", " + x + ")";
        argTraceSet(parameterIndex, "/*<Date>*/", x);
        try {
            delegate.setDate(parameterIndex, x);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        String methodCall = "getParameterMetaData()";
        try {
            return reportReturn(methodCall, delegate.getParameterMetaData());
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        String methodCall = "setRowId(" + parameterIndex + ", " + x + ")";
        argTraceSet(parameterIndex, "/*<RowId>*/", x);
        try {
            delegate.setRowId(parameterIndex, x);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        String methodCall = "setNString(" + parameterIndex + ", " + value + ")";
        argTraceSet(parameterIndex, "/*<String>*/", value);
        try {
            delegate.setNString(parameterIndex, value);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        String methodCall = "setNCharacterStream(" + parameterIndex + ", " + value + ", " + length + ")";
        argTraceSet(parameterIndex, "/*<Reader>*/", "<Reader of length " + length + ">");
        try {
            delegate.setNCharacterStream(parameterIndex, value, length);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        String methodCall = "setNClob(" + parameterIndex + ", " + value + ")";
        argTraceSet(parameterIndex, "/*<NClob>*/", "<NClob>");
        try {
            delegate.setNClob(parameterIndex, value);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        String methodCall = "setClob(" + parameterIndex + ", " + reader + ", " + length + ")";
        argTraceSet(parameterIndex, "/*<Reader>*/", "<Reader of length " + length + ">");
        try {
            delegate.setClob(parameterIndex, reader, length);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        String methodCall = "setBlob(" + parameterIndex + ", " + inputStream + ", " + length + ")";
        argTraceSet(parameterIndex, "/*<InputStream>*/", "<InputStream of length " + length + ">");
        try {
            delegate.setBlob(parameterIndex, inputStream, length);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        String methodCall = "setNClob(" + parameterIndex + ", " + reader + ", " + length + ")";
        argTraceSet(parameterIndex, "/*<Reader>*/", "<Reader of length " + length + ">");
        try {
            delegate.setNClob(parameterIndex, reader, length);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        String methodCall = "setSQLXML(" + parameterIndex + ", " + xmlObject + ")";
        argTraceSet(parameterIndex, "/*<SQLXML>*/", xmlObject);
        try {
            delegate.setSQLXML(parameterIndex, xmlObject);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        String methodCall = "setDate(" + parameterIndex + ", " + x + ", " + cal + ")";
        argTraceSet(parameterIndex, "/*<Date>*/", x);

        try {
            delegate.setDate(parameterIndex, x, cal);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        String methodCall = "executeQuery()";
        String dumpedSql = dumpedSql();
        reportSql(dumpedSql, methodCall);
        long tstartNano = System.nanoTime();

        try {
            ResultSet r = delegate.executeQuery();
            reportSqlTiming(System.nanoTime() - tstartNano, dumpedSql, methodCall);
            LoggingResultSet rsp = new LoggingResultSet(this, r);
            return reportReturn(methodCall, rsp);
        }
        catch(SQLException s) {
            reportException(methodCall, s, dumpedSql, System.nanoTime() - tstartNano);
            throw s;
        }
    }

    private String getTypeHelp(Object x) {
        if(x == null) {
            return "/*<null>*/";
        }
        else {
            return "(" + x.getClass().getName() + ")";
        }
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scale) throws SQLException {
        String methodCall = "setObject(" + parameterIndex + ", " + x + ", " + targetSqlType + ", " + scale + ")";
        argTraceSet(parameterIndex, getTypeHelp(x), x);

        try {
            delegate.setObject(parameterIndex, x, targetSqlType, scale);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    /**
     * Sets the designated parameter to the given input stream, which will have
     * the specified number of bytes.
     * When a very large ASCII value is input to a <code>LONGVARCHAR</code>
     * parameter, it may be more practical to send it via a
     * <code>java.io.InputStream</code>. Data will be read from the stream
     * as needed until end-of-file is reached.  The JDBC driver will
     * do any necessary conversion from ASCII to the database char format.
     * <p>
     * <b>Note:</b> This stream object can either be a standard
     * Java stream object or your own subclass that implements the
     * standard interface.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x              the Java input stream that contains the ASCII parameter value
     * @param length         the number of bytes in the stream
     * @throws java.sql.SQLException if parameterIndex does not correspond to a parameter
     *                               marker in the SQL statement; if a database access error occurs or
     *                               this method is called on a closed <code>PreparedStatement</code>
     * @since 1.6
     */
    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        String methodCall = "setAsciiStream(" + parameterIndex + ", " + x + ", " + length + ")";
        argTraceSet(parameterIndex, "/*<Ascii InputStream>*/", "<Ascii InputStream of length " + length + ">");
        try {
            delegate.setAsciiStream(parameterIndex, x, length);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        String methodCall = "setBinaryStream(" + parameterIndex + ", " + x + ", " + length + ")";
        argTraceSet(parameterIndex, "/*<Binary InputStream>*/", "<Binary InputStream of length " + length + ">");
        try {
            delegate.setBinaryStream(parameterIndex, x, length);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        String methodCall = "setCharacterStream(" + parameterIndex + ", " + reader + ", " + length + ")";
        argTraceSet(parameterIndex, "/*<Reader>*/", "<Reader of length " + length + ">");
        try {
            delegate.setCharacterStream(parameterIndex, reader, length);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);

    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        String methodCall = "setAsciiStream(" + parameterIndex + ", " + x + ")";
        argTraceSet(parameterIndex, "/*<Ascii InputStream>*/", "<Ascii InputStream>");
        try {
            delegate.setAsciiStream(parameterIndex, x);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        String methodCall = "setBinaryStream(" + parameterIndex + ", " + x + ")";
        argTraceSet(parameterIndex, "/*<Binary InputStream>*/", "<Binary InputStream>");
        try {
            delegate.setBinaryStream(parameterIndex, x);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);

    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        String methodCall = "setCharacterStream(" + parameterIndex + ", " + reader + ")";
        argTraceSet(parameterIndex, "/*<Reader>*/", "<Reader>");
        try {
            delegate.setCharacterStream(parameterIndex, reader);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        String methodCall = "setNCharacterStream(" + parameterIndex + ", " + reader + ")";
        argTraceSet(parameterIndex, "/*<Reader>*/", "<Reader>");
        try {
            delegate.setNCharacterStream(parameterIndex, reader);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        String methodCall = "setClob(" + parameterIndex + ", " + reader + ")";
        argTraceSet(parameterIndex, "/*<Reader>*/", "<Reader>");
        try {
            delegate.setClob(parameterIndex, reader);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        String methodCall = "setBlob(" + parameterIndex + ", " + inputStream + ")";
        argTraceSet(parameterIndex, "/*<InputStream>*/", "<InputStream>");
        try {
            delegate.setBlob(parameterIndex, inputStream);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        String methodCall = "setNClob(" + parameterIndex + ", " + reader + ")";
        argTraceSet(parameterIndex, "/*<Reader>*/", "<Reader>");
        try {
            delegate.setNClob(parameterIndex, reader);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);

    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        String methodCall = "setObject(" + parameterIndex + ", " + x + ", " + targetSqlType + ")";
        argTraceSet(parameterIndex, getTypeHelp(x), x);
        try {
            delegate.setObject(parameterIndex, x, targetSqlType);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        String methodCall = "setObject(" + parameterIndex + ", " + x + ")";
        argTraceSet(parameterIndex, getTypeHelp(x), x);
        try {
            delegate.setObject(parameterIndex, x);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        String methodCall = "setTimestamp(" + parameterIndex + ", " + x + ")";
        argTraceSet(parameterIndex, "/*<Date>*/", x);
        try {
            delegate.setTimestamp(parameterIndex, x);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        String methodCall = "setTimestamp(" + parameterIndex + ", " + x + ", " + cal + ")";
        argTraceSet(parameterIndex, "/*<Timestamp>*/", x);
        try {
            delegate.setTimestamp(parameterIndex, x, cal);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public int executeUpdate() throws SQLException {
        String methodCall = "executeUpdate()";
        String dumpedSql = dumpedSql();
        reportSql(dumpedSql, methodCall);
        long tstartNano = System.nanoTime();

        try {
            int result = delegate.executeUpdate();
            reportSqlTiming(System.nanoTime() - tstartNano, dumpedSql, methodCall);
            return reportReturn(methodCall, result);
        }
        catch(SQLException s) {
            reportException(methodCall, s, dumpedSql, System.nanoTime() - tstartNano);
            throw s;
        }
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        String methodCall = "setAsciiStream(" + parameterIndex + ", " + x + ", " + length + ")";
        argTraceSet(parameterIndex, "/*<Ascii InputStream>*/", "<Ascii InputStream of length " + length + ">");
        try {
            delegate.setAsciiStream(parameterIndex, x, length);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        String methodCall = "setBinaryStream(" + parameterIndex + ", " + x + ", " + length + ")";
        argTraceSet(parameterIndex, "/*<Binary InputStream>*/", "<Binary InputStream of length " + length + ">");
        try {
            delegate.setBinaryStream(parameterIndex, x, length);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public void clearParameters() throws SQLException {
        String methodCall = "clearParameters()";

        synchronized(argTrace) {
            argTrace.clear();
        }

        try {
            delegate.clearParameters();
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
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
    public void addBatch() throws SQLException {
        String methodCall = "addBatch()";
        currentBatch.add(dumpedSql());
        try {
            delegate.addBatch();
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        String methodCall = "unwrap(" + (iface == null ? "null" : iface.getName()) + ")";
        try {
            //todo: double check this logic
            //NOTE: could call super.isWrapperFor to simplify this logic, but it would result in extra log output
            //because the super classes would be invoked, thus executing their logging methods too...
            return reportReturn(methodCall, (iface != null && (iface == PreparedStatement.class || iface == Statement.class || iface == JdbcSpy.class)) ? (T)this : delegate
                .unwrap(iface));
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
            //NOTE: could call super.isWrapperFor to simplify this logic, but it would result in extra log output
            //when the super classes would be invoked..
            return reportReturn(methodCall, (iface != null && (iface == PreparedStatement.class || iface == Statement.class || iface == JdbcSpy.class)) || delegate
                .isWrapperFor(iface));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    @Override
    public void setObject(int parameterIndex, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
        String methodCall = "setObject(" + parameterIndex + ", " + x + ", " + targetSqlType + ", " + scaleOrLength + ")";
        argTraceSet(parameterIndex, targetSqlType.getName(), x);

        try {
            delegate.setObject(parameterIndex, x, targetSqlType, scaleOrLength);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public void setObject(int parameterIndex, Object x, SQLType targetSqlType) throws SQLException {
        String methodCall = "setObject(" + parameterIndex + ", " + x + ", " + targetSqlType + ")";
        argTraceSet(parameterIndex, targetSqlType.getName(), x);

        try {
            delegate.setObject(parameterIndex, x, targetSqlType);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public long executeLargeUpdate() throws SQLException {
        String methodCall = "executeLargeUpdate()";
        String dumpedSql = dumpedSql();
        reportSql(dumpedSql, methodCall);
        long tstartNano = System.nanoTime();

        try {
            long result = delegate.executeLargeUpdate();
            reportSqlTiming(System.nanoTime() - tstartNano, dumpedSql, methodCall);
            return reportReturn(methodCall, result);
        }
        catch(SQLException s) {
            reportException(methodCall, s, dumpedSql, System.nanoTime() - tstartNano);
            throw s;
        }
    }

}
