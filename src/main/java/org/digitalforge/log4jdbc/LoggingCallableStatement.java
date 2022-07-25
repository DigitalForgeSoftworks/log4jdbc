package org.digitalforge.log4jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;
import java.util.Map;

import org.digitalforge.log4jdbc.util.Utilities;

/**
 * Wraps a CallableStatement and reports method calls, returns and exceptions.
 */
public class LoggingCallableStatement<S extends CallableStatement> extends LoggingPreparedStatement<CallableStatement> implements CallableStatement {

    private static final SpyLogDelegator log = SpyLogFactory.getSpyLogDelegator();

    protected void reportAllReturns(String methodCall, String msg) {
        log.methodReturned(this, methodCall, msg);
    }

    /**
     * Create a LoggingCallableStatement (JDBC 4 version) to spy upon a CallableStatement.
     *
     * @param sql           The SQL used for this CallableStatement
     * @param connection    The ConnectionSpy which produced this LoggingCallableStatement
     * @param delegate      The real CallableStatement that is being spied upon
     */
    public LoggingCallableStatement(final String sql, final LoggingConnection connection, final S delegate) {
        super(sql, connection, delegate);
    }

    @Override
    public String getClassType() {
        return "CallableStatement";
    }

    // forwarding methods

    @Override
    public Date getDate(int parameterIndex) throws SQLException {
        String methodCall = "getDate(" + parameterIndex + ")";
        try {
            return (Date)reportReturn(methodCall, delegate.getDate(parameterIndex));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    @Override
    public Date getDate(int parameterIndex, Calendar cal) throws SQLException {
        String methodCall = "getDate(" + parameterIndex + ", " + cal + ")";
        try {
            return (Date)reportReturn(methodCall, delegate.getDate(parameterIndex, cal));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    @Override
    public Ref getRef(String parameterName) throws SQLException {
        String methodCall = "getRef(" + parameterName + ")";
        try {
            return (Ref)reportReturn(methodCall, delegate.getRef(parameterName));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    @Override
    public Time getTime(String parameterName) throws SQLException {
        String methodCall = "getTime(" + parameterName + ")";
        try {
            return (Time)reportReturn(methodCall, delegate.getTime(parameterName));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    @Override
    public void setTime(String parameterName, Time x) throws SQLException {
        String methodCall = "setTime(" + parameterName + ", " + x + ")";
        try {
            delegate.setTime(parameterName, x);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
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

    @Override
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

    @Override
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

    public byte[] getBytes(int parameterIndex) throws SQLException {
        String methodCall = "getBytes(" + parameterIndex + ")";
        try {
            return (byte[])reportReturn(methodCall, delegate.getBytes(parameterIndex));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    @Override
    public double getDouble(int parameterIndex) throws SQLException {
        String methodCall = "getDouble(" + parameterIndex + ")";
        try {
            return reportReturn(methodCall, delegate.getDouble(parameterIndex));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    @Override
    public int getInt(int parameterIndex) throws SQLException {
        String methodCall = "getInt(" + parameterIndex + ")";
        try {
            return reportReturn(methodCall, delegate.getInt(parameterIndex));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    @Override
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

    @Override
    public Time getTime(int parameterIndex) throws SQLException {
        String methodCall = "getTime(" + parameterIndex + ")";
        try {
            return (Time)reportReturn(methodCall, delegate.getTime(parameterIndex));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    @Override
    public Time getTime(int parameterIndex, Calendar cal) throws SQLException {
        String methodCall = "getTime(" + parameterIndex + ", " + cal + ")";
        try {
            return (Time)reportReturn(methodCall, delegate.getTime(parameterIndex, cal));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    @Override
    public Timestamp getTimestamp(String parameterName) throws SQLException {
        String methodCall = "getTimestamp(" + parameterName + ")";
        try {
            return (Timestamp)reportReturn(methodCall, delegate.getTimestamp(parameterName));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    @Override
    public void setTimestamp(String parameterName, Timestamp x) throws SQLException {
        String methodCall = "setTimestamp(" + parameterName + ", " + x + ")";
        try {
            delegate.setTimestamp(parameterName, x);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public String getString(int parameterIndex) throws SQLException {
        String methodCall = "getString(" + parameterIndex + ")";
        try {
            return (String)reportReturn(methodCall, delegate.getString(parameterIndex));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    @Override
    public void registerOutParameter(int parameterIndex, int sqlType) throws SQLException {
        String methodCall = "registerOutParameter(" + parameterIndex + ", " + sqlType + ")";
        argTraceSet(parameterIndex, null, "<OUT>");
        try {
            delegate.registerOutParameter(parameterIndex, sqlType);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public void registerOutParameter(int parameterIndex, int sqlType, int scale) throws SQLException {
        String methodCall = "registerOutParameter(" + parameterIndex + ", " + sqlType + ", " + scale + ")";
        argTraceSet(parameterIndex, null, "<OUT>");
        try {
            delegate.registerOutParameter(parameterIndex, sqlType, scale);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public void registerOutParameter(int paramIndex, int sqlType, String typeName) throws SQLException {
        String methodCall = "registerOutParameter(" + paramIndex + ", " + sqlType + ", " + typeName + ")";
        argTraceSet(paramIndex, null, "<OUT>");
        try {
            delegate.registerOutParameter(paramIndex, sqlType, typeName);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public byte getByte(String parameterName) throws SQLException {
        String methodCall = "getByte(" + parameterName + ")";
        try {
            return reportReturn(methodCall, delegate.getByte(parameterName));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    @Override
    public double getDouble(String parameterName) throws SQLException {
        String methodCall = "getDouble(" + parameterName + ")";
        try {
            return reportReturn(methodCall, delegate.getDouble(parameterName));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    @Override
    public float getFloat(String parameterName) throws SQLException {
        String methodCall = "getFloat(" + parameterName + ")";
        try {
            return reportReturn(methodCall, delegate.getFloat(parameterName));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    @Override
    public int getInt(String parameterName) throws SQLException {
        String methodCall = "getInt(" + parameterName + ")";
        try {
            return reportReturn(methodCall, delegate.getInt(parameterName));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    @Override
    public long getLong(String parameterName) throws SQLException {
        String methodCall = "getLong(" + parameterName + ")";
        try {
            return reportReturn(methodCall, delegate.getLong(parameterName));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    @Override
    public short getShort(String parameterName) throws SQLException {
        String methodCall = "getShort(" + parameterName + ")";
        try {
            return reportReturn(methodCall, delegate.getShort(parameterName));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    @Override
    public boolean getBoolean(String parameterName) throws SQLException {
        String methodCall = "getBoolean(" + parameterName + ")";
        try {
            return reportReturn(methodCall, delegate.getBoolean(parameterName));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    @Override
    public byte[] getBytes(String parameterName) throws SQLException {
        String methodCall = "getBytes(" + parameterName + ")";
        try {
            return (byte[])reportReturn(methodCall, delegate.getBytes(parameterName));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    @Override
    public void setByte(String parameterName, byte x) throws SQLException {
        String methodCall = "setByte(" + parameterName + ", " + x + ")";
        try {
            delegate.setByte(parameterName, x);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public void setDouble(String parameterName, double x) throws SQLException {
        String methodCall = "setDouble(" + parameterName + ", " + x + ")";
        try {
            delegate.setDouble(parameterName, x);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public void setFloat(String parameterName, float x) throws SQLException {
        String methodCall = "setFloat(" + parameterName + ", " + x + ")";
        try {
            delegate.setFloat(parameterName, x);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public void registerOutParameter(String parameterName, int sqlType) throws SQLException {
        String methodCall = "registerOutParameter(" + parameterName + ", " + sqlType + ")";
        try {
            delegate.registerOutParameter(parameterName, sqlType);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public void setInt(String parameterName, int x) throws SQLException {
        String methodCall = "setInt(" + parameterName + ", " + x + ")";
        try {
            delegate.setInt(parameterName, x);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public void setNull(String parameterName, int sqlType) throws SQLException {
        String methodCall = "setNull(" + parameterName + ", " + sqlType + ")";
        try {
            delegate.setNull(parameterName, sqlType);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public void registerOutParameter(String parameterName, int sqlType, int scale) throws SQLException {
        String methodCall = "registerOutParameter(" + parameterName + ", " + sqlType + ", " + scale + ")";
        try {
            delegate.registerOutParameter(parameterName, sqlType, scale);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public void setLong(String parameterName, long x) throws SQLException {
        String methodCall = "setLong(" + parameterName + ", " + x + ")";
        try {
            delegate.setLong(parameterName, x);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public void setShort(String parameterName, short x) throws SQLException {
        String methodCall = "setShort(" + parameterName + ", " + x + ")";
        try {
            delegate.setShort(parameterName, x);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public void setBoolean(String parameterName, boolean x) throws SQLException {
        String methodCall = "setBoolean(" + parameterName + ", " + x + ")";
        try {
            delegate.setBoolean(parameterName, x);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public void setBytes(String parameterName, byte[] x) throws SQLException {
        String argVal = (x.length <= 32) ? ("0x" + Utilities.hex(x)) : ("<byte[" + x.length + "]>");
        String methodCall = "setBytes(" + parameterName + ", " + argVal + ")";
        try {
            delegate.setBytes(parameterName, x);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public boolean getBoolean(int parameterIndex) throws SQLException {
        String methodCall = "getBoolean(" + parameterIndex + ")";
        try {
            return reportReturn(methodCall, delegate.getBoolean(parameterIndex));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    @Override
    public Timestamp getTimestamp(int parameterIndex) throws SQLException {
        String methodCall = "getTimestamp(" + parameterIndex + ")";
        try {
            return (Timestamp)reportReturn(methodCall, delegate.getTimestamp(parameterIndex));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    @Override
    public void setAsciiStream(String parameterName, InputStream x, int length) throws SQLException {
        String methodCall = "setAsciiStream(" + parameterName + ", " + x + ", " + length + ")";
        try {
            delegate.setAsciiStream(parameterName, x, length);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public void setBinaryStream(String parameterName, InputStream x, int length) throws SQLException {
        String methodCall = "setBinaryStream(" + parameterName + ", " + x + ", " + length + ")";
        try {
            delegate.setBinaryStream(parameterName, x, length);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public void setCharacterStream(String parameterName, Reader reader, int length) throws SQLException {
        String methodCall = "setCharacterStream(" + parameterName + ", " + reader + ", " + length + ")";
        try {
            delegate.setCharacterStream(parameterName, reader, length);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public Object getObject(String parameterName) throws SQLException {
        String methodCall = "getObject(" + parameterName + ")";
        try {
            return reportReturn(methodCall, delegate.getObject(parameterName));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    @Override
    public void setObject(String parameterName, Object x) throws SQLException {
        String methodCall = "setObject(" + parameterName + ", " + x + ")";
        try {
            delegate.setObject(parameterName, x);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public void setObject(String parameterName, Object x, int targetSqlType) throws SQLException {
        String methodCall = "setObject(" + parameterName + ", " + x + ", " + targetSqlType + ")";
        try {
            delegate.setObject(parameterName, x, targetSqlType);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public void setObject(String parameterName, Object x, int targetSqlType, int scale) throws SQLException {
        String methodCall = "setObject(" + parameterName + ", " + x + ", " + targetSqlType + ", " + scale + ")";
        try {
            delegate.setObject(parameterName, x, targetSqlType, scale);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public Timestamp getTimestamp(int parameterIndex, Calendar cal) throws SQLException {
        String methodCall = "getTimestamp(" + parameterIndex + ", " + cal + ")";
        try {
            return (Timestamp)reportReturn(methodCall, delegate.getTimestamp(parameterIndex, cal));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    @Override
    public Date getDate(String parameterName, Calendar cal) throws SQLException {
        String methodCall = "getDate(" + parameterName + ", " + cal + ")";
        try {
            return (Date)reportReturn(methodCall, delegate.getDate(parameterName, cal));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    @Override
    public Time getTime(String parameterName, Calendar cal) throws SQLException {
        String methodCall = "getTime(" + parameterName + ", " + cal + ")";
        try {
            return (Time)reportReturn(methodCall, delegate.getTime(parameterName, cal));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    @Override
    public Timestamp getTimestamp(String parameterName, Calendar cal) throws SQLException {
        String methodCall = "getTimestamp(" + parameterName + ", " + cal + ")";
        try {
            return (Timestamp)reportReturn(methodCall, delegate.getTimestamp(parameterName, cal));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    @Override
    public void setDate(String parameterName, Date x, Calendar cal) throws SQLException {
        String methodCall = "setDate(" + parameterName + ", " + x + ", " + cal + ")";
        try {
            delegate.setDate(parameterName, x, cal);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public void setTime(String parameterName, Time x, Calendar cal) throws SQLException {
        String methodCall = "setTime(" + parameterName + ", " + x + ", " + cal + ")";
        try {
            delegate.setTime(parameterName, x, cal);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public void setTimestamp(String parameterName, Timestamp x, Calendar cal) throws SQLException {
        String methodCall = "setTimestamp(" + parameterName + ", " + x + ", " + cal + ")";
        try {
            delegate.setTimestamp(parameterName, x, cal);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public short getShort(int parameterIndex) throws SQLException {
        String methodCall = "getShort(" + parameterIndex + ")";
        try {
            return reportReturn(methodCall, delegate.getShort(parameterIndex));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    @Override
    public long getLong(int parameterIndex) throws SQLException {
        String methodCall = "getLong(" + parameterIndex + ")";
        try {
            return reportReturn(methodCall, delegate.getLong(parameterIndex));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    @Override
    public float getFloat(int parameterIndex) throws SQLException {
        String methodCall = "getFloat(" + parameterIndex + ")";
        try {
            return reportReturn(methodCall, delegate.getFloat(parameterIndex));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    @Override
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

    /**
     * @deprecated
     */
    @Override
    @Deprecated
    public BigDecimal getBigDecimal(int parameterIndex, int scale) throws SQLException {
        String methodCall = "getBigDecimal(" + parameterIndex + ", " + scale + ")";
        try {
            return (BigDecimal)reportReturn(methodCall, delegate.getBigDecimal(parameterIndex, scale));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    @Override
    public URL getURL(int parameterIndex) throws SQLException {
        String methodCall = "getURL(" + parameterIndex + ")";
        try {
            return (URL)reportReturn(methodCall, delegate.getURL(parameterIndex));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }

    }

    @Override
    public BigDecimal getBigDecimal(int parameterIndex) throws SQLException {
        String methodCall = "getBigDecimal(" + parameterIndex + ")";
        try {
            return (BigDecimal)reportReturn(methodCall, delegate.getBigDecimal(parameterIndex));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    @Override
    public byte getByte(int parameterIndex) throws SQLException {
        String methodCall = "getByte(" + parameterIndex + ")";
        try {
            return reportReturn(methodCall, delegate.getByte(parameterIndex));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    @Override
    public Object getObject(int parameterIndex) throws SQLException {
        String methodCall = "getObject(" + parameterIndex + ")";
        try {
            return reportReturn(methodCall, delegate.getObject(parameterIndex));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    @Override
    public Object getObject(int i, Map map) throws SQLException {
        String methodCall = "getObject(" + i + ", " + map + ")";
        try {
            return reportReturn(methodCall, delegate.getObject(i, map));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    @Override
    public String getString(String parameterName) throws SQLException {
        String methodCall = "getString(" + parameterName + ")";
        try {
            return (String)reportReturn(methodCall, delegate.getString(parameterName));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    @Override
    public void registerOutParameter(String parameterName, int sqlType, String typeName) throws SQLException {
        String methodCall = "registerOutParameter(" + parameterName + ", " + sqlType + ", " + typeName + ")";
        try {
            delegate.registerOutParameter(parameterName, sqlType, typeName);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public void setNull(String parameterName, int sqlType, String typeName) throws SQLException {
        String methodCall = "setNull(" + parameterName + ", " + sqlType + ", " + typeName + ")";
        try {
            delegate.setNull(parameterName, sqlType, typeName);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public void setString(String parameterName, String x) throws SQLException {
        String methodCall = "setString(" + parameterName + ", " + x + ")";

        try {
            delegate.setString(parameterName, x);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public BigDecimal getBigDecimal(String parameterName) throws SQLException {
        String methodCall = "getBigDecimal(" + parameterName + ")";
        try {
            return (BigDecimal)reportReturn(methodCall, delegate.getBigDecimal(parameterName));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    @Override
    public Object getObject(String parameterName, Map<String,Class<?>> map) throws SQLException {
        String methodCall = "getObject(" + parameterName + ", " + map + ")";
        try {
            return reportReturn(methodCall, delegate.getObject(parameterName, map));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    @Override
    public void setBigDecimal(String parameterName, BigDecimal x) throws SQLException {
        String methodCall = "setBigDecimal(" + parameterName + ", " + x + ")";
        try {
            delegate.setBigDecimal(parameterName, x);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public URL getURL(String parameterName) throws SQLException {
        String methodCall = "getURL(" + parameterName + ")";
        try {
            return (URL)reportReturn(methodCall, delegate.getURL(parameterName));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    @Override
    public RowId getRowId(int parameterIndex) throws SQLException {
        String methodCall = "getRowId(" + parameterIndex + ")";
        try {
            return (RowId)reportReturn(methodCall, delegate.getRowId(parameterIndex));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    @Override
    public RowId getRowId(String parameterName) throws SQLException {
        String methodCall = "getRowId(" + parameterName + ")";
        try {
            return (RowId)reportReturn(methodCall, delegate.getRowId(parameterName));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    @Override
    public void setRowId(String parameterName, RowId x) throws SQLException {
        String methodCall = "setRowId(" + parameterName + ", " + x + ")";
        try {
            delegate.setRowId(parameterName, x);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public void setNString(String parameterName, String value) throws SQLException {
        String methodCall = "setNString(" + parameterName + ", " + value + ")";
        try {
            delegate.setNString(parameterName, value);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public void setNCharacterStream(String parameterName, Reader reader, long length) throws SQLException {
        String methodCall = "setNCharacterStream(" + parameterName + ", " + reader + ", " + length + ")";
        try {
            delegate.setNCharacterStream(parameterName, reader, length);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public void setNClob(String parameterName, NClob value) throws SQLException {
        String methodCall = "setNClob(" + parameterName + ", " + value + ")";
        try {
            delegate.setNClob(parameterName, value);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public void setClob(String parameterName, Reader reader, long length) throws SQLException {
        String methodCall = "setClob(" + parameterName + ", " + reader + ", " + length + ")";
        try {
            delegate.setClob(parameterName, reader, length);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public void setBlob(String parameterName, InputStream inputStream, long length) throws SQLException {
        String methodCall = "setBlob(" + parameterName + ", " + inputStream + ", " + length + ")";
        try {
            delegate.setBlob(parameterName, inputStream, length);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public void setNClob(String parameterName, Reader reader, long length) throws SQLException {
        String methodCall = "setNClob(" + parameterName + ", " + reader + ", " + length + ")";
        try {
            delegate.setNClob(parameterName, reader, length);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public NClob getNClob(int parameterIndex) throws SQLException {
        String methodCall = "getNClob(" + parameterIndex + ")";
        try {
            return (NClob)reportReturn(methodCall, delegate.getNClob(parameterIndex));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    @Override
    public NClob getNClob(String parameterName) throws SQLException {
        String methodCall = "getNClob(" + parameterName + ")";
        try {
            return (NClob)reportReturn(methodCall, delegate.getNClob(parameterName));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    @Override
    public void setSQLXML(String parameterName, SQLXML xmlObject) throws SQLException {
        String methodCall = "setSQLXML(" + parameterName + ", " + xmlObject + ")";
        try {
            delegate.setSQLXML(parameterName, xmlObject);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public SQLXML getSQLXML(int parameterIndex) throws SQLException {
        String methodCall = "getSQLXML(" + parameterIndex + ")";
        try {
            return (SQLXML)reportReturn(methodCall, delegate.getSQLXML(parameterIndex));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    @Override
    public SQLXML getSQLXML(String parameterName) throws SQLException {
        String methodCall = "getSQLXML(" + parameterName + ")";
        try {
            return (SQLXML)reportReturn(methodCall, delegate.getSQLXML(parameterName));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }

    }

    @Override
    public String getNString(int parameterIndex) throws SQLException {
        String methodCall = "getNString(" + parameterIndex + ")";
        try {
            return (String)reportReturn(methodCall, delegate.getNString(parameterIndex));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    @Override
    public String getNString(String parameterName) throws SQLException {
        String methodCall = "getNString(" + parameterName + ")";
        try {
            return (String)reportReturn(methodCall, delegate.getNString(parameterName));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    @Override
    public Reader getNCharacterStream(int parameterIndex) throws SQLException {
        String methodCall = "getNCharacterStream(" + parameterIndex + ")";
        try {
            return (Reader)reportReturn(methodCall, delegate.getNCharacterStream(parameterIndex));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    @Override
    public Reader getNCharacterStream(String parameterName) throws SQLException {
        String methodCall = "getNCharacterStream(" + parameterName + ")";
        try {
            return (Reader)reportReturn(methodCall, delegate.getNCharacterStream(parameterName));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    @Override
    public Reader getCharacterStream(int parameterIndex) throws SQLException {
        String methodCall = "getCharacterStream(" + parameterIndex + ")";
        try {
            return (Reader)reportReturn(methodCall, delegate.getCharacterStream(parameterIndex));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    @Override
    public Reader getCharacterStream(String parameterName) throws SQLException {
        String methodCall = "getCharacterStream(" + parameterName + ")";
        try {
            return (Reader)reportReturn(methodCall, delegate.getCharacterStream(parameterName));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    @Override
    public void setBlob(String parameterName, Blob x) throws SQLException {
        String methodCall = "setBlob(" + parameterName + ", " + x + ")";
        try {
            delegate.setBlob(parameterName, x);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public void setClob(String parameterName, Clob x) throws SQLException {
        String methodCall = "setClob(" + parameterName + ", " + x + ")";
        try {
            delegate.setClob(parameterName, x);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public void setAsciiStream(String parameterName, InputStream x, long length) throws SQLException {
        String methodCall = "setAsciiStream(" + parameterName + ", " + x + ", " + length + ")";
        try {
            delegate.setAsciiStream(parameterName, x, length);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public void setBinaryStream(String parameterName, InputStream x, long length) throws SQLException {
        String methodCall = "setBinaryStream(" + parameterName + ", " + x + ", " + length + ")";
        try {
            delegate.setBinaryStream(parameterName, x, length);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public void setCharacterStream(String parameterName, Reader reader, long length) throws SQLException {
        String methodCall = "setCharacterStream(" + parameterName + ", " + reader + ", " + length + ")";
        try {
            delegate.setCharacterStream(parameterName, reader, length);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public void setAsciiStream(String parameterName, InputStream x) throws SQLException {
        String methodCall = "setAsciiStream(" + parameterName + ", " + x + ")";
        try {
            delegate.setAsciiStream(parameterName, x);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public void setBinaryStream(String parameterName, InputStream x) throws SQLException {
        String methodCall = "setBinaryStream(" + parameterName + ", " + x + ")";
        try {
            delegate.setBinaryStream(parameterName, x);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public void setCharacterStream(String parameterName, Reader reader) throws SQLException {
        String methodCall = "setCharacterStream(" + parameterName + ", " + reader + ")";
        try {
            delegate.setCharacterStream(parameterName, reader);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public void setNCharacterStream(String parameterName, Reader reader) throws SQLException {
        String methodCall = "setNCharacterStream(" + parameterName + ", " + reader + ")";
        try {
            delegate.setNCharacterStream(parameterName, reader);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public void setClob(String parameterName, Reader reader) throws SQLException {
        String methodCall = "setClob(" + parameterName + ", " + reader + ")";
        try {
            delegate.setClob(parameterName, reader);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public void setBlob(String parameterName, InputStream inputStream) throws SQLException {
        String methodCall = "setBlob(" + parameterName + ", " + inputStream + ")";
        try {
            delegate.setBlob(parameterName, inputStream);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public void setNClob(String parameterName, Reader reader) throws SQLException {
        String methodCall = "setNClob(" + parameterName + ", " + reader + ")";
        try {
            delegate.setNClob(parameterName, reader);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public <T> T getObject(int parameterIndex, Class<T> type) throws SQLException {
        String methodCall = "getObject(" + parameterIndex + ", " + type.getName() + ")";
        try {
            return (T)reportReturn(methodCall, delegate.getObject(parameterIndex, type));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    @Override
    public <T> T getObject(String parameterName, Class<T> type) throws SQLException {
        String methodCall = "getObject(" + parameterName + ", " + type.getName() + ")";
        try {
            return (T)reportReturn(methodCall, delegate.getObject(parameterName, type));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    @Override
    public void setObject(String parameterName, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
        String methodCall = "setObject(" + parameterName + ", " + x + ", " + targetSqlType + ", " + scaleOrLength + ")";
        try {
            delegate.setObject(parameterName, x, targetSqlType, scaleOrLength);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public void setObject(String parameterName, Object x, SQLType targetSqlType) throws SQLException {
        String methodCall = "setObject(" + parameterName + ", " + x + ", " + targetSqlType + ")";
        try {
            delegate.setObject(parameterName, x, targetSqlType);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public void registerOutParameter(int parameterIndex, SQLType sqlType) throws SQLException {
        String methodCall = "registerOutParameter(" + parameterIndex + ", " + sqlType.getName() + ")";
        try {
            delegate.registerOutParameter(parameterIndex, sqlType);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public void registerOutParameter(int parameterIndex, SQLType sqlType, int scale) throws SQLException {
        String methodCall = "registerOutParameter(" + parameterIndex + ", " + sqlType.getName() + ", " + scale + ")";
        try {
            delegate.registerOutParameter(parameterIndex, sqlType, scale);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public void registerOutParameter(int parameterIndex, SQLType sqlType, String typeName) throws SQLException {
        String methodCall = "registerOutParameter(" + parameterIndex + ", " + sqlType.getName() + ", " + typeName + ")";
        try {
            delegate.registerOutParameter(parameterIndex, sqlType, typeName);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public void registerOutParameter(String parameterName, SQLType sqlType) throws SQLException {
        String methodCall = "registerOutParameter(" + parameterName + ", " + sqlType + ")";
        try {
            delegate.registerOutParameter(parameterName, sqlType);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public void registerOutParameter(String parameterName, SQLType sqlType, int scale) throws SQLException {
        String methodCall = "registerOutParameter(" + parameterName + ", " + sqlType.getName() + ", " + scale + ")";
        try {
            delegate.registerOutParameter(parameterName, sqlType, scale);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public void registerOutParameter(String parameterName, SQLType sqlType, String typeName) throws SQLException {
        String methodCall = "registerOutParameter(" + parameterName + ", " + sqlType.getName() + ", " + typeName + ")";
        try {
            delegate.registerOutParameter(parameterName, sqlType, typeName);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public void setURL(String parameterName, URL val) throws SQLException {
        String methodCall = "setURL(" + parameterName + ", " + val + ")";
        try {
            delegate.setURL(parameterName, val);
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
        reportReturn(methodCall);
    }

    @Override
    public Array getArray(String parameterName) throws SQLException {
        String methodCall = "getArray(" + parameterName + ")";
        try {
            return (Array)reportReturn(methodCall, delegate.getArray(parameterName));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    @Override
    public Blob getBlob(String parameterName) throws SQLException {
        String methodCall = "getBlob(" + parameterName + ")";
        try {
            return (Blob)reportReturn(methodCall, delegate.getBlob(parameterName));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    @Override
    public Clob getClob(String parameterName) throws SQLException {
        String methodCall = "getClob(" + parameterName + ")";
        try {
            return (Clob)reportReturn(methodCall, delegate.getClob(parameterName));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    @Override
    public Date getDate(String parameterName) throws SQLException {
        String methodCall = "getDate(" + parameterName + ")";
        try {
            return (Date)reportReturn(methodCall, delegate.getDate(parameterName));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

    @Override
    public void setDate(String parameterName, Date x) throws SQLException {
        String methodCall = "setDate(" + parameterName + ", " + x + ")";
        try {
            delegate.setDate(parameterName, x);
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
            return (T)reportReturn(methodCall, (iface != null && (iface == CallableStatement.class || iface == PreparedStatement.class || iface == Statement.class || iface == JdbcSpy.class)) ? (T)this : delegate.unwrap(iface));
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
            return reportReturn(methodCall, (iface != null && (iface == CallableStatement.class || iface == PreparedStatement.class || iface == Statement.class || iface == JdbcSpy.class)) || delegate.isWrapperFor(iface));
        }
        catch(SQLException s) {
            reportException(methodCall, s);
            throw s;
        }
    }

}
