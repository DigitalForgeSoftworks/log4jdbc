package org.digitalforge.log4jdbc;

import java.sql.*;
import java.util.*;
import java.util.logging.Logger;

import org.digitalforge.log4jdbc.formatter.MySqlParameterFormatter;
import org.digitalforge.log4jdbc.formatter.OracleParameterFormatter;
import org.digitalforge.log4jdbc.formatter.ParameterFormatter;
import org.digitalforge.log4jdbc.formatter.SqlQueryPrettifier;
import org.digitalforge.log4jdbc.formatter.SqlServerParameterFormatter;

/**
 * A JDBC driver which is a facade that delegates to one or more real underlying
 * JDBC drivers. The driver will spy on any other JDBC driver that is loaded,
 * simply by prepending <code>jdbc:log4</code> to the normal jdbc driver URL
 * used by any other JDBC driver. The driver, by default, also loads several
 * well known drivers at class load time, so that this driver can be
 * "dropped in" to any Java program that uses these drivers without making any
 * code changes.
 * <p>
 * <p>
 * The well known driver classes that are loaded are:
 * <p>
 * <ul>
 * <li><code>com.mysql.jdbc.Driver</code></li>
 * <li><code>com.mysql.cj.jdbc.Driver</code></li>
 * <li><code>org.mariadb.jdbc.Driver</code></li>
 * <li><code>org.postgresql.Driver</code></li>
 * <li><code>org.hsqldb.jdbcDriver</code></li>
 * <li><code>org.h2.Driver</code></li>
 * </ul>
 * <p>
 * <p>
 * Additional drivers can be set via a property: <b>log4jdbc.drivers</b> This
 * can be either a single driver class name or a list of comma separated driver
 * class names.
 * <p>
 * The autoloading behavior can be disabled by setting a property:
 * <b>log4jdbc.auto.load.popular.drivers</b> to false. If that is done, then the
 * only drivers that log4jdbc will attempt to load are the ones specified in
 * <b>log4jdbc.drivers</b>.
 * <p>
 * If any of the above driver classes cannot be loaded, the driver continues on
 * without failing.
 * <p>
 * Note that the <code>getMajorVersion</code>, <code>getMinorVersion</code> and
 * <code>jdbcCompliant</code> method calls attempt to delegate to the last
 * underlying driver requested through any other call that accepts a JDBC URL.
 * <p>
 * This can cause unexpected behavior in certain circumstances. For example, if
 * one of these 3 methods is called before any underlying driver has been
 * established, then they will return default values that might not be correct
 * in all situations. Similarly, if this spy driver is used to spy on more than
 * one underlying driver concurrently, the values returned by these 3 method
 * calls may change depending on what the last underlying driver used was at the
 * time. This will not usually be a problem, since the driver is retrieved by
 * it's URL from the DriverManager in the first place (thus establishing an
 * underlying real driver), and in most applications their is only one database.
 */
public class LoggingDriver implements Driver {

    /**
     * The last actual, underlying driver that was requested via a URL.
     */
    private Driver lastUnderlyingDriverRequested;

    private static final SpyLogDelegator log = SpyLogFactory.getSpyLogDelegator();

    public static SqlQueryPrettifier sqlPrettifier;

    static LoggingDriverConfig config;

    static {

        log.debug("... log4jdbc initializing ...");

        config = new LoggingDriverConfig();

        sqlPrettifier = new SqlQueryPrettifier() {
            @Override
            public String prettifySql(String sql) {
                return sql;
            }
        };

        // The Set of drivers that the log4jdbc driver will preload at instantiation
        // time. The driver can spy on any driver type, it's just a little bit
        // easier to configure log4jdbc if it's one of these types!

        Set<String> subDrivers = new TreeSet<>();

        if(config.isAutoLoadPopularDrivers()) {
            subDrivers.add("com.mysql.cj.jdbc.Driver");
            subDrivers.add("org.mariadb.jdbc.Driver");
            subDrivers.add("org.postgresql.Driver");
            subDrivers.add("org.hsqldb.jdbcDriver");
            subDrivers.add("org.h2.Driver");
        }

        // look for additional driver specified in properties
        for(String driver : config.getDrivers()) {
            subDrivers.add(driver);
            log.debug("    will look for specific driver " + driver);
        }

        try {
            DriverManager.registerDriver(new LoggingDriver());
        }
        catch(SQLException ex) {
            // this exception should never be thrown
            throw new RuntimeException("Could not register log4jdbc driver!", ex);
        }

        // instantiate all the supported drivers and remove
        // those not found
        for(Iterator<String> itr = subDrivers.iterator(); itr.hasNext(); ) {
            String driverClass = itr.next();
            try {
                Class.forName(driverClass);
                log.debug("FOUND DRIVER " + driverClass);
            }
            catch(Throwable c) {
                itr.remove();
            }
        }

        if(subDrivers.isEmpty()) {
            log.debug("WARNING!  log4jdbc couldn't find any underlying jdbc drivers.");
        }

        SqlServerParameterFormatter sqlServer = new SqlServerParameterFormatter();
        OracleParameterFormatter oracle = new OracleParameterFormatter();
        MySqlParameterFormatter mySql = new MySqlParameterFormatter();

        /** create lookup Map for specific rdbms formatters */
        Map<String,ParameterFormatter> parameterFormatters = config.getParameterFormatters();
        parameterFormatters.put("oracle.jdbc.driver.OracleDriver", oracle);
        parameterFormatters.put("oracle.jdbc.OracleDriver", oracle);
        parameterFormatters.put("net.sourceforge.jtds.jdbc.Driver", sqlServer);
        parameterFormatters.put("com.microsoft.jdbc.sqlserver.SQLServerDriver", sqlServer);
        parameterFormatters.put("weblogic.jdbc.sqlserver.SQLServerDriver", sqlServer);
        parameterFormatters.put("com.mysql.jdbc.Driver", mySql);
        parameterFormatters.put("com.mysql.cj.jdbc.Driver", mySql);
        parameterFormatters.put("org.mariadb.jdbc.Driver", mySql);

        log.debug("... log4jdbc initialized! ...");

    }

    static ParameterFormatter defaultParameterFormatter = new ParameterFormatter();

    /**
     * Get the ParameterFormatter object for a given Connection.
     *
     * @param con JDBC connection to get ParameterFormatter for.
     * @return ParameterFormatter for the given connection.
     */
    static ParameterFormatter getRdbmsSpecifics(Connection con) {

        String driverName = null;
        try {
            driverName = con.getMetaData().getDriverName();
        }
        catch(SQLException ex) {
            // silently fail
        }

        log.debug("Driver name is " + driverName);

        ParameterFormatter formatter = config.getParameterFormatter(driverName);

        return formatter;

    }

    /**
     * Default constructor.
     */
    public LoggingDriver() {
    }

    /**
     * Get the major version of the driver. This call will be delegated to the
     * underlying driver that is being spied upon (if there is no underlying
     * driver found, then 1 will be returned.)
     *
     * @return the major version of the JDBC driver.
     */
    public int getMajorVersion() {
        return (lastUnderlyingDriverRequested != null) ? lastUnderlyingDriverRequested.getMajorVersion() : 1;
    }

    /**
     * Get the minor version of the driver. This call will be delegated to the
     * underlying driver that is being spied upon (if there is no underlying
     * driver found, then 0 will be returned.)
     *
     * @return the minor version of the JDBC driver.
     */
    public int getMinorVersion() {
        return (lastUnderlyingDriverRequested != null) ? lastUnderlyingDriverRequested.getMinorVersion() : 0;
    }

    /**
     * Report whether the underlying driver is JDBC compliant. If there is no
     * underlying driver, false will be returned, because the driver cannot
     * actually do any work without an underlying driver.
     *
     * @return <code>true</code> if the underlying driver is JDBC Compliant;
     *         <code>false</code> otherwise.
     */
    public boolean jdbcCompliant() {
        return lastUnderlyingDriverRequested != null && lastUnderlyingDriverRequested.jdbcCompliant();
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return (lastUnderlyingDriverRequested != null) ? lastUnderlyingDriverRequested.getParentLogger() : null;
    }

    /**
     * Returns true if this is a <code>jdbc:log4</code> URL and if the URL is for
     * an underlying driver that this DriverSpy can spy on.
     *
     * @param url JDBC URL.
     *
     * @return true if this Driver can handle the URL.
     *
     * @throws SQLException if a database access error occurs
     */
    public boolean acceptsURL(String url) throws SQLException {

        Driver driver = getUnderlyingDriver(url);

        if(driver == null) {
            return false;
        }

        lastUnderlyingDriverRequested = driver;

        return true;

    }

    /**
     * Given a <code>jdbc:log4</code> type URL, find the underlying real driver
     * that accepts the URL.
     *
     * @param url JDBC connection URL.
     *
     * @return Underlying driver for the given URL. Null is returned if the URL is
     *         not a <code>jdbc:log4</code> type URL or there is no underlying
     *         driver that accepts the URL.
     *
     * @throws SQLException if a database access error occurs.
     */
    private Driver getUnderlyingDriver(String url) throws SQLException {

        Enumeration<Driver> e = DriverManager.getDrivers();

        while(e.hasMoreElements()) {

            Driver driver = e.nextElement();

            if(driver.getClass() == LoggingDriver.class) {
                continue;
            }

            if(driver.acceptsURL(url)) {
                return driver;
            }

        }

        return null;

    }

    /**
     * Get a Connection to the database from the underlying driver that this
     * DriverSpy is spying on. If logging is not enabled, an actual Connection to
     * the database returned. If logging is enabled, a ConnectionSpy object which
     * wraps the real Connection is returned.
     *
     * @param url JDBC connection URL .
     * @param info a list of arbitrary string tag/value pairs as connection
     *        arguments. Normally at least a "user" and "password" property should
     *        be included.
     *
     * @return a <code>Connection</code> object that represents a connection to
     *         the URL.
     *
     * @throws SQLException if a database access error occurs
     */
    public Connection connect(String url, Properties info) throws SQLException {

        Driver driver = getUnderlyingDriver(url);

        if(driver == null) {
            return null;
        }

        lastUnderlyingDriverRequested = driver;

        Connection con = driver.connect(url, info);

        if(con == null) {
            throw new SQLException("Invalid or unknown driver url: " + url);
        }

        if(!log.isJdbcLoggingEnabled()) {
            return con;
        }

        LoggingConnection logcon = new LoggingConnection(con);
        ParameterFormatter formatter = null;
        String dclass = driver.getClass().getName();

        if((dclass != null) && !dclass.isEmpty()) {
            formatter = config.getParameterFormatter(dclass);
        }

        if(formatter == null) {
            formatter = defaultParameterFormatter;
        }

        logcon.setParameterFormatter(formatter);

        return logcon;

    }

    /**
     * Gets information about the possible properties for the underlying driver.
     *
     * @param url the URL of the database to which to connect
     *
     * @param info a proposed list of tag/value pairs that will be sent on connect open
     * @return an array of <code>DriverPropertyInfo</code> objects describing
     *         possible properties. This array may be an empty array if no
     *         properties are required.
     *
     * @throws SQLException if a database access error occurs
     */
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {

        Driver driver = getUnderlyingDriver(url);

        if(driver == null) {
            return new DriverPropertyInfo[0];
        }

        lastUnderlyingDriverRequested = driver;

        return driver.getPropertyInfo(url, info);

    }

}
