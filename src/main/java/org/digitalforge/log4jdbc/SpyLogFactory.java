package org.digitalforge.log4jdbc;

/**
 * A provider for a SpyLogDelegator.  This allows a single switch point to abstract
 * away which logging system to use for spying on JDBC calls.
 *
 * The SLF4J logging facade is used, which is a very good general purpose facade for plugging into
 * numerous java logging systems, simply and easily.
 */
public class SpyLogFactory {

    /**
     * Do not allow instantiation.  Access is through static method.
     */
    private SpyLogFactory() {
    }

    /**
     * The logging system of choice.
     */
    private static final SpyLogDelegator logger = new Slf4jSpyLogDelegator();

    /**
     * Get the default SpyLogDelegator for logging to the logger.
     *
     * @return the default SpyLogDelegator for logging to the logger.
     */
    public static SpyLogDelegator getSpyLogDelegator() {
        return logger;
    }

}

