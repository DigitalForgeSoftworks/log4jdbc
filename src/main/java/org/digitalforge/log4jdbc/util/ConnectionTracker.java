package org.digitalforge.log4jdbc.util;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;

import org.digitalforge.log4jdbc.LoggingConnection;
import org.digitalforge.log4jdbc.LoggingStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConnectionTracker {

    private static final Logger log = LoggerFactory.getLogger(ConnectionTracker.class);

    private static final int CONNECTION_DUMP_THRESHOLD = Integer.parseInt(System.getenv().getOrDefault("LOG4JDBC_CONNECTION_DUMP_THRESHOLD", "0"));

    private final Map<Integer, LoggingConnection> connections = new ConcurrentHashMap<>();
    private final Set<LoggingStatement> statements = Collections.synchronizedSet(Collections.newSetFromMap(new WeakHashMap<>()));

    private Instant lastDumpTime;

    public ConnectionTracker() {

    }

    public void track(int number, LoggingConnection connection) {

        connections.put(number, connection);

        if(connections.size() < CONNECTION_DUMP_THRESHOLD) {
            return;
        }

        if((lastDumpTime != null) && !lastDumpTime.isBefore(Instant.now().minus(10, ChronoUnit.MINUTES))) {
            return;
        }

        dumpStatements();

    }

    public void track(LoggingStatement statement) {
        statements.add(statement);
    }

    public void untrack(int number) {
        connections.remove(number);
    }

    public void untrack(LoggingStatement statement) {
        statements.remove(statement);
    }

    /**
     * Get a dump of how many connections are open, and which connection numbers
     * are open.
     *
     * @return an open connection dump.
     */
    public String getOpenConnectionsDump() {

        StringBuilder sb = new StringBuilder();
        int size;
        Integer[] keys;

        synchronized(connections) {

            size = connections.size();

            if(size == 0) {

                return "open connections: none";
            }

            keys = connections.keySet().toArray(new Integer[size]);

        }

        Arrays.sort(keys);

        sb.append("open connections: ");

        sb.append("(");
        sb.append(size);
        sb.append(") ");

        for(Integer key : keys) {
            sb.append(key);
            sb.append(" ");
        }

        return sb.toString().trim();

    }

    private void dumpStatements() {

        lastDumpTime = Instant.now();

        List<String> sql;

        synchronized(statements) {
            sql = new ArrayList<>(statements.size());
            for(LoggingStatement s : statements) {
                sql.add(s.getCurrentSql());
            }
        }

        for(String s : sql) {
            log.info("Active statement: " + s);
        }

    }


}
