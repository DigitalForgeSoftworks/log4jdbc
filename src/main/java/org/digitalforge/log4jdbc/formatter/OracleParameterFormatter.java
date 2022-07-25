package org.digitalforge.log4jdbc.formatter;

import java.sql.Timestamp;
import java.time.format.DateTimeFormatter;
import java.util.Date;

/**
 * RDBMS specifics for the Oracle DB.
 */
public class OracleParameterFormatter extends ParameterFormatter {

    private static final DateTimeFormatter DATETIME_FORMATTER = DateTimeFormatter.ofPattern("MM/dd/yyyy HH:mm:ss");
    private static final DateTimeFormatter DATETIME_LONG_FORMATTER = DateTimeFormatter.ofPattern("MM/dd/yyyy HH:mm:ss.SSS");

    public OracleParameterFormatter() {

    }

    public String formatParameterObject(Object object) {

        if(object instanceof Timestamp) {
            return "to_timestamp('" + DATETIME_LONG_FORMATTER.format(((Timestamp)object).toInstant()) + "', 'mm/dd/yyyy hh24:mi:ss.ff3')";
        }

        if(object instanceof Date) {
            return "to_date('" + DATETIME_FORMATTER.format(((Date)object).toInstant()) + "', 'mm/dd/yyyy hh24:mi:ss')";
        }

        return super.formatParameterObject(object);

    }

}