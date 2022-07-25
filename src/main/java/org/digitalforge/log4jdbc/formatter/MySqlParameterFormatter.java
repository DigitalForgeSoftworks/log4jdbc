package org.digitalforge.log4jdbc.formatter;

import java.sql.Date;
import java.sql.Time;
import java.time.format.DateTimeFormatter;

/**
 * RDBMS specifics for the MySql db.
 */
public class MySqlParameterFormatter extends ParameterFormatter {

    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("'yyyy-MM-dd'");
    private static final DateTimeFormatter DATETIME_FORMATTER = DateTimeFormatter.ofPattern("'yyyy-MM-dd HH:mm:ss'");
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("'HH:mm:ss'");

    public MySqlParameterFormatter() {

    }

    @Override
    public String formatParameterObject(final Object object) {

        if(object instanceof java.sql.Time) {
            return TIME_FORMATTER.format(((Time)object).toInstant());
        }

        if(object instanceof java.sql.Date) {
            return DATE_FORMATTER.format(((Date)object).toInstant());
        }

        if(object instanceof java.util.Date) {// (includes java.sql.Timestamp)
            return DATETIME_FORMATTER.format(((java.util.Date)object).toInstant());
        }

        return super.formatParameterObject(object);

    }

}