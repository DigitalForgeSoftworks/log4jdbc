package org.digitalforge.log4jdbc.formatter;

import java.time.format.DateTimeFormatter;
import java.util.Date;

import org.digitalforge.log4jdbc.util.Utilities;

/**
 * Encapsulate sql formatting details about a particular relational database management system so that
 * accurate, useable SQL can be composed for that RDMBS.
 */
public class ParameterFormatter {

    /**
     * Default constructor.
     */
    public ParameterFormatter() {

    }

    private static final DateTimeFormatter DATETIME_FORMATTER = DateTimeFormatter.ofPattern("MM/dd/yyyy HH:mm:ss.SSS");

    /**
     * Format an Object that is being bound to a PreparedStatement parameter, for display. The goal is to reformat the
     * object in a format that can be re-run against the native SQL client of the particular Rdbms being used.  This
     * class should be extended to provide formatting instances that format objects correctly for different RDBMS
     * types.
     *
     * @param object jdbc object to be formatted.
     * @return formatted dump of the object.
     */
    public String formatParameterObject(final Object object) {

        if(object == null) {
            return "NULL";
        }

        if(object instanceof CharSequence) {
            return "'" + escapeString((CharSequence)object) + "'";
        }

        if(object instanceof Date) {
            return "'" + DATETIME_FORMATTER.format(((Date)object).toInstant()) + "'";
        }

        if(object instanceof Boolean) {
            return ((Boolean)object).booleanValue() ? "1" : "0";
        }

        if((object instanceof byte[]) && ((byte[])object).length <= 48) {
            return "0x" + Utilities.hex((byte[])object).toUpperCase();
        }

        return object.toString();

    }

    /**
     * Make sure string is escaped properly so that it will run in a SQL query analyzer tool.
     * At this time all we do is double any single tick marks.
     * Do not call this with a null string or else an exception will occur.
     *
     * @return the input String, escaped.
     */
    String escapeString(CharSequence in) {

        StringBuilder out = new StringBuilder();

        for(int i = 0, j = in.length(); i < j; i++) {

            char c = in.charAt(i);

            if(c == '\'') {
                out.append(c);
            }

            out.append(c);

        }

        return out.toString();

    }

}
