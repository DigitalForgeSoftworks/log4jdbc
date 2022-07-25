package org.digitalforge.log4jdbc.util;

import java.nio.charset.StandardCharsets;

/**
 * Static utility methods for use throughout the project.
 */
public class Utilities {

    private static final byte[] HEX_CHARS = "0123456789abcdef".getBytes(StandardCharsets.US_ASCII);

    public static String hex(byte[] bytes) {

        byte[] hexChars = new byte[bytes.length * 2];

        for (int j = 0; j < bytes.length; j++) {

            int v = bytes[j] & 0xFF;

            hexChars[j * 2] = HEX_CHARS[v >>> 4];
            hexChars[j * 2 + 1] = HEX_CHARS[v & 0x0F];

        }

        return new String(hexChars, StandardCharsets.UTF_8);

    }

    /**
     * Right justify a field within a certain number of spaces.
     * @param fieldSize field size to right justify field within.
     * @param field contents to right justify within field.
     * @return the field, right justified within the requested size.
     */
    public static String rightJustify(int fieldSize, String field) {
        if(field == null) {
            field = "";
        }
        StringBuffer output = new StringBuffer();
        for(int i = 0, j = fieldSize - field.length(); i < j; i++) {
            output.append(' ');
        }
        output.append(field);
        return output.toString();
    }

    /**
     * Trim whitespace off the right of a string.
     * @param s input String to trim.
     * @return output trimmed string.
     */
    public static String rtrim(String s) {
        if(s == null) {
            return null;
        }
        int i = s.length() - 1;
        while(i >= 0 && Character.isWhitespace(s.charAt(i))) {
            i--;
        }
        return s.substring(0, i + 1);
    }

}
