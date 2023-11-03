package gov.nasa.ziggy.util;

/**
 * Provides formatting options for integers of various kinds.
 *
 * @author PT
 */
public class IntegerFormatter {

    /**
     * Abbreviations for engineering notation.
     */
    private static final String[] ABBREVIATION = { "", "k", "M", "G", "T", "P", "E" };

    /**
     * Converts a long to engineering notation and returns as a string. Returned values always have
     * a maximum 3 digits precision, with fewer digits for small integers. For example, a value of
     * 1,234,567 is returned as "1.23 M"; a value of 22 is returned as "22".
     */
    public static String engineeringNotation(long value) {

        String valueInEngineeringNotation = null;
        long unsignedValue = Math.abs(value);
        String unsignedValueString = Long.toString(unsignedValue);
        int groupsOf3Digits = (unsignedValueString.length() - 1) / 3;
        int digitsBeforeDecimalPoint = unsignedValueString.length() % 3;
        if (groupsOf3Digits == 0) { // < 1000, special case
            valueInEngineeringNotation = Long.toString(value);
        } else {
            StringBuilder sb = new StringBuilder();
            if (value < 0) {
                sb.append("-");
            }
            sb.append(unsignedValueString.charAt(0));
            if (digitsBeforeDecimalPoint == 1) {
                sb.append(".");
            }
            sb.append(unsignedValueString.charAt(1));
            if (digitsBeforeDecimalPoint == 2) {
                sb.append(".");
            }
            sb.append(unsignedValueString.charAt(2));
            sb.append(" ");
            sb.append(ABBREVIATION[groupsOf3Digits]);
            valueInEngineeringNotation = sb.toString();
        }
        return valueInEngineeringNotation;
    }
}
