package gov.nasa.ziggy.util;

import java.text.DecimalFormat;

/** Utilities for formatting real values (i.e., floats and doubles). */
public class RealValueFormatter {

    /**
     * Format a double-precision value with a reasonable number of digits. This is optimized for
     * displaying the cost of one or more remote jobs.
     */
    public static String costFormatter(double cost) {
        DecimalFormat decimalFormat = defaultCostFormat();
        if (cost < 1) {
            decimalFormat = ultraSmallCostFormat();
        } else if (cost < 10) {
            decimalFormat = smallCostFormat();
        } else if (cost < 100) {
            decimalFormat = mediumCostFormat();
        }

        return decimalFormat.format(cost);
    }

    private static DecimalFormat ultraSmallCostFormat() {
        return new DecimalFormat("#.####");
    }

    private static DecimalFormat smallCostFormat() {
        return new DecimalFormat("#.###");
    }

    private static DecimalFormat mediumCostFormat() {
        return new DecimalFormat("#.##");
    }

    private static DecimalFormat defaultCostFormat() {
        return new DecimalFormat("#.#");
    }

    public static String costFormatter(float realValue) {
        return costFormatter((double) realValue);
    }
}
