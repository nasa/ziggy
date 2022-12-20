package gov.nasa.ziggy.data.management;

import jakarta.xml.bind.annotation.adapters.XmlAdapter;

/**
 * Provides status for several users within data receipt. Specifically:
 * <ol>
 * <li>ABSENT and PRESENT are used to indicate whether a file that was listed in a manifest was also
 * found in the delivered files.
 * <li>VALID and INVALID are used to indicate whether the file size and checksum are equal to the
 * values in the manifest. They are also used in an acknowledgement to indicate whether every file
 * in the acknowledgement is both present and valid.
 * <li>IMPORTED and NOT_IMPORTED indicate whether the file was imported. These are used when
 * displaying the results of a data receipt activity on the pipeline console (i.e., a table of files
 * is presented, and each one is either IMPORTED or NOT_IMPORTED).
 * </ol>
 * <p>
 * To simplify the process of acknowledgement generation, the {@link DataReceiptStatus} also
 * provides a method to combine instances, the {@link #and(DataReceiptStatus)} method. The logic for
 * this method is as follows:
 * <ol>
 * <li>ABSENT and anything yields INVALID.
 * <li>INVALID and anything yields INVALID.
 * <li>PRESENT and VALID yields VALID.
 * <li>VALID and VALID yields VALID.
 * <li>PRESENT and PRESENT yields VALID.
 * <li>All other combinations result in {@link IllegalArgumentException}.
 * </ol>
 *
 * @author PT
 */
public enum DataReceiptStatus {

    ABSENT {
        @Override
        public DataReceiptStatus and(DataReceiptStatus other) {
            return INVALID;
        }

    },
    PRESENT {
        @Override
        public DataReceiptStatus and(DataReceiptStatus other) {
            if (other.equals(PRESENT) || other.equals(VALID)) {
                return VALID;
            }
            if (other.equals(ABSENT) || other.equals(INVALID)) {
                return INVALID;
            } else {
                throw new IllegalArgumentException(
                    "Cannot AND DataReceiptStatus of " + toString() + " with PRESENT");
            }
        }

    },
    VALID {
        @Override
        public DataReceiptStatus and(DataReceiptStatus other) {
            if (other.equals(PRESENT) || other.equals(VALID)) {
                return VALID;
            }
            if (other.equals(ABSENT) || other.equals(INVALID)) {
                return INVALID;
            } else {
                throw new IllegalArgumentException(
                    "Cannot AND DataReceiptStatus of " + toString() + " with VALID");
            }

        }

    },
    INVALID {
        @Override
        public DataReceiptStatus and(DataReceiptStatus other) {
            return INVALID;
        }

    },
    IMPORTED {
        @Override
        public DataReceiptStatus and(DataReceiptStatus other) {
            throw new IllegalArgumentException(
                "Cannot AND DataReceiptStatus of IMPORTED with other DataReceiptStatus");
        }

    },
    NOT_IMPORTED {
        @Override
        public DataReceiptStatus and(DataReceiptStatus other) {
            throw new IllegalArgumentException(
                "Cannot AND DataReceiptStatus of NOT_IMPORTED with other DataReceiptStatus");
        }

    };

    public abstract DataReceiptStatus and(DataReceiptStatus other);

    /**
     * Provides conversion between the {@link DataReceiptStatus} enumerated type and {@link String},
     * for use in XML files.
     *
     * @author PT
     */
    public static class DataReceiptStatusAdapter extends XmlAdapter<String, DataReceiptStatus> {

        @Override
        public DataReceiptStatus unmarshal(String status) throws Exception {
            return DataReceiptStatus.valueOf(status.toUpperCase());
        }

        @Override
        public String marshal(DataReceiptStatus status) throws Exception {
            return status.toString().toLowerCase();
        }

    }

}
