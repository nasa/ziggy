package gov.nasa.ziggy.util;

/**
 * Support for human-readable heap sizes, and conversions between human-readable and heap sizes in
 * GB.
 *
 * @author PT
 */
public class HumanReadableHeapSize {

    public enum HeapSizeUnit {
        MB {
            @Override
            public float heapSizeGigabytes(float humanReadableHeapSize) {
                return (int) humanReadableHeapSize / 1000;
            }
        },
        GB {
            @Override
            public float heapSizeGigabytes(float humanReadableHeapSize) {
                return humanReadableHeapSize;
            }
        },
        TB {
            @Override
            public float heapSizeGigabytes(float humanReadableHeapSize) {
                return humanReadableHeapSize * 1000;
            }
        };

        public abstract float heapSizeGigabytes(float humanReadableHeapSize);
    }

    private final float humanReadableHeapSize;
    private final HeapSizeUnit heapSizeUnit;

    public HumanReadableHeapSize(float humanReadableHeapSize, HeapSizeUnit heapSizeUnit) {
        this.humanReadableHeapSize = humanReadableHeapSize;
        this.heapSizeUnit = heapSizeUnit;
    }

    public HumanReadableHeapSize(float heapSizeGigabytes) {
        if (heapSizeGigabytes < 1) {
            humanReadableHeapSize = heapSizeGigabytes * 1000;
            heapSizeUnit = HeapSizeUnit.MB;
            return;
        }
        if (heapSizeGigabytes >= 1000) {
            humanReadableHeapSize = heapSizeGigabytes / 1000;
            heapSizeUnit = HeapSizeUnit.TB;
            return;
        }
        humanReadableHeapSize = heapSizeGigabytes;
        heapSizeUnit = HeapSizeUnit.GB;
    }

    public float getHumanReadableHeapSize() {
        return humanReadableHeapSize;
    }

    public HeapSizeUnit getHeapSizeUnit() {
        return heapSizeUnit;
    }

    public float heapSizeGigabytes() {
        return heapSizeUnit.heapSizeGigabytes(humanReadableHeapSize);
    }

    @Override
    public String toString() {
        return Float.toString(humanReadableHeapSize) + " " + heapSizeUnit.name();
    }
}
