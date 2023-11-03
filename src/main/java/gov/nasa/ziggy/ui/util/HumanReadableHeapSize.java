package gov.nasa.ziggy.ui.util;

/**
 * Support for human-readable heap sizes, and conversions between human-readable and heap sizes in
 * MB.
 *
 * @author PT
 */
public class HumanReadableHeapSize {

    public enum HeapSizeUnit {
        MB {
            @Override
            public int heapSizeMb(float humanReadableHeapSize) {
                return (int) humanReadableHeapSize;
            }
        },
        GB {
            @Override
            public int heapSizeMb(float humanReadableHeapSize) {
                return (int) (humanReadableHeapSize * 1000);
            }
        },
        TB {
            @Override
            public int heapSizeMb(float humanReadableHeapSize) {
                return (int) (humanReadableHeapSize * 1000000);
            }
        };

        public abstract int heapSizeMb(float humanReadableHeapSize);
    }

    private final float humanReadableHeapSize;
    private final HeapSizeUnit heapSizeUnit;

    public HumanReadableHeapSize(float humanReadableHeapSize, HeapSizeUnit heapSizeUnit) {
        this.humanReadableHeapSize = humanReadableHeapSize;
        this.heapSizeUnit = heapSizeUnit;
    }

    public HumanReadableHeapSize(int heapSizeMb) {
        if (heapSizeMb < 1000) {
            humanReadableHeapSize = heapSizeMb;
            heapSizeUnit = HeapSizeUnit.MB;
            return;
        }
        if (heapSizeMb > 1000000) {
            humanReadableHeapSize = (float) heapSizeMb / 1000000;
            heapSizeUnit = HeapSizeUnit.TB;
            return;
        }
        humanReadableHeapSize = (float) heapSizeMb / 1000;
        heapSizeUnit = HeapSizeUnit.GB;
    }

    public float getHumanReadableHeapSize() {
        return humanReadableHeapSize;
    }

    public HeapSizeUnit getHeapSizeUnit() {
        return heapSizeUnit;
    }

    public int heapSizeMb() {
        return heapSizeUnit.heapSizeMb(humanReadableHeapSize);
    }

    @Override
    public String toString() {
        return Float.toString(humanReadableHeapSize) + " " + heapSizeUnit.name();
    }
}
