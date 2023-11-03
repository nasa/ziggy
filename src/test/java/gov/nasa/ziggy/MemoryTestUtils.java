package gov.nasa.ziggy;

import java.util.ArrayList;
import java.util.List;

/**
 * Utility methods for testing memory management.
 */
public class MemoryTestUtils {

    /**
     * Allocate as many chunks of memory as possible, until out of memory is reached. The chunk size
     * should be large enough that this routine can allocate a small array after the
     * <code>OutOfMemoryError</code> to hold references to the allocated chunks.
     *
     * @param chunkSize the size of each chunk to allocate
     * @return an array of allocated chunks
     */
    public static byte[][] allocateAllChunksPossible(int chunkSize) {
        List<byte[]> chunks = new ArrayList<>();
        for (;;) {
            try {
                byte[] chunk = new byte[chunkSize];
                chunks.add(chunk);
            } catch (OutOfMemoryError ex) {
                break;
            }
        }

        return chunks.toArray(new byte[chunks.size()][]);
    }
}
