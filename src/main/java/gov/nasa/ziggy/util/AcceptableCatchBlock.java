package gov.nasa.ziggy.util;

import static java.lang.annotation.ElementType.CONSTRUCTOR;
import static java.lang.annotation.ElementType.METHOD;

import java.lang.annotation.Repeatable;
import java.lang.annotation.Target;

/**
 * Marks a method that has a catch block that has been reviewed and deemed acceptable.
 * <p>
 * As a general matter, Ziggy code should be free of catch blocks. In some cases, this is either
 * impossible or impractical, in which case the method with the catch block should be annotated with
 * {@link AcceptableCatchBlock}. This annotation takes a {@code rationale} parameter whose value is
 * of type {@link Rationale} that indicates why the catch block is acceptable.
 * <p>
 * A method that has multiple catch blocks should have multiple annotations, one annotation per
 * catch block, with the annotation ordering matched to the catch block ordering.
 *
 * @author PT
 */
@Target({ METHOD, CONSTRUCTOR })
@Repeatable(AcceptableCatchBlock.List.class)
public @interface AcceptableCatchBlock {
    public enum Rationale {

        /** The caught exception must not be permitted to halt execution prematurely. */
        MUST_NOT_CRASH,

        /**
         * The caught exception is thought to be impossible by construction. In this case, the catch
         * block should throw {@link AssertionError} so that if the impossible ever happens, we'll
         * find out about it sooner rather than later.
         */
        CAN_NEVER_OCCUR,

        /**
         * The caught exception occurs in the {@link Runnable#run()} block of a {@link Runnable}.
         */
        EXCEPTION_IN_RUNNABLE,

        /**
         * The caught exception is translated to a runtime exception that is more appropriate for
         * the abstraction.
         */
        EXCEPTION_CHAIN,

        /**
         * The caught exception must not prevent some necessary cleanup that occurs before the
         * exception passes control back to the caller.
         */
        CLEANUP_BEFORE_EXIT,

        /**
         * The caught exception triggers a call to {@link System#exit(int)}.
         */
        SYSTEM_EXIT,

        /**
         * The caught exception triggers a call to a usage display, followed by
         * {@link System#exit(int)}.
         */
        USAGE
    }

    public Rationale rationale();

    @Target({ METHOD, CONSTRUCTOR })
    @interface List {
        AcceptableCatchBlock[] value();
    }
}
