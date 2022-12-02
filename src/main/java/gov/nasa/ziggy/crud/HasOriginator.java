package gov.nasa.ziggy.crud;

/**
 * Interface that can be implemented on all pipeline classes that have an originator. The typical
 * example is a pipeline result, but the concept potentially has greater generality. In combination
 * with the AbstractProducerConsumerCrud, this ensures that all instances of classes that have an
 * originator have their producer-consumer relationships properly managed (specifically, instances
 * retrieved for use in a processing step will have their originators added to that task's list of
 * producers, and instances produced by a task will have that task's ID set as the originator for
 * those instances).
 *
 * @author PT
 */
public interface HasOriginator {

    long getOriginator();

    void setOriginator(long originator);
}
