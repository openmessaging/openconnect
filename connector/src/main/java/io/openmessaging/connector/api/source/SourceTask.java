package io.openmessaging.connector.api.source;

import io.openmessaging.connector.api.Task;
import io.openmessaging.connector.api.data.SourceDataEntry;
import java.util.Collection;

public abstract class SourceTask implements Task {

  protected SourceTaskContext context;

  /**
   * Initialize this sourceTask.
   *
   * @param context the context of current task
   */
  public void initialize(SourceTaskContext context) {
    this.context = context;
  }

  /** Return a collection of message entries to send. */
  public abstract Collection<SourceDataEntry> poll();

  /**
   * If the user wants to use external storage to save the position,user can implement this
   * function.
   */
  public void commit() {}
}
