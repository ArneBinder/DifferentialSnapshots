package concarne.scheduler;

import concarne.Snapshot;

/**
 * Created by macbookdata on 27.05.14.
 */
public abstract class Scheduler {
    Snapshot[] snapshots;
    int numberOfSnapshots;

    public Scheduler(Snapshot[] snapshots) {
        this.numberOfSnapshots = snapshots.length;
        this.snapshots = snapshots;
    }

    public abstract void execute();
}
