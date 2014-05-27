package concarne;

import concarne.scheduler.ShortestPathScheduler;

import java.io.File;
import java.io.IOException;

public class Main {

    static int numberOfSnapshots = 5;

    public static void main(String[] args) throws IOException {

        long before = System.currentTimeMillis();

        String path = args.length > 0 ? args[0] : "/local/II2014/uebung3";

        // generate test data, if arguments are passed
        if (args.length > 1) {
            generateSnapshots(Integer.parseInt(args[1]), path);
            System.out.println("Total time elapsed: " + (System.currentTimeMillis() - before));
            return;
        }

        // read snapshot files from disk
        Snapshot[] snapshots = new Snapshot[numberOfSnapshots];
        for (int i = 1; i <= numberOfSnapshots; i++) {
            snapshots[i - 1] = new Snapshot(path + "/R" + i + ".csv");
            System.out.println(String.format("Snapshot %s size: %s tuples", i, snapshots[i - 1].tuples.size()));
        }
        System.out.println("\ntimeRead: " + Snapshot.timeRead + " ms");
        System.out.println("timeParse (IDs): " + (Snapshot.timeParse - Snapshot.timeHash) + " ms");
        System.out.println("timeHash: " + Snapshot.timeHash + " ms\n");

        // compute all snapshots by exploiting the transitive dependencies that that cost the least
        ShortestPathScheduler scheduler = new ShortestPathScheduler(snapshots);
//        AllTransitiveScheduler scheduler = new AllTransitiveScheduler(snapshots);
        scheduler.execute();

//      Compute all snapshots in their input order
//        NaiveSnapshots(snapshots);

        System.out.println("\ntimeParsed(values): " + ColumnValues.timeParsed + " ms");

        System.out.println("Total time elapsed: " + (System.currentTimeMillis() - before));
    }

    public static void generateSnapshots(int size, String path) throws IOException {
        //Random random = new Random();
        float[] opProbabilities = new float[]{0.02f, 0.02f, 0.02f, 0.94f};

        // size = 20.000.000
        Snapshot[] snapshots = new Snapshot[5];
        snapshots[0] = new Snapshot(size);
        snapshots[1] = new Snapshot(snapshots[0], opProbabilities);
        snapshots[2] = new Snapshot(snapshots[1], opProbabilities);
        snapshots[3] = new Snapshot(snapshots[0], opProbabilities);
        snapshots[4] = new Snapshot(snapshots[1], opProbabilities);

        for (int i = 0; i < 5; i++) {
            snapshots[i].writeSnapshot(path + File.separator + "R" + (i + 1) + ".csv");
        }

    }

    private static void naiveSnapshots(Snapshot[] snapshots) {
        int i = 1, j = 1;
        for (Snapshot s1 : snapshots) {

            j = 1;
            for (Snapshot s2 : snapshots) {


                if (i < j) {

                    Diff diff = new Diff(s1, s2);
                    System.out.println(String.format("R%s,R%s,%s", i, j, diff.getCounts(false)));
                }


                j++;
            }
            i++;
        }
    }


}
