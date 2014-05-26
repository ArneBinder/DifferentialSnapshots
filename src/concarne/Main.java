package concarne;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

public class Main {

    static int numberOfSnapshots = 5;

    public static void main(String[] args) throws IOException {

        long before = System.currentTimeMillis();
        if(args.length > 1){
            generateSnapshots(Integer.parseInt(args[0]),args[1]);
            System.out.println("Total time elapsed: " + (System.currentTimeMillis() - before));
            return;
        }



        Snapshot[] snapshots = new Snapshot[numberOfSnapshots];
        for (int i = 1; i <= numberOfSnapshots; i++) {
            snapshots[i - 1] = new Snapshot("./R" + i + ".csv");
            System.out.println(String.format("Snapshot %s size: %s tuples", i, snapshots[i - 1].tuples.size()));
        }

        // contains the maximum number of operations to convert a snapshot into another
        DiffCandidate[][] upperBounds = new DiffCandidate[numberOfSnapshots][numberOfSnapshots];
        for (int i = 0; i < numberOfSnapshots; i++) {
            for (int j = 0; j < numberOfSnapshots; j++) {

                // diagonal contains trivial results, that shall not be computed.
                if (i == j){
                    upperBounds[i][i] = new DiffCandidate(i, i, null, Long.MAX_VALUE);
                    upperBounds[i][i].resultAlreadyCalculated = true;
                } else {
                    long lengthBound = snapshots[i].tuples.size() + snapshots[j].tuples.size();
                    upperBounds[i][j] = new DiffCandidate(i, j, null, lengthBound);
                }
            }
        }

        // contains the diffs between the snapshots
        Diff[][] results = new Diff[numberOfSnapshots][numberOfSnapshots];

        // create trivial results
        for (int i = 0; i < numberOfSnapshots; i++) {
            results[i][i] = new Diff();
        }

        DiffCandidate nextPair = findMinEntry(upperBounds);
        while(nextPair != null){

            Diff result;

//            if the best way to calculate the snapshot is indirectly
            if (nextPair.via != null) {

                int x = nextPair.x, y = nextPair.y, via = nextPair.via;

                // retrieve ∆(x, via)
                Diff diffAB = x < via ? results[x][via] : results[via][x];
                boolean turnAB = via < x;

                // retrieve ∆(via, y)
                Diff diffBC = via < y ? results[via][y] : results[y][via];
                boolean turnBC = via > y;

                result = new Diff(snapshots[nextPair.x], snapshots[nextPair.y], diffAB, diffBC, turnAB, turnBC);

            } else {

//                calculate without transitive stuff
                result = new Diff(snapshots[nextPair.x], snapshots[nextPair.y]);

            }

            System.out.println("Diff size: "+result.ops.size() + " "+result.forSnaps);
            results[nextPair.x][nextPair.y] = result;

            // update upper bounds matrix
//            nextPair.resultAlreadyCalculated = true;
            upperBounds[nextPair.x][nextPair.y].resultAlreadyCalculated = true;
            upperBounds[nextPair.x][nextPair.y].upperBound = result.totalCount();
            upperBounds[nextPair.y][nextPair.x].resultAlreadyCalculated = true;
            upperBounds[nextPair.y][nextPair.x].upperBound = result.totalCount();
            updateBounds(upperBounds, nextPair);

//            for(DiffCandidate[] row : upperBounds)
//                System.out.println(Arrays.toString(row));

            nextPair = findMinEntry(upperBounds);

        }


        for (int i = 0; i < 5; i++) {
            for (int j = 0; j < 5; j++) {
                if(i!=j)
                    if(i>j)
                        System.out.println("R"+(i+1)+",R"+(j+1)+","+results[j][i].getCounts(true));
                    else
                        System.out.println("R"+(i+1)+",R"+(j+1)+","+results[i][j].getCounts(false));
                else
                    System.out.println("R"+(i+1)+",R"+(j+1)+",0,0,0");
            }
        }

//    int i=1,j=1;
//    for (Snapshot s1 : snapshots ){
//
//        j = 1;
//        for (Snapshot s2 : snapshots ){
//
//
//            if(i < j) {
//
//                Diff diff = new Diff(s1,s2);
//                System.out.println(String.format("R%s,R%s,%s",i,j,diff.getCounts(false)));
//            }
//
//
//            j++;
//        }
//        i++;
//    }
        System.out.println("Total time elapsed: " + (System.currentTimeMillis() - before));
    }

    /**
     * @return update the shortest paths
     */
    private static void updateBounds(DiffCandidate[][] upperBounds, DiffCandidate updatedCandidate) {

        // the edge (x,y) has got a new weight

        int x = updatedCandidate.x, y = updatedCandidate.y;

        for (int z = 0; z < numberOfSnapshots; z++) {
            if (z == x || z == y) continue;

            // start from x
            long newCost = upperBounds[x][y].upperBound + upperBounds[y][z].upperBound;

            if (newCost < upperBounds[x][z].upperBound) {
                upperBounds[x][z].upperBound = newCost;
                upperBounds[x][z].via = y;
                upperBounds[z][x].upperBound = newCost;     // keep matrix symmetric
                upperBounds[z][x].via = y;
//                System.out.println("new upper bound estimate for "+x+" "+ z + ": "+newCost);
            }

            // start from y
            newCost = upperBounds[x][y].upperBound + upperBounds[x][z].upperBound;

            if (newCost < upperBounds[y][z].upperBound) {
                upperBounds[y][z].upperBound = newCost;
                upperBounds[y][z].via = x;
                upperBounds[z][y].upperBound = newCost;
                upperBounds[z][y].via = x;
//                System.out.println("new upper bound estimate for "+y+" "+ z + ": "+newCost);
            }
        }


    }

    /**
     * @return the matrix entry with the lowest upper bound.
     */
    private static DiffCandidate findMinEntry(DiffCandidate[][] upperBounds) {

        DiffCandidate best = upperBounds[0][0];

        for (int i = 0; i < upperBounds.length; i++) {

            for (int j = 1; j < upperBounds[0].length; j++) {

                if ( ! upperBounds[i][j].resultAlreadyCalculated && upperBounds[i][j].upperBound < best.upperBound){
                    best = upperBounds[i][j];
                }

            }

        }

//        System.out.println("Min: "+best);
        return best.resultAlreadyCalculated ? null : best;

    }

    static class DiffCandidate {
        Integer x, y, via;            // via is the transitive snapshot (null if direct is optimal)
        long upperBound;
        boolean resultAlreadyCalculated = false;

        DiffCandidate(Integer x, Integer y, Integer via, long upperBound) {
            this.x = x;
            this.y = y;
            this.via = via;
            this.upperBound = upperBound;
        }

        @Override
        public String toString() {
            return "DiffCandidate{" +
                    "row=" + x +
                    ", col=" + y +
                    ", via=" + via +
                    ", upperBound=" + upperBound +
                    ", resultAlreadyCalculated=" + resultAlreadyCalculated +
                    '}';
        }
    }

    public static void generateSnapshots(int size, String path) throws IOException {
        //Random random = new Random();
        float[] opProbabilities = new float[]{0.02f,0.02f,0.02f,0.94f};

        // size = 20.000.000
        Snapshot[] snapshots = new Snapshot[5];
        snapshots[0] = new Snapshot(size);
        snapshots[1] = new Snapshot(snapshots[0],opProbabilities);
        snapshots[2] = new Snapshot(snapshots[1],opProbabilities);
        snapshots[3] = new Snapshot(snapshots[0],opProbabilities);
        snapshots[4] = new Snapshot(snapshots[1],opProbabilities);

        for (int i = 0; i < 5; i++) {
            snapshots[i].writeSnapshot(path + File.separator + "R" + (i+1) + ".csv");
        }

    }


}
