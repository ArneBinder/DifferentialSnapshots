package concarne.scheduler;

import concarne.Diff;
import concarne.Snapshot;

/**
 * Computes snapshot differences in an order that exploits the transitive algorithm.
 * Created by macbookdata on 27.05.14.
 */
public class ShortestPathScheduler extends Scheduler {

    public ShortestPathScheduler(Snapshot[] snapshots){
        super(snapshots);
    }

    /**
     * Computes the results.
     */
    @Override
    public void execute() {

        // contains the maximum number of operations to convert a snapshot into another
        DiffCandidate[][] upperBounds = new DiffCandidate[numberOfSnapshots][numberOfSnapshots];
        for (int i = 0; i < numberOfSnapshots; i++) {
            for (int j = 0; j < numberOfSnapshots; j++) {

                // diagonal contains trivial results, that shall not be computed.
                if (i == j) {
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
        while (nextPair != null) {

            Diff result;

            long before = System.currentTimeMillis();
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

            System.out.println(String.format("Diff size: %s %s (%s ms)",result.ops.size(), result.forSnaps, System.currentTimeMillis()-before));
            results[nextPair.x][nextPair.y] = result;

            // update upper bounds matrix
//            nextPair.resultAlreadyCalculated = true;
            upperBounds[nextPair.x][nextPair.y].resultAlreadyCalculated = true;
            upperBounds[nextPair.x][nextPair.y].upperBound = result.totalCount();
            upperBounds[nextPair.y][nextPair.x].resultAlreadyCalculated = true;
            upperBounds[nextPair.y][nextPair.x].upperBound = result.totalCount();
            updateBounds(upperBounds, nextPair);

            nextPair = findMinEntry(upperBounds);

        }


        // print results
        for (int i = 0; i < 5; i++) {
            for (int j = 0; j < 5; j++) {
                if (i != j)
                    if (i > j)
                        System.out.println("R" + (i + 1) + ",R" + (j + 1) + "," + results[j][i].getCounts(true));
                    else
                        System.out.println("R" + (i + 1) + ",R" + (j + 1) + "," + results[i][j].getCounts(false));
                else
                    System.out.println("R" + (i + 1) + ",R" + (j + 1) + ",0,0,0");
            }
        }
    }

    /**
     * @return update the shortest paths
     */
    private void updateBounds(DiffCandidate[][] upperBounds, DiffCandidate updatedCandidate) {

        // the edge (x,y) has got a new weight

        int x = updatedCandidate.x, y = updatedCandidate.y;

        for (int z = 0; z < numberOfSnapshots; z++) {
            if (z == x || z == y) continue;

            // start from x
            // ∆(x,y) + ∆(y,z) would be a provable upper bound but in practice, the distances between snapshots are far from worst case.
            // and although computing a direct pair is usually expensive, the cost for that can be amortized by exploit transitiveness in the next step.
            long newCost = 2*upperBounds[x][y].upperBound + upperBounds[y][z].upperBound;

            if (newCost < upperBounds[x][z].upperBound) {
                upperBounds[x][z].upperBound = newCost;
                upperBounds[x][z].via = y;
                upperBounds[z][x].upperBound = newCost;     // keep matrix symmetric
                upperBounds[z][x].via = y;
//                System.out.println("new upper bound estimate for "+x+" "+ z + ": "+newCost);
            }

            // start from y
            // see comment above for amortized cost
            newCost = 2*upperBounds[x][y].upperBound + upperBounds[x][z].upperBound;

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
    private DiffCandidate findMinEntry(DiffCandidate[][] upperBounds) {

        DiffCandidate best = upperBounds[0][0];

        for (int i = 0; i < upperBounds.length; i++) {

            for (int j = 1; j < upperBounds[0].length; j++) {

                if (!upperBounds[i][j].resultAlreadyCalculated && upperBounds[i][j].upperBound < best.upperBound) {
                    best = upperBounds[i][j];
                }

            }

        }

//        System.out.println("Min: "+best);
        return best.resultAlreadyCalculated ? null : best;

    }

    class DiffCandidate {

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

}
