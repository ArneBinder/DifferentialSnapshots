package concarne;

import com.sun.org.apache.bcel.internal.generic.NOP;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by macbookdata on 23.05.14.
 */
public class Diff {


    public ConcurrentHashMap<Identifier, Byte> ops = new ConcurrentHashMap<>();
    int[] counts = new int[3];

    public Diff(){

        countOps();

    }


    public String forSnaps;


    public Diff(Snapshot snapshotA, final Snapshot snapshotB){

        this(snapshotA, snapshotB, Runtime.getRuntime().availableProcessors());

    }

    public Diff (Snapshot snapshotA, final Snapshot snapshotB, int numThreads) {

        // output info to describe for which snapshots this diff is
        forSnaps = snapshotA.path + " " + snapshotB.path;

        // partition input snapshot
        Set<Map.Entry<Identifier, ColumnValues>> entrySet = snapshotA.tuples.entrySet();
        final Map.Entry<Identifier,ColumnValues>[] entries = entrySet.toArray(new Map.Entry[entrySet.size()]);

        Thread[] workers = new Thread[numThreads];

        int step = entries.length/numThreads;
        for (int i = 0; i < numThreads; i++) {

            final int from = i*step, to = i == numThreads-1 ? entries.length-1 : (i+1)*step-1;
            System.out.println(String.format("Starting thread %s for diff %s in range [%s, %s]", i+1, forSnaps, from, to ));

            workers[i] = new Thread(new Runnable() {
                int fromIdx = from, toIdx = to;
                @Override public void run() {

                    for (int j = fromIdx; j <= toIdx; j++) {
                        Map.Entry<Identifier, ColumnValues> t1 = entries[j];

                        ColumnValues t1ValuesInB = snapshotB.tuples.get(t1.getKey());
                        if (t1ValuesInB != null){
                            if ( ! t1.getValue().equals(t1ValuesInB)) {
                                ops.put(t1.getKey(), SUB);
                            }

                        } else {
                            ops.put(t1.getKey(), DEL);
                        }
                    }

                }
            });

            workers[i].start();
        }

        for (int i = 0; i < numThreads; i++) {
            try {
                workers[i].join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        countOps();
    }


    byte[][] combinations = new byte[][]{
        new byte[]{X, NOP, INS, INS},
        new byte[]{SUBNOP, X, X, DEL},
        new byte[]{X, DEL, SUBNOP, SUB},
        new byte[]{INS, DEL, SUB, NOP},
    };

    public Diff(Snapshot snapshotA, Snapshot snapshotC, Diff diffAB, Diff diffBC){
        this(snapshotA,snapshotC,diffAB,diffBC,false, false);
    }

    /**
     *
     * @param diffAB
     * @param diffBC
     * @return snapshot difference between a and c
     */
    public Diff(Snapshot snapshotA, Snapshot snapshotC, Diff diffAB, Diff diffBC, boolean turnAB, boolean turnBC){

        forSnaps = snapshotA.path + " (via " + diffAB.forSnaps.replace(snapshotA.path,"").replace(snapshotC.path,"") + ") "+ snapshotC.path;

        for(Map.Entry<Identifier, Byte> entryAB : diffAB.ops.entrySet()){

            Identifier id = entryAB.getKey();

            Byte opBC = diffBC.ops.get(id);
            if(opBC == null) {
                opBC = NOP;
            }
            opBC = turnOp(opBC, turnBC);

            byte opAC = combinations[turnOp(entryAB.getValue(),turnAB)][opBC];

            if(opAC == SUBNOP){
                if(! snapshotA.tuples.get(id).equals(snapshotC.tuples.get(id))){
                    ops.put(id, SUB);
                }
            } else {
                ops.put(id, opAC);
            }

        }

        for(Map.Entry<Identifier, Byte> entryBC : diffBC.ops.entrySet()){

            Identifier id = entryBC.getKey();
            if( ! diffAB.ops.containsKey(id) ){

                byte operation = combinations[NOP][turnOp(entryBC.getValue(), turnBC)];
                ops.put(id, operation);
            }

        }

        countOps();

    }


    private static byte turnOp(byte op, boolean turn){
        if(!turn)
            return op;
        if(op==INS)
            return DEL;
        if(op==DEL)
            return INS;
        return op;
    }

    private void countOps(){

        counts = new int[3];
        for (byte op : ops.values()){
            switch (op){
                case INS:
                    counts[0]++;
                    break;
                case DEL:
                    counts[1]++;
                    break;
                case SUB:
                    counts[2]++;
                    break;
            }
        }
    }

    public String getCounts(boolean turn){
        if(turn)
            return counts[1]+","+counts[0]+","+counts[2];
        else
            return counts[0]+","+counts[1]+","+counts[2];
    }

    public static final byte INS = 0;
    public static final byte DEL = 1;
    public static final byte SUB = 2;
    public static final byte NOP = 3;
    public static final byte SUBNOP = 4;
    public static final byte X = -1;

    public long totalCount() {
        return counts[0] + counts[1] + counts[2];
    }

//    enum OP {INS, DEL, SUB, NOP, SUBNOP, X}
}
