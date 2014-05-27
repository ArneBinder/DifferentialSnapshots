package concarne;

import com.sun.org.apache.bcel.internal.generic.NOP;

import java.util.*;
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


    public Diff (Snapshot snapshotA, Snapshot snapshotB) {

        forSnaps = snapshotA.path + " " + snapshotB.path;

        for (Map.Entry<Identifier, ColumnValues> t1 : snapshotA.tuples.entrySet()) {

            ColumnValues t1ValuesInB = snapshotB.tuples.get(t1.getKey());
            if (t1ValuesInB != null){
                if ( ! t1.getValue().equals(t1ValuesInB)) {
                    ops.put(t1.getKey(), SUB);
                }

            } else {
                ops.put(t1.getKey(), DEL);
            }
        }

        for (Map.Entry<Identifier, ColumnValues> t2 : snapshotB.tuples.entrySet()) {

            ColumnValues t2ValuesInA = snapshotA.tuples.get(t2.getKey());
            if (t2ValuesInA == null){
                ops.put(t2.getKey(), INS);
            }
        }

        countOps();
    }

//    public Diff(Snapshot snapshotA, final Snapshot snapshotB){
//
//        this(snapshotA, snapshotB, Runtime.getRuntime().availableProcessors());//Runtime.getRuntime().availableProcessors()
//
//    }

    public Diff (Snapshot snapshotA, final Snapshot snapshotB, int numThreads) {


        // output info to describe for which snapshots this diff is
        forSnaps = snapshotA.path + " " + snapshotB.path;

        // partition input snapshot
        Set<Map.Entry<Identifier, ColumnValues>> entrySet = snapshotA.tuples.entrySet();
        final Map.Entry<Identifier,ColumnValues>[] entries = entrySet.toArray(new Map.Entry[entrySet.size()]);

        Thread[] workers = new Thread[numThreads];

        long start = System.currentTimeMillis();

        int step = entries.length/numThreads;
        for (int i = 0; i < numThreads; i++) {

            final int from = i*step, to = i == numThreads-1 ? entries.length-1 : (i+1)*step-1;
//            System.out.println(String.format("Starting thread %s for diff %s in range [%s, %s]", i+1, forSnaps, from, to ));

            workers[i] = new Thread(new Runnable() {
                int fromIdx = from, toIdx = to;
                @Override public void run() {

                    long before = System.currentTimeMillis();
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
                    totalOpWithoutInsCost += System.currentTimeMillis() - before;

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

        long before = System.currentTimeMillis();
        for (Map.Entry<Identifier, ColumnValues> t2 : snapshotB.tuples.entrySet()) {
//
            if ( ! snapshotA.tuples.containsKey(t2.getKey()) ){
                ops.put(t2.getKey(), INS);
            }
        }
        totalInsCost += System.currentTimeMillis() - before;

        checkSum += System.currentTimeMillis() - start;

        countOps();
    }
    static long totalInsCost = 0, totalOpWithoutInsCost = 0, checkSum = 0;

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
        System.out.println("Diff result: "+Arrays.toString(counts));

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


// non parallel
///Library/Java/JavaVirtualMachines/jdk1.7.0_51.jdk/Contents/Home/bin/java -Xmx6000m -Didea.launcher.port=7533 "-Didea.launcher.bin.path=/Applications/IntelliJ IDEA 13 CE.app/bin" -Dfile.encoding=UTF-8 -classpath "/Library/Java/JavaVirtualMachines/jdk1.7.0_51.jdk/Contents/Home/lib/ant-javafx.jar:/Library/Java/JavaVirtualMachines/jdk1.7.0_51.jdk/Contents/Home/lib/dt.jar:/Library/Java/JavaVirtualMachines/jdk1.7.0_51.jdk/Contents/Home/lib/javafx-doclet.jar:/Library/Java/JavaVirtualMachines/jdk1.7.0_51.jdk/Contents/Home/lib/javafx-mx.jar:/Library/Java/JavaVirtualMachines/jdk1.7.0_51.jdk/Contents/Home/lib/jconsole.jar:/Library/Java/JavaVirtualMachines/jdk1.7.0_51.jdk/Contents/Home/lib/sa-jdi.jar:/Library/Java/JavaVirtualMachines/jdk1.7.0_51.jdk/Contents/Home/lib/tools.jar:/Library/Java/JavaVirtualMachines/jdk1.7.0_51.jdk/Contents/Home/jre/lib/charsets.jar:/Library/Java/JavaVirtualMachines/jdk1.7.0_51.jdk/Contents/Home/jre/lib/deploy.jar:/Library/Java/JavaVirtualMachines/jdk1.7.0_51.jdk/Contents/Home/jre/lib/htmlconverter.jar:/Library/Java/JavaVirtualMachines/jdk1.7.0_51.jdk/Contents/Home/jre/lib/javaws.jar:/Library/Java/JavaVirtualMachines/jdk1.7.0_51.jdk/Contents/Home/jre/lib/jce.jar:/Library/Java/JavaVirtualMachines/jdk1.7.0_51.jdk/Contents/Home/jre/lib/jfr.jar:/Library/Java/JavaVirtualMachines/jdk1.7.0_51.jdk/Contents/Home/jre/lib/jfxrt.jar:/Library/Java/JavaVirtualMachines/jdk1.7.0_51.jdk/Contents/Home/jre/lib/jsse.jar:/Library/Java/JavaVirtualMachines/jdk1.7.0_51.jdk/Contents/Home/jre/lib/management-agent.jar:/Library/Java/JavaVirtualMachines/jdk1.7.0_51.jdk/Contents/Home/jre/lib/plugin.jar:/Library/Java/JavaVirtualMachines/jdk1.7.0_51.jdk/Contents/Home/jre/lib/resources.jar:/Library/Java/JavaVirtualMachines/jdk1.7.0_51.jdk/Contents/Home/jre/lib/rt.jar:/Library/Java/JavaVirtualMachines/jdk1.7.0_51.jdk/Contents/Home/jre/lib/ext/dnsns.jar:/Library/Java/JavaVirtualMachines/jdk1.7.0_51.jdk/Contents/Home/jre/lib/ext/localedata.jar:/Library/Java/JavaVirtualMachines/jdk1.7.0_51.jdk/Contents/Home/jre/lib/ext/sunec.jar:/Library/Java/JavaVirtualMachines/jdk1.7.0_51.jdk/Contents/Home/jre/lib/ext/sunjce_provider.jar:/Library/Java/JavaVirtualMachines/jdk1.7.0_51.jdk/Contents/Home/jre/lib/ext/sunpkcs11.jar:/Library/Java/JavaVirtualMachines/jdk1.7.0_51.jdk/Contents/Home/jre/lib/ext/zipfs.jar:/Users/macbookdata/IdeaProjects/DifferentialSnapshots/out/production/DifferentialSnapshots:/Users/macbookdata/IdeaProjects/DifferentialSnapshots/lib/guava-17.0.jar:/Users/macbookdata/IdeaProjects/DifferentialSnapshots/lib/hamcrest-core-1.3.jar:/Users/macbookdata/IdeaProjects/DifferentialSnapshots/lib/junit-4.11.jar:/Users/macbookdata/IdeaProjects/DifferentialSnapshots/lib/commons-io-2.4/commons-io-2.4.jar:/Users/macbookdata/IdeaProjects/DifferentialSnapshots/lib/commons-io-2.4/commons-io-2.4-tests.jar:/Users/macbookdata/IdeaProjects/DifferentialSnapshots/lib/commons-io-2.4/commons-io-2.4-javadoc.jar:/Users/macbookdata/IdeaProjects/DifferentialSnapshots/lib/commons-io-2.4/commons-io-2.4-sources.jar:/Users/macbookdata/IdeaProjects/DifferentialSnapshots/lib/commons-io-2.4/commons-io-2.4-test-sources.jar:/Applications/IntelliJ IDEA 13 CE.app/lib/idea_rt.jar" com.intellij.rt.execution.application.AppMain concarne.Main ./data
//        Snapshot 1 size: 1000000 tuples
//        Snapshot 2 size: 979956 tuples
//        Snapshot 3 size: 960364 tuples
//        Snapshot 4 size: 980320 tuples
//        Snapshot 5 size: 960384 tuples
//
//        timeRead: 2654 ms
//        timeParse (IDs): 20854 ms
//        timeHash: 1639 ms
//
//        Diff size: 151415 ./data/R3.csv ./data/R5.csv (5990 ms)
//        Diff size: 78574 ./data/R2.csv ./data/R3.csv (1476 ms)
//        Diff size: 97753 ./data/R2.csv (via  ./data/R3.csv) ./data/R5.csv (217 ms)
//        Diff size: 224951 ./data/R3.csv ./data/R4.csv (992 ms)
//        Diff size: 174152 ./data/R2.csv (via  ./data/R3.csv) ./data/R4.csv (203 ms)
//        Diff size: 246521 ./data/R4.csv (via ./data/R2.csv (via  ./data/R3.csv) ) ./data/R5.csv (82 ms)
//        Diff size: 155360 ./data/R1.csv ./data/R3.csv (1413 ms)
//        Diff size: 99672 ./data/R1.csv (via  ./data/R3.csv) ./data/R2.csv (109 ms)
//        Diff size: 175380 ./data/R1.csv (via  (via  ./data/R3.csv) ./data/R2.csv) ./data/R5.csv (52 ms)
//        Diff size: 119386 ./data/R1.csv (via  (via  ./data/R3.csv) ./data/R2.csv) ./data/R4.csv (123 ms)
//        R1,R1,0,0,0
//        R1,R2,20050,40094,19895
//        R1,R3,38849,78485,38026
//        R1,R4,19888,39568,20247
//        R1,R5,38847,78463,37631
//        R2,R1,40094,20050,19895
//        R2,R2,0,0,0
//        R2,R3,19634,39226,19714
//        R2,R4,58346,57982,38191
//        R2,R5,19603,39175,19341
//        R3,R1,78485,38849,38026
//        R3,R2,39226,19634,19714
//        R3,R3,0,0,0
//        R3,R4,95233,75277,54441
//        R3,R5,57179,57159,37077
//        R4,R1,39568,19888,20247
//        R4,R2,57982,58346,38191
//        R4,R3,75277,95233,54441
//        R4,R4,0,0,0
//        R4,R5,75272,95208,54095
//        R5,R1,78463,38847,37631
//        R5,R2,39175,19603,19341
//        R5,R3,57159,57179,37077
//        R5,R4,95208,75272,54095
//        R5,R5,0,0,0
//
//        timeParsed(values): 8146 ms
//        Total time elapsed: 35826
