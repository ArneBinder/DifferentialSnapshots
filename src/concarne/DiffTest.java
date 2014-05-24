package concarne;

import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

import static org.junit.Assert.*;

public class DiffTest {

    @Test
    public void transitiveConstructor() throws IOException {

        Snapshot a = new Snapshot("data/R1.csv");
        Snapshot b = new Snapshot("data/R2.csv");
        Snapshot c = new Snapshot("data/R3.csv");

        Diff expectedAC = new Diff(a,c);
        Diff ab = new Diff(a,b);
        Diff bc = new Diff(b,c);
        Diff transitive = new Diff(a,c,ab,bc);

        System.out.println(expectedAC.getCounts(false));
        System.out.println(transitive.getCounts(false));

    }

    @Test public void transitiveNaiveOrder() throws IOException {

        long before = System.currentTimeMillis();

        int A = 0, B = 1, C = 2, D = 3, E = 4;
        Snapshot[] snapshots = new Snapshot[]{
            new Snapshot("data/R1.csv"),
            new Snapshot("data/R2.csv"),
            new Snapshot("data/R3.csv"),
            new Snapshot("data/R4.csv"),
            new Snapshot("data/R5.csv")
        };


        Diff[][] anchor = new Diff[5][5];
        anchor[0] = new Diff[]{
                new Diff(),
                new Diff(snapshots[A],snapshots[B]),
                new Diff(snapshots[A],snapshots[C]),
                new Diff(snapshots[A],snapshots[D]),
                new Diff(snapshots[A],snapshots[E]),
        };

        for (int i = B; i < E; i++) {
            //anchor[i] = new Diff[5];
            //anchor[i]
            for (int j = i+1; j <= E; j++) {
                //System.out.println(String.format("calc Diff %s/%s with snap ",i+1,j+1));
                System.out.println("calc Diff "+i+"/"+j+" with snap " +i+" and snap "+j + " over Diff "+i+"/0 and Diff 0/"+j);
                anchor[i][j] = new Diff(snapshots[i],snapshots[j], anchor[A][i],anchor[A][j], true, false);
                //System.out.println(Arrays.toString(anchor[i][j].countOps()));
            }
        }

        for (int i = 0; i < 5; i++) {
            for (int j = 0; j < 5; j++) {
                if(i!=j)
                    if(i>j)
                        System.out.println("R"+(i+1)+",R"+(j+1)+","+anchor[j][i].getCounts(true));
                    else
                        System.out.println("R"+(i+1)+",R"+(j+1)+","+anchor[i][j].getCounts(false));
                else
                    System.out.println("R"+(i+1)+",R"+(j+1)+",0,0,0");
            }
        }

        System.out.println("Total time: "+(System.currentTimeMillis()-before));

    }

}