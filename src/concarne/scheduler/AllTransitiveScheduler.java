package concarne.scheduler;

import concarne.Diff;
import concarne.Snapshot;

/**
 * Created by macbookdata on 27.05.14.
 */
public class AllTransitiveScheduler extends Scheduler{

    public AllTransitiveScheduler(Snapshot[] snapshots){
        super(snapshots);
    }

    public void execute(){

        int A = 0, B = 1, C = 2, D = 3, E = 4;
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

    }
}
