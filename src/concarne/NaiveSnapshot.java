package concarne;

import java.util.*;

/**
 * Created by macbookdata on 23.05.14.
 */
public class NaiveSnapshot {

    static List<Operation> compare(Snapshot a, Snapshot b){

        ArrayList<Operation> operations = new ArrayList<>();

        for(Iterator<Map.Entry<Identifier,ColumnValues>> itA=a.getIterator(); itA.hasNext();  ){

            Map.Entry<Identifier,ColumnValues> t1 = itA.next();
            Operation operationWithT1 = new Operation(t1.getKey(), Diff.DEL);

            for(Iterator<Map.Entry<Identifier,ColumnValues>> itB = b.getIterator(); itB.hasNext(); ){

                Map.Entry<Identifier,ColumnValues> t2 = itB.next();

                if(t1.getKey().equals(t2.getKey())){


                    if(! t1.getValue().equals(t2.getValue())) {
                        operationWithT1 = new Operation(t1.getKey(), Diff.SUB);
                    }
                    else {
                        operationWithT1 = null; // identical in both
                    }
                    itB.remove();
                    break;

                }

            }

            if(operationWithT1 != null)
                operations.add(operationWithT1);

        }

        for(Iterator<Map.Entry<Identifier,ColumnValues>> itB = b.getIterator(); itB.hasNext(); ){
            operations.add(new Operation(itB.next().getKey(), Diff.INS));
        }

        return operations;
    }

}
