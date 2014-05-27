package concarne;

import org.apache.commons.io.FileUtils;
import org.junit.Test;

import java.io.*;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SnapshotTest {

    @Test
    public void testReadFileFileUtils() throws IOException {

        // 100 mb input
        String filename = "data/mediumSize/R2.csv";

        long before = System.currentTimeMillis();

        List<String> result = FileUtils.readLines(new File((filename)));
        System.out.println(result.get(0));

        System.out.println("FileUtils: ");
        System.out.println(System.currentTimeMillis()-before);

    }

    @Test
    public void testReadFileBufferStream() throws IOException {

        // 100 mb input
//        String filename = "data/mediumSize/R2.csv";
        String filename = "data/R2.csv";
        int bufferSize = 1;

        long before = System.currentTimeMillis();

        BufferedInputStream f = new BufferedInputStream(new FileInputStream(filename));
        byte[] buffer = new byte[bufferSize];


        long checkSum = 0L;
        int nRead, buffersFilled = 0;
        while ( (nRead=f.read( buffer, 0, bufferSize )) != -1 ) {
            buffersFilled++;
            break;
//            for ( int i=0; i<nRead; i++ ){
//
//            }
        }

        System.out.println(String.format("nRead %s buffersFilled %s last buffer content:\n%s",nRead,buffersFilled, Arrays.toString(buffer)));

        System.out.println((char)buffer[0]);
        System.out.println("BufferStream: ");
        System.out.println(System.currentTimeMillis()-before);


    }

    @Test public void byteToString(){

        String s = new String(new byte[]{52,53});
        System.out.println(s);
        s.hashCode();

    }

    @Test public void createImmutableMap() throws IOException {

        System.out.println(Runtime.getRuntime().availableProcessors());

        Snapshot s = new Snapshot("data/R1.csv");
        System.out.println("file size (bytes) "+(new File("data/R1.csv").length()) );
        long before = System.currentTimeMillis();

//        ImmutableMap<Identifier, ColumnValues> tup = ImmutableMap.copyOf(s.tuples);
//        ImmutableList<Identifier> keys = tup.keySet().asList();
//        System.out.print("Immutable conversion time: ");
//        System.out.println(System.currentTimeMillis()-before);


        before = System.currentTimeMillis();

        Set<Map.Entry<Identifier, ColumnValues>> entrySet = s.tuples.entrySet();
        Map.Entry<Identifier,ColumnValues>[] entries = entrySet.toArray(new Map.Entry[entrySet.size()]);

//        for (int i = 0; i < 10; i++) {
//            System.out.println(entries[i].getKey()+" "+entries[i].getValue());
//        }
//        Set<Identifier> keySet = s.tuples.keySet();
//        Identifier[] keysArray = keySet.toArray(new Identifier[keySet.size()]);
        System.out.print("Needed time: ");
        System.out.println(System.currentTimeMillis()-before);

    }

    private List<String> resultsForSample(){

        return Arrays.asList(
        "R1,R1,0,0,0",
        "R1,R2,295,245,911",
        "R1,R3,299,239,948",
        "R1,R4,298,278,968",
        "R1,R5,297,317,955",
        "R2,R1,245,295,911",
        "R2,R2,0,0,0",
        "R2,R3,249,239,929",
        "R2,R4,250,280,944",
        "R2,R5,249,319,945",
        "R3,R1,239,299,948",
        "R3,R2,239,249,929",
        "R3,R3,0,0,0",
        "R3,R4,237,277,991",
        "R3,R5,236,316,983",
        "R4,R1,278,298,968",
        "R4,R2,280,250,944",
        "R4,R3,277,237,991",
        "R4,R4,0,0,0",
        "R4,R5,277,317,1009",
        "R5,R1,317,297,955",
        "R5,R2,319,249,945",
        "R5,R3,316,236,983",
        "R5,R4,317,277,1009",
        "R5,R5,0,0,0");

    }

    @Test public void iterateSwitchArray(){

        byte[] arr1 = new byte[4];
        byte[] arr2 = new byte[8];

        for (int i = 0; i < arr1.length; i++) {

            System.out.println(i);
            if( arr1 != arr2 && i == arr1.length-1){
                arr1 = arr2;
                i = -1;
            }

        }

    }


    @Test public void readFirstBlock() throws IOException {
        Snapshot snapshot = new Snapshot("data/R3.csv",1024);
    }

}