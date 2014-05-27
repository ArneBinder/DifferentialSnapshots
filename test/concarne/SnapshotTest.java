package concarne;

import com.sun.xml.internal.fastinfoset.util.CharArray;
import org.apache.commons.io.FileUtils;
import org.junit.Test;

import java.io.*;
import java.nio.CharBuffer;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

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

}