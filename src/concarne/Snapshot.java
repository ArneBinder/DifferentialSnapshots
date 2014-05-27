package concarne;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.io.FileUtils;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by macbookdata on 23.05.14.
 */
public class Snapshot {

    public HashMap<Identifier, ColumnValues> tuples = new HashMap<>();

    boolean escaping = false;
    byte delimiter = (byte)';';
    byte lineFeed = 0;
    static final byte escapeVal = (byte)'"';

    static final byte[] allowedDelimiters = new byte[]{(byte)',',(byte)';', (byte)' ', (byte)':'};

    static final float[] delimiterProbabilites = new float[]{0.2f,0.6f,0.2f,0.2f};
    static final float escapeProbabillity = 0.2f;

    //stats
    public static long timeParse = 0;
    public static long timeRead = 0;
    public static long timeHash = 0;

    String path;

    public Snapshot(String path, int bufferSize) throws IOException {

        this.path = path;
        int expectedBlockCount = (int) (new File(path).length() / (long) bufferSize);

        long before = System.currentTimeMillis();

        BufferedInputStream f = new BufferedInputStream(new FileInputStream(path));
        byte[] buffer = new byte[bufferSize];

        // half line endings and half line starts
        //brokenLines = new byte[expectedBlockCount*2][];//<Integer, byte[]>(expectedBlockCount);
        byte[] lastBlock = new byte[bufferSize];
        int lastOffset = 0;
        delimiter = 0;

        byte[] brokenLine = new byte[0];
        int nRead, bufferPos;

        // read file block wise
        while ( (nRead=f.read( buffer, 0, bufferSize )) != -1 ) {

            // cursor for current block
            bufferPos=0;

            //read first block: determine delimiter and escaping and line endings
            if(delimiter == 0){
                // if first character in file is " then escaping is on
                if(buffer[0]==escapeVal) {
                    escaping = true;
                    delimiter = buffer[4];  // "PK"<delim> is on position 4
                    bufferPos = 32;         // position cursor on new line character of first line
                }else{
                    escaping = false;
                    delimiter = buffer[2];
                    bufferPos = 22;
                }

                // determine whether lines end with \n or with \r\n
                if(buffer[bufferPos]=='\r') {
                    lineFeed = 1;
                    bufferPos++;
                }
                else
                    lineFeed = 0;

                System.out.println("escaping: "+escaping);
                System.out.println("delimiter: "+(char)delimiter);

                if(nRead < bufferSize){
                    lastBlock = new byte[nRead];
                }

               // parseBlock(lastBlock, bufferPos+1, brokenLine);
                System.arraycopy(buffer, 0, lastBlock, 0, lastBlock.length);
                lastOffset = bufferPos;

            // read another block
            } else {
                // read until first new line in block
                for (; bufferPos < bufferSize; bufferPos++) {
                    if (buffer[bufferPos]=='\n'){
                        brokenLine = new byte[bufferPos+1-lineFeed];
//                        System.out.println(String.format(""));
                        System.arraycopy(buffer, 0, brokenLine, 0, bufferPos+1-lineFeed);

                        // save part before new line (second half of a broken line)
                        //synchronized (brokenLines){
                            //System.out.println(Arrays.toString(brokenLine));
                        //}
                        break;
                    }
                }
                assert (buffer[bufferPos]=='\n');
                parseBlock(lastBlock, lastOffset+1, brokenLine);

                if(nRead < bufferSize){
                    lastBlock = new byte[nRead];
                }

                System.arraycopy(buffer, 0, lastBlock, 0, lastBlock.length);

                lastOffset = bufferPos;


            }

        } // end while file has more bytes

        // process last block of file
        parseBlock(lastBlock, lastOffset+1, new byte[0]);

    }

    /** Maximum number of characters per column */
    final static int COLUMN_LENGTH = 30;

    /**
     * Parses 1 column value for each line of the block
     * store last line end in broken lines
     * @param block byte data of the block
     * @param offset first character offset of first complete line
     */
    private void parseBlock(byte[] block, int offset, byte[] brokenLine){

        int blockSize = block.length;

 //       System.out.println("Process block: "+new String(block) + "\nfrom offset "+offset);
//        System.out.println("Broken line: "+new String(brokenLine));

        byte[][] valueBytes = new byte[5][COLUMN_LENGTH];
        int columnIndex = 0;    // which column are we reading bytes for?
        int byteIdx = 0;        // where in the byte array for the column goes the next byte

        boolean readEscape = false; // whether the current characters are escaped

        byte[] workingBuffer = block;
        int size = blockSize;

        for (int i = offset; i < size; i++) {

//            System.out.print(new String(new byte[]{workingBuffer[i]}));

            if(escaping && workingBuffer[i] == escapeVal){
                readEscape = !readEscape; // toggle within escaped char sequence
            } else if(! readEscape && workingBuffer[i] == delimiter){
                columnIndex++;
                byteIdx = 0;
            } else if(workingBuffer[i] == '\n'){

               tuples.put(new Identifier(valueBytes[0]), new ColumnValues(valueBytes));
               columnIndex = 0;
               byteIdx = 0;

            // read byte into column bytes
            } else if(workingBuffer[i] != '\r'){
                if(byteIdx==30){
                    System.out.println("");
                }
                valueBytes[columnIndex][byteIdx] = workingBuffer[i];
                byteIdx++;
            }

            // start grabbing data from block after having finished the broken line data
            if(workingBuffer != brokenLine && i == size-1){
                workingBuffer = brokenLine;
                size = brokenLine.length;
                i = -1; // because i is incremented after each iteration
            }

        }




    }

    /**
     * Reads the data from disk, determines the delimiter and escaping and hashes all the tuples.
     * @param path
     * @throws IOException
     */
    public Snapshot(String path) throws IOException {

        this.path = path;

        long tstamp_start = System.currentTimeMillis();
        List<String> lines = FileUtils.readLines(new File(path));
        long tstamp_read = System.currentTimeMillis();
        timeRead+=tstamp_read-tstamp_start;
        //System.out.println("time_read: "+(tstamp_read-tstamp_start)+" ms");
        String firstLine = lines.get(0);
        lines.remove(0);

        escaping = lines.get(1).startsWith("\"");
        delimiter = 0;

        for(byte possibleDelim : allowedDelimiters){

            delimiter = possibleDelim;
            if(firstLine.contains(""+(char)possibleDelim))
                break;

        }
        if (delimiter == 0){
            System.out.println("WARNING: no delimiter found. Take default: \";\"");
            delimiter = (byte) ';';
        }

        long tstamp_startParsing = System.currentTimeMillis();
        long tstamp_startHashing;
        for(String line : lines){

            String[] parts;
            if(escaping){
//                parts = Splitter.on(String.format("\"%s\"", delimiter)).split(line.substring(1,line.length()-1));
                parts = line.substring(1, line.length()-1).split("\"" + (char)delimiter + "\"");
            } else {
//                parts = Splitter.on(delimiter).split(line);
                parts = line.split(""+(char) delimiter);
            }

            Identifier id = new Identifier(parts[0]);
            ColumnValues columnValues = new ColumnValues(parts[1],parts[2],parts[3],parts[4]);
            tstamp_startHashing = System.currentTimeMillis();
            tuples.put(id, columnValues);
            timeHash += (System.currentTimeMillis()-tstamp_startHashing);

        }
        long tstamp_finishParsing = System.currentTimeMillis();
        timeParse += tstamp_finishParsing-tstamp_startParsing;
        //System.out.println("time_parsing: "+(tstamp_finishParsing-tstamp_startParsing)+ " ms");

    }

    public Iterator<Map.Entry<Identifier,ColumnValues> > getIterator(){
        return tuples.entrySet().iterator();
    }

    public void writeSnapshot(String path) throws IOException {
        File file = new File(path);
        Random random = new Random();
        escaping = (random.nextFloat() <= escapeProbabillity);
        float delimitorRandom = random.nextFloat();
        for (int i = 0; i < 4; i++) {
            if(delimitorRandom <= delimiterProbabilites[i]){
                delimiter = allowedDelimiters[i];
                break;
            }
            delimitorRandom -= delimiterProbabilites[i];
        }
        BufferedWriter bw = new BufferedWriter(new FileWriter(file));
        bw.write(buildLine(new Identifier("PK"), new ColumnValues("lat1", "lon1", "lat2", "lon2")));
        for (Map.Entry<Identifier, ColumnValues> tuple : tuples.entrySet()) {
            bw.write(buildLine(tuple.getKey(), tuple.getValue()));
        }
        System.out.println(tuples.size());
        bw.close();
    }

    private String buildLine(Identifier identifier, ColumnValues columnValues){
        String line;
        if(!escaping) {
            line = identifier.toString();
            for (byte[] col : columnValues.rawData) {
                line += (char) delimiter + new String(col);
            }
        }else{
            line = "\""+identifier.toString()+"\"";
            for (byte[] col : columnValues.rawData) {
                line += (char) delimiter + "\""+ new String(col) + "\"";
            }
        }
        //System.out.println(line);
        return line+"\n";
    }


    /**
     * Constructor for generating test data.
     * @param snapshot
     * @param opProbabilities probability of each operation
     */
    public Snapshot(Snapshot snapshot, float[] opProbabilities){
        Random random = new Random();
        float opRandom;
        boolean added;
        for(Map.Entry<Identifier,ColumnValues> entry: snapshot.tuples.entrySet()){

            // choose a random operation according to distribution
            byte op;
            opRandom = random.nextFloat();
            for (op = 0; op < 5; op++) {
                if(opRandom <= opProbabilities[op]){

                    break;
                }
                opRandom -= opProbabilities[op];
            }

            added = false;

            // perform operation
            switch(op){
                case Diff.INS:
                    Identifier newIdentifier = Identifier.random();
                    for (int i = 0; i < 10; i++) {
                        if( ! snapshot.tuples.containsKey(newIdentifier)){
                            tuples.put(newIdentifier,ColumnValues.random());
                            added = true;
                            break;
                        }
                        newIdentifier = Identifier.random();
                    }
                    if(!added)
                        tuples.put(newIdentifier,ColumnValues.random());
                    break;
                case Diff.DEL:
                    //tuples.put(entry.getKey(),op);
                    break;
                case Diff.SUB:
                    ColumnValues newColumnValues = ColumnValues.random();
                    for (int i = 0; i < 10; i++) {
                       if(!newColumnValues.equals(entry.getValue())){
                           tuples.put(entry.getKey(),newColumnValues);
                           added = true;
                           break;
                       }
                       newColumnValues = ColumnValues.random();
                    }
                    if(!added)
                        tuples.put(entry.getKey(), newColumnValues);
                    break;
                case Diff.NOP:
                    tuples.put(entry.getKey(),entry.getValue());
                    break;
            }


        }
    }

    /**
     * Test data constructor.
     * @param size
     */
    public Snapshot(int size){
        for (int i = 0; i < size; i++) {
            tuples.put(new Identifier(""+i), ColumnValues.random());
        }

    }

    @Override
    public boolean equals(Object obj) {

        Snapshot other = (Snapshot) obj;
        if(tuples.size() != other.tuples.size()) return false;

        boolean equals = true;
        equals &= tuples.entrySet().containsAll(other.tuples.entrySet());
        equals &= other.tuples.entrySet().containsAll(tuples.entrySet());

        return equals;

    }
}
