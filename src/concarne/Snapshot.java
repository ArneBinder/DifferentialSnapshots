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

    public ConcurrentHashMap<Identifier, ColumnValues> tuples = new ConcurrentHashMap<>();

    private byte[][] brokenLines; //blockID * 2 + (beginOfBlock ? 0 : 1)

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

        int expectedBlockCount = (int) (new File(path).length() / (long) bufferSize);

        long before = System.currentTimeMillis();

        BufferedInputStream f = new BufferedInputStream(new FileInputStream(path));
        byte[] buffer = new byte[bufferSize];

        // half line endings and half line starts
        brokenLines = new byte[expectedBlockCount*2][];//<Integer, byte[]>(expectedBlockCount);
        delimiter = 0;

        byte[] brokenLine;
        int nRead, blockId = 0, bufferPos;

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

            // read another block
            } else {
                // read until first new line in block
                for (; bufferPos < bufferSize; bufferPos++) {
                    if (buffer[bufferPos]=='\n'){
                        brokenLine = new byte[bufferPos-lineFeed];
                        System.arraycopy(buffer, 0, brokenLine, 0, bufferPos-lineFeed);

                        // save part before new line (second half of a broken line)
                        synchronized (brokenLines){
                            brokenLines[blockId] = brokenLine;
                            //System.out.println(Arrays.toString(brokenLine));
                        }
                        break;
                    }
                }
            }

            assert (buffer[bufferPos]=='\n');
            //System.out.println("bufferPos: "+bufferPos);

            parseBLock(buffer, blockId, bufferPos+1);
            blockId++;

        }

    }

    /** Maximum number of characters per column */
    final static int COLUMN_LENGTH = 30;

    /**
     * Parses 1 column value for each line of the block
     * store last line end in broken lines
     * @param block byte data of the block
     * @param blockId index of the block (to write broken line to correct position)
     * @param offset first character offset of first complete line
     */
    private void parseBLock(byte[] block, int blockId, int offset){


        byte[][] valueBytes = new byte[5][COLUMN_LENGTH];
        int columnIndex = 0;    // which column are we reading bytes for?
        int byteIdx = 0;        // where in the byte array for the column goes the next byte

        boolean readEscape = false; // whether the current characters are escaped
        boolean lineComplete = false;

        byte[] workingBuffer = block;

        for (int i = offset; i < workingBuffer.length; i++) {

            if(escaping && block[i] == escapeVal){
                readEscape = !readEscape; // toggle within escaped char sequence
            } else if(! readEscape && block[i] == delimiter){
                columnIndex++;
                byteIdx = 0;
            } else if(block[i] == '\n'){
//               tuples.put(new Identifier(valueBytes[0]), new ColumnValues(valueBytes));
               lineComplete = true;
               columnIndex = 0;
               byteIdx = 0;

            // read byte into column bytes
            } else if(block[i] != '\r'){
                valueBytes[columnIndex][byteIdx] = block[i];
                byteIdx++;
                lineComplete = false;
            }

            // start grabbing data from block after having finished the broken line data
            if(workingBuffer != brokenLines[blockId] && i == workingBuffer.length-1){
                workingBuffer = brokenLines[blockId];
                i = -1; // because i is incremented after each iteration
            }

        }

        // check if line was broken by blocks
        if(! lineComplete){



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
            ColumnValues columnValues = new ColumnValues(new String[]{parts[1],parts[2],parts[3],parts[4]});
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
        bw.write(buildLine(new Identifier("PK"), new ColumnValues(new String[]{"lat1", "lon1", "lat2", "lon2"})));
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
            for (String col : columnValues.columnStrings) {
                line += (char) delimiter + col;
            }
        }else{
            line = "\""+identifier.toString()+"\"";
            for (String col : columnValues.columnStrings) {
                line += (char) delimiter + "\""+col+"\"";
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
                    Identifier newIdentifier = new Identifier(random.nextInt());
                    for (int i = 0; i < 10; i++) {
                        if(newIdentifier.id >= 0 && ! snapshot.tuples.containsKey(newIdentifier)){
                            tuples.put(newIdentifier,ColumnValues.random());
                            added = true;
                            break;
                        }
                        newIdentifier = new Identifier(random.nextInt() + snapshot.tuples.size());
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

}
