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

    private Map<Integer, byte[]> brokenLines; //blockID * 2 + (beginOfBlock ? 0 : 1)

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

    public Snapshot(String path, int bufferSize, int expectedBlockCount) throws IOException {


        long before = System.currentTimeMillis();

        BufferedInputStream f = new BufferedInputStream(new FileInputStream(path));
        byte[] buffer = new byte[bufferSize];


        brokenLines = new ConcurrentHashMap<Integer, byte[]>(expectedBlockCount);
        delimiter = 0;

        byte[] brokenLine;
        long checkSum = 0L;
        int nRead, blockId = 0, bufferPos;
        while ( (nRead=f.read( buffer, 0, bufferSize )) != -1 ) {
            bufferPos=0;

            //read first block
            if(delimiter == 0){
                if(buffer[0]==escapeVal) {
                    escaping = true;
                    delimiter = buffer[4];
                    bufferPos = 32;
                }else{
                    escaping = false;
                    delimiter = buffer[2];
                    bufferPos = 22;
                }
                if(buffer[bufferPos]=='\r') {
                    lineFeed = 1;
                    bufferPos++;
                }
                else
                    lineFeed = 0;
                System.out.println("escaping: "+escaping);
                System.out.println("delimiter: "+(char)delimiter);

            // read another block
            }else {
                for (; bufferPos < bufferSize; bufferPos++) {
                    if (buffer[bufferPos]=='\n'){
                        brokenLine = new byte[bufferPos];
                        System.arraycopy(buffer,0,brokenLine,0,bufferPos);
                        synchronized (brokenLines){
                            brokenLines.put(blockId*2,brokenLine);
                            //System.out.println(Arrays.toString(brokenLine));
                        }
                        break;
                    }
                }
            }

            assert (buffer[bufferPos]=='\n');
            //System.out.println("bufferPos: "+bufferPos);

            blockId++;

        }

    }

    private void parseBLock(byte[] block, int offset){
        //if(offset==0)
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
        if (delimiter ==0){
            System.out.println("WARNING: no delimiter found. Take default: \";\"");
            delimiter = ';';
        }

        long tstamp_startParsing = System.currentTimeMillis();
        long tstamp_startHashing;
        for(String line : lines){

            String[] parts;
            if(escaping){
//                parts = Splitter.on(String.format("\"%s\"", delimiter)).split(line.substring(1,line.length()-1));
                parts = line.substring(1, line.length()-1).split("\"" + delimiter + "\"");
            } else {
//                parts = Splitter.on(delimiter).split(line);
                parts = line.split(""+ delimiter);
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
                line += delimiter + col;
            }
        }else{
            line = "\""+identifier.toString()+"\"";
            for (String col : columnValues.columnStrings) {
                line += delimiter + "\""+col+"\"";
            }
        }
        //System.out.println(line);
        return line+"\n";
    }


    /**
     * Constructor for generating test data.
     * @param snapshot
     * @param opProbabilities
     */
    public Snapshot(Snapshot snapshot, float[] opProbabilities){
        Random random = new Random();
        float opRandom;
        boolean added;
        for(Map.Entry<Identifier,ColumnValues> entry: snapshot.tuples.entrySet()){
            opRandom = random.nextFloat();
            byte op;
            for (op = 0; op < 5; op++) {
                if(opRandom <= opProbabilities[op]){

                    break;
                }
                opRandom -= opProbabilities[op];
            }
            added = false;
            //System.out.println(op);
            switch(op){
                case Diff.INS:
                    Identifier newIdentifier = new Identifier(random.nextInt());
                    for (int i = 0; i < 10; i++) {
                        if(newIdentifier.id >= 0 && snapshot.tuples.containsKey(newIdentifier)){
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
