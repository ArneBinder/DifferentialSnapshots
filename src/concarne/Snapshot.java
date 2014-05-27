package concarne;

import com.google.common.base.Splitter;
import org.apache.commons.io.FileUtils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

/**
 * Created by macbookdata on 23.05.14.
 */
public class Snapshot {

    public HashMap<Identifier, ColumnValues> tuples = new HashMap<>();

    private byte[][] brokenLines;

    boolean escaping = false;
    char delimitor = ';';

    static final char[] allowedDelimitors = new char[]{',', ';', ' ', ':'};
    static final float[] delimiterProbabilites = new float[]{0.2f,0.6f,0.2f,0.2f};
    static final float escapeProbabillity = 0.2f;

    //stats
    public static long timeParse = 0;
    public static long timeRead = 0;
    public static long timeHash = 0;

    String path;

    //public Snapshot(String path, int bufferSize, )

    /**
     * Reads the data from disk, determines the delimitor and escaping and hashes all the tuples.
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
        delimitor = 0;

        for(char possibleDelim : allowedDelimitors){

            delimitor = possibleDelim;
            if(firstLine.contains(""+possibleDelim))
                break;

        }
        if (delimitor==0){
            System.out.println("WARNING: no delimitor found. Take default: \";\"");
            delimitor = ';';
        }

        long tstamp_startParsing = System.currentTimeMillis();
        long tstamp_startHashing;
        for(String line : lines){

            String[] parts;
            if(escaping){
//                parts = Splitter.on(String.format("\"%s\"", delimitor)).split(line.substring(1,line.length()-1));
                parts = line.substring(1, line.length()-1).split("\"" + delimitor + "\"");
            } else {
//                parts = Splitter.on(delimitor).split(line);
                parts = line.split(""+delimitor);
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
                delimitor = allowedDelimitors[i];
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
                line += delimitor + col;
            }
        }else{
            line = "\""+identifier.toString()+"\"";
            for (String col : columnValues.columnStrings) {
                line += delimitor + "\""+col+"\"";
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
        for(Map.Entry<Identifier,ColumnValues> entry: snapshot.tuples.entrySet()){
            opRandom = random.nextFloat();
            byte op;
            for (op = 0; op < 5; op++) {
                if(opRandom <= opProbabilities[op]){

                    break;
                }
                opRandom -= opProbabilities[op];
            }
            //System.out.println(op);
            switch(op){
                case Diff.INS:
                    Identifier newIdentifier = new Identifier(random.nextInt());
                    while(newIdentifier.id < 0 || snapshot.tuples.containsKey(newIdentifier)){
                        newIdentifier = new Identifier(random.nextInt());
                    }
                    tuples.put(newIdentifier,ColumnValues.random());
                    break;
                case Diff.DEL:
                    //tuples.put(entry.getKey(),op);
                    break;
                case Diff.SUB:
                    ColumnValues newColumnValues = ColumnValues.random();
                    while(newColumnValues.equals(entry.getValue())){
                        newColumnValues = ColumnValues.random();
                    }
                    tuples.put(entry.getKey(),newColumnValues);
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
