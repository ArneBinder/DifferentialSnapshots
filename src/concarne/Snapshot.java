package concarne;

import com.google.common.base.Splitter;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * Created by macbookdata on 23.05.14.
 */
public class Snapshot {

    HashMap<Identifier, ColumnValues> tuples = new HashMap<>();

    static String[] allowedDelimitors = new String[]{",", ";", " ", ":"};

    String path;

    public Snapshot(String path) throws IOException {

        this.path = path.substring(7,9);

        List<String> lines = FileUtils.readLines(new File(path));
        String firstLine = lines.get(0);
        lines.remove(0);

        boolean escaping = lines.get(1).startsWith("\"");
        String delimitor = null;

        for(String possibleDelim : allowedDelimitors){

            delimitor = possibleDelim;
            if(firstLine.contains(possibleDelim))
                break;

        }

        for(String line : lines){

            String[] parts;
            if(escaping){
//                parts = Splitter.on(String.format("\"%s\"", delimitor)).split(line.substring(1,line.length()-1));
                parts = line.substring(1, line.length()-1).split("\"" + delimitor + "\"");
            } else {
//                parts = Splitter.on(delimitor).split(line);
                parts = line.split(delimitor);
            }

            Identifier id = new Identifier(parts[0]);
            ColumnValues columnValues = new ColumnValues(parts[1],parts[2],parts[3],parts[4]);
            tuples.put(id, columnValues);

        }

    }

    public Iterator<Map.Entry<Identifier,ColumnValues> > getIterator(){
        return tuples.entrySet().iterator();
    }

}
