package concarne;

import com.google.common.base.Splitter;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by macbookdata on 23.05.14.
 */
public class Tuple {

    Identifier id;
    ColumnValues columnValues;

    String[] columnStrings;

    public Tuple(String line, String delimitor, boolean escaping){

        columnStrings = new String[5];

        Iterable<String> parts;
        if(escaping){
            parts = Splitter.on(String.format("\"%s\"", delimitor)).split(line.substring(1,line.length()-1));
        } else {
            parts = Splitter.on(delimitor).split(line);
        }

        int i = 0;
        for (String s : parts){
            columnStrings[i++] = s;
        }

        id = new Identifier(columnStrings[0]);

        columnValues = new ColumnValues(columnStrings[1],columnStrings[2],columnStrings[3],columnStrings[4]);

    }

    public boolean equalsAttributes(Tuple other){
//        if(!Arrays.equals(columnStrings, other.columnStrings)) return false;
//        if(attributes == null || other.attributes == null){
//            for (int i = 1; i < 5; i++) {
//                if (!columnStrings[i].equals(other.columnStrings[i])) return false;
//            }
//        }else{
//            if (!Arrays.equals(attributes, other.attributes)) return false;
////            for (int i = 0; i < 4; i++) {
////                if (Math.abs(attributes[i] - other.attributes[i]) > 1e-2) return false;
////            }
//        }
        return true;
    }
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Tuple other = (Tuple) o;

        return id.equals(other.id) && equalsAttributes(other);
    }

    @Override
    public int hashCode() {
        int result = id.hashCode();
        result = 31 * result + (columnStrings != null ? Arrays.hashCode(columnStrings) : 0);
//        result = 31 * result + (attributes != null ? Arrays.hashCode(attributes) : 0);
        return result;
    }

    @Override
    public String toString() {

//        try {
////            return String.format("%s (%s, %s, %s, %s)", id, attributes[0], attributes[1], attributes[2], attributes[3]);
//        } catch (NullPointerException e){
//            e.printStackTrace();
//            return String.format("%s (%s, %s, %s, %s)", columnStrings[0], columnStrings[1], columnStrings[2], columnStrings[3], columnStrings[4]);
//        }
        return "";
    }
}
