package concarne;

import com.google.common.base.Joiner;

import java.util.Arrays;
import java.util.Random;

/**
 * Created by macbookdata on 23.05.14.
 */
public class ColumnValues {

//    double[] values;
//    String[] columnStrings;
//    boolean hashed = false;
//    String normString;

    byte[][] rawData = new byte[4][];
    int[] hashes;

    public static long timeParsed = 0;
    static final int maxRandomValue = 100000;

//    public ColumnValues(String[] valueStrings){
//
//        columnStrings = valueStrings;
//
//    }

    public ColumnValues(byte[][] valueBytes) {
        System.arraycopy(valueBytes,1,rawData,0,4); // use last 4 entries

        for (int i = 0; i < 4; i++) {
            rawData[i] = Arrays.copyOf(valueBytes[i+1], valueBytes[i+1].length);
        }
        hashes = new int[rawData.length];
        for (int i = 0; i < rawData.length; i++) {
            hashes[i] = Arrays.hashCode(rawData[i]);
        }
    }

    public ColumnValues(String... strings) {
        for (int i = 0; i < strings.length; i++) {
            rawData[i] = Arrays.copyOf(strings[i].getBytes(), Snapshot.COLUMN_LENGTH);
        }
        hashes = new int[rawData.length];
        for (int i = 0; i < rawData.length; i++) {
            hashes[i] = Arrays.hashCode(rawData[i]);
        }
    }

//    protected void parse(){
//
//        long before = System.currentTimeMillis();
//
//        StringBuilder sb = new StringBuilder();
//        for (int i = 0; i < columnStrings.length; i++) {
//            sb.append(columnStrings[i]);
//            sb.append("\n");
//        }
//        normString = sb.toString();
//        valuesHash = normString.hashCode();
//
//        timeParsed += System.currentTimeMillis()-before;
////        long start = System.currentTimeMillis();
////        try {
////            values = new double[4];
////            for (int j = 0; j < 4; j++) {
////
////                values[j] = Double.parseDouble(columnStrings[j]);
//////                BigDecimal mustValue = new BigDecimal(columnStrings[j+1]);
//////                if(mustValue.compareTo() != 0){
//////                    throw new NumberFormatException(String.format("Wrongly hashed. Should be %s is %s",mustValue,attributes[j]));
//////                }
////                String test = (""+values[j]).endsWith(".0") ? (""+values[j]).replace(".0", "") : ""+values[j];
////                if(! columnStrings[j].equals(test)) throw new NumberFormatException("Wrongly hashed. "+columnStrings[j] + " as "+test);
////            }
////        } catch (NumberFormatException e){
//////            e.printStackTrace();
////            values = null;
////        }
////        timeParsed += System.currentTimeMillis() - start;
//        hashed = true;
//    }

    @Override
    public boolean equals(Object o) {
        ColumnValues other = (ColumnValues) o;

//        if(!hashed)
//            parse();
//        if(!other.hashed)
//            other.parse();
//
////        if(other.valuesHash != this.valuesHash){
////            System.out.println(this);
////            System.out.println();
////            System.out.println(other);
////        }
//

        if(Arrays.equals(other.hashes,this.hashes) && ! Arrays.deepEquals(this.rawData, other.rawData)){
            throw new ArithmeticException(String.format("Same hash %s for different values: %s    and    %s", Arrays.toString(hashes), this.toString(), other.toString()));
        }
        return Arrays.equals(other.hashes,this.hashes);
//        if(!other.hashed)
//            other.parse();
//        if(values == null || other.values == null){
//            for (int i = 0; i < 4; i++) {
//                if (!columnStrings[i].equals(other.columnStrings[i])) return false;
//            }
//        }else{
//            if (!Arrays.equals(values, other.values)) return false;
////            for (int i = 0; i < 4; i++) {
////                if (Math.abs(attributes[i] - other.attributes[i]) > 1e-2) return false;
////            }
//        }
//        return true;

    }

//    @Override
//    public int hashCode() {
//        return hash;
////        int result = values != null ? Arrays.hashCode(values) : 0;
////        result = 31 * result + (columnStrings != null ? Arrays.hashCode(columnStrings) : 0);
////        return result;
//    }

    /**
     * Test data constructor.
     */
    public static ColumnValues random(){
        Random random = new Random();
        byte[][] byteValues = new byte[4][Snapshot.COLUMN_LENGTH];
        for (int i = 0; i < byteValues.length; i++) {
            random.nextBytes(byteValues[i]);
        }
        return new ColumnValues(byteValues);
    }

    @Override
    public String toString() {
        String result = "";
        for (int i = 0; i < rawData.length; i++) {
            result += new String(rawData[i]) + ";";
        }
        return result;
//        if(!hashed)parse();
//        return normString;
    }
}
