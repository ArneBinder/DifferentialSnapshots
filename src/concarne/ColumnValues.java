package concarne;

import java.util.Arrays;

/**
 * Created by macbookdata on 23.05.14.
 */
public class ColumnValues {

    double[] values;
    String[] columnStrings;
    boolean parsed = false;

    public ColumnValues(String... valueStrings){

        columnStrings = valueStrings;

    }

    protected void parse(){

        try {
            values = new double[4];
            for (int j = 0; j < 4; j++) {

                values[j] = Double.parseDouble(columnStrings[j]);
//                BigDecimal mustValue = new BigDecimal(columnStrings[j+1]);
//                if(mustValue.compareTo() != 0){
//                    throw new NumberFormatException(String.format("Wrongly parsed. Should be %s is %s",mustValue,attributes[j]));
//                }
                String test = (""+values[j]).endsWith(".0") ? (""+values[j]).replace(".0", "") : ""+values[j];
                if(! columnStrings[j].equals(test)) throw new NumberFormatException("Wrongly parsed. "+columnStrings[j] + " as "+test);
            }
        } catch (NumberFormatException e){
//            e.printStackTrace();
            values = null;
        }

        parsed = true;
    }

    @Override
    public boolean equals(Object o) {
        if(o==null)
            return false;
        ColumnValues other = (ColumnValues) o;
        if(!parsed)
            parse();
        if(!other.parsed)
            other.parse();
        if(values == null || other.values == null){
            for (int i = 0; i < 4; i++) {
                if (!columnStrings[i].equals(other.columnStrings[i])) return false;
            }
        }else{
            if (!Arrays.equals(values, other.values)) return false;
//            for (int i = 0; i < 4; i++) {
//                if (Math.abs(attributes[i] - other.attributes[i]) > 1e-2) return false;
//            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = values != null ? Arrays.hashCode(values) : 0;
        result = 31 * result + (columnStrings != null ? Arrays.hashCode(columnStrings) : 0);
        return result;
    }
}
