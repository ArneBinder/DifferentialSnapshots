package concarne;

import java.util.Arrays;
import java.util.Random;

/**
 * Created by macbookdata on 23.05.14.
 */
class Identifier {

//    long id = Long.MIN_VALUE;
//    String idString;

    byte[] rawData;
    int hash;

    public Identifier(String string){

        rawData = Arrays.copyOf(string.getBytes(), Snapshot.COLUMN_LENGTH);
        this.hash = Arrays.hashCode(rawData);

    }

//    public Identifier(long id){
//        this.id = id;
//        this.idString = ""+id;
//    }

    public Identifier(byte[] valueByte) {
        this.rawData = Arrays.copyOf(valueByte,valueByte.length);
        this.hash = Arrays.hashCode(rawData);
    }

    @Override
    public boolean equals(Object o) {

        Identifier other = (Identifier) o;

        if(hash == other.hash && ! Arrays.equals(rawData, other.rawData))
            throw new ArithmeticException("Hash collision: "+hash+" "+this.toString()+" other: "+other);

        return hash == other.hash;



//        // compare ids as string if parse error
//        if(id == Long.MIN_VALUE || other.id == Long.MIN_VALUE){
//            if ( ! idString.equals(other.idString)) return false;
//        } else {
//            if (id != other.id) return false;
//        }
//        return true;
    }

    @Override
    public int hashCode() {
        return hash;
        //return Arrays.hashCode(rawData);
//        int result = (int) (id ^ (id >>> 32));
//        result = 31 * result + (idString != null ? idString.hashCode() : 0);
//        return result;
    }

    @Override
    public String toString(){
        return new String(rawData);
//        if(idString!=null)
//            return idString;
//        return ""+id;
    }

    public static Identifier random() {
        byte[] randoms = new byte[Snapshot.COLUMN_LENGTH];
        new Random().nextBytes(randoms);
        return new Identifier(randoms);
    }
}
