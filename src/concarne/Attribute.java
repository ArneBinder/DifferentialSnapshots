package concarne;

/**
 * Created by macbookdata on 23.05.14.
 */
public class Attribute {

        double value = Double.MIN_VALUE;
        String idString;

        public Attribute(String string){

            try {
                value = Double.parseDouble(string);
                if(! string.equals(""+ value)) throw new NumberFormatException("Wrongly parsed.");
            } catch (NumberFormatException e){
                e.printStackTrace();
                this.idString = string;
            }

        }

        @Override
        public boolean equals(Object o) {
            Identifier other = (Identifier) o;
            // compare ids as string if parse error
            if(value == Double.MIN_VALUE || other.id == Double.MIN_VALUE){
                if ( ! idString.equals(other.idString)) return false;
            } else {
                if (value != other.id) return false;
            }

            return true;
        }


}
