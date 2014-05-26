package concarne;

/**
 * Created by macbookdata on 23.05.14.
 */
class Identifier {

    long id = Long.MIN_VALUE;
    String idString;

    public Identifier(String string){

        try {
            id = Long.parseLong(string);
            if(! string.equals(""+id)) throw new NumberFormatException("Wrongly parsed.");
        } catch (NumberFormatException e){
            //e.printStackTrace();
            this.idString = string;
        }

    }

    public Identifier(long id){
        this.id = id;
        this.idString = ""+id;
    }

    @Override
    public boolean equals(Object o) {
        Identifier other = (Identifier) o;
        // compare ids as string if parse error
        if(id == Long.MIN_VALUE || other.id == Long.MIN_VALUE){
            if ( ! idString.equals(other.idString)) return false;
        } else {
            if (id != other.id) return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (id ^ (id >>> 32));
        result = 31 * result + (idString != null ? idString.hashCode() : 0);
        return result;
    }

    @Override
    public String toString(){
        if(idString!=null)
            return idString;
        return ""+id;
    }
}
