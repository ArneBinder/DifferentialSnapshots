package concarne;

import org.junit.Test;

import java.math.BigDecimal;

import static org.junit.Assert.*;

public class TupleTest {

    @Test
    public void parseDouble(){

        double p1 = Double.parseDouble("0.5");
        BigDecimal p2 = new BigDecimal("0.5");


        // compareTo ignores scale (2.0 == 2.00)
        // equals doesn't
        System.out.println(p2.compareTo(new BigDecimal(p1)));

        double parsed = Double.valueOf("696.51666699999998"); // gives 696,516667!
        BigDecimal bd = new BigDecimal("696.51666699999998");
        System.out.println(bd.compareTo(new BigDecimal(parsed)));
//        parsed = bd.doubleValue(); // doesn't work either!

        System.out.println(String.format("%.100f",parsed));
        System.out.println(String.format("%.100f",bd));

    }

}