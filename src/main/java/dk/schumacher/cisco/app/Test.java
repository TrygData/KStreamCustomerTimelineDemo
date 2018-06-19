package dk.schumacher.cisco.app;

import java.io.IOException;
import static dk.schumacher.cisco.app.KafkaAvroProducerGeneric.randomNumber;

/**
 * @author Gøran Schumacher (GS) / Schumacher Consulting Aps
 * @version $Revision$ 15/06/2018
 */
public class Test {

    public static void main(String[] args) throws IOException, InterruptedException {
        for (int i = 0; i < 100; i++) {
            System.out.println(i % 20 +1);
        }

        System.out.println(randomNumber(10));
    }
}
