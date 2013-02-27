import org.voltdb.*;
import org.voltdb.client.*;
import java.io.*;
import java.util.*;

public class ClientAsync2 {
	public static void main(String[] args) throws Exception {
		// *** Instantiate Client and connect to Database ***
		org.voltdb.client.Client myApp;
		myApp = ClientFactory.createClient();
		myApp.createConnection("localhost");
		
		int numberOfCards = 1000000; // 10 Mio dauert ca. eine Stunde
		long cardnumber_start = 1111222233330000L;

		long number_rndcard = 0;
		long amount = 0;
		String country_code = "DE";
		double lat_neu = 48.4;
		double long_neu = 13.9;
		amount = (long) (Math.random() * 1000 + 40);
		
		long rnd_cardnumber = 0L;
		int number_transactions = 2000000;										
		for (int k = 0; k < number_transactions; k++) {
			amount = (int) (Math.random()*1000);
			rnd_cardnumber = cardnumber_start+((int) (Math.random()*numberOfCards));
			if ( k % 100000 == 0 ) {
				System.out.println("Transfer-number: " + k);
			}
			String zweck = "TEST " + k;
			
			try {
			myApp.callProcedure(new NullCallback(), "new_transfer", 
								rnd_cardnumber,  /*card_number*/
								amount,      /*amount*/
								48.2,      /*/lat*/
								13.0,      /*long*/
								"DE",      /*country_code*/
								zweck     /*purpose*/
								);
			} catch (Exception e) {
				//e.printStackTrace();
				//System.exit(-1);
			}
		}
	}
}
