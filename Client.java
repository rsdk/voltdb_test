import org.voltdb.*;
import org.voltdb.client.*;
import java.io.*;
import java.util.*;

public class Client {
	public static void main(String[] args) throws Exception {
		// *** Instantiate Client and connect to Database ***
		org.voltdb.client.Client myApp;
		myApp = ClientFactory.createClient();
		myApp.createConnection("localhost");
		
		// *** Load the DB ***
		
		//Load Countries from file
		FileReader fr = new FileReader("country_names_and_code_elements_txt.txt");
		BufferedReader br = new BufferedReader(fr);
			
		String zeile = "";
		String[] parts = new String[4];

		while( (zeile = br.readLine()) != null )
		{
			parts = zeile.split(";");
			myApp.callProcedure("Insert_country", parts[1],parts[0],"");
			myApp.callProcedure("Insert_country_specific", parts[1], parts[2],parts[3]);
		}
		br.close();
		fr.close();
		//Load Card with random cards

		//load String Array with names from file
		FileReader frfn = new FileReader("baby-names.csv");
		BufferedReader brfn = new BufferedReader(frfn);
			
		zeile = "";
		List<String> firstnames = new ArrayList<String>();

		while( (zeile = brfn.readLine()) != null )
		{
			firstnames.add(zeile);
		}
		brfn.close();
		frfn.close();
		//load String Array with names from file
		FileReader frln = new FileReader("surnames.csv");
		BufferedReader brln = new BufferedReader(frln);
			
		zeile = "";
		List<String> lastnames = new ArrayList<String>();

		while( (zeile = brln.readLine()) != null )
		{
			lastnames.add(zeile);
		}
		brln.close();
		frln.close();
		
		
		int numberOfCards = 10000;
		long cardnumber_start = 1111222233330000L;
		long cardnumber = cardnumber_start;
		int[] daily = {100, 200, 500, 1000, 2000, 5000, 10000, 20000, 100000, 1000000};
		int[] monthly = {1000, 2000, 5000, 10000, 20000, 50000, 100000, 200000, 1000000, 10000000};
		int number_rnd10 = 0;
		int number_rndfn = 0;
		int number_rndln = 0;
		
		for (int i = 0; i < numberOfCards; i++){
			number_rnd10 = (int) (Math.random()*10);  //random number from 0 till 9
			number_rndfn = (int) (Math.random()*firstnames.size());  //random number for namearray
			number_rndln = (int) (Math.random()*lastnames.size());   //random number for namearray
			myApp.callProcedure("Insert_card", cardnumber++, daily[number_rnd10], monthly[number_rnd10], 1, 5, firstnames.get(number_rndfn)+" "+lastnames.get(number_rndln));
		}
		
		
		System.out.printf("Datenbank vorbereitet \n\n");
		
		// Transactions
		/*
		 * CREATE TABLE transfer (
			transfer_num BIGINT NOT NULL,
			transfer_time TIMESTAMP,
			card_num BIGINT NOT NULL,
			amount DECIMAL NOT NULL,
			purpose VARCHAR(64),
			latitude FLOAT,
			longitude FLOAT,
			country_code VARCHAR(2),
			PRIMARY KEY (transfer_num)
			);
		 */
		 
		//neue Daten
		long number_rndcard = 0;
		long amount = 0;
		String country_code = "DE";
		double lat_neu = 48.4;
		double long_neu = 13.9;
		amount = (long) (Math.random() * 1000 + 40);		
		
		
		number_rndcard = (long) (Math.random() * numberOfCards + cardnumber); //zufallszahl für card
		
		// INSERT
		
		final ClientResponse response = myApp.callProcedure("new_transfer", 
															cardnumber_start+1,  /*card_number*/
															10,      /*amount*/
															48.2,      /*/lat*/
															13.0,      /*long*/
															"DE",      /*country_code*/
															"TEST"     /*purpose*/
															);
		long rnd_cardnumber = 0L;
												
		for (int k = 0; k < 100000000; k++) {
			amount = (int) (Math.random()*100);
			rnd_cardnumber = cardnumber_start+((int) (Math.random()*numberOfCards));
			if ( k % 1000 == 0 ) {
				System.out.println("Transfer-number: " + k + "   Amount: "+ amount);
			}
			String zweck = "TEST " + k;
			
			try {
			myApp.callProcedure("new_transfer", 
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
