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

		//String Array aus vornamendatei laden
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
		//String Array aus nachnamendatei laden
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
		long cardnumber = 1111222233330000L;
		int[] daily = {100, 200, 500, 1000, 2000, 5000, 10000, 20000, 100000, 1000000};
		int[] monthly = {1000, 2000, 5000, 10000, 20000, 50000, 100000, 200000, 1000000, 10000000};
		int number_rnd10 = 0;
		int number_rndfn = 0;
		int number_rndln = 0;
		
		for (int i = 0; i < numberOfCards; i++){
			number_rnd10 = (int) (Math.random()*10);
			number_rndfn = (int) (Math.random()*firstnames.size());
			number_rndln = (int) (Math.random()*lastnames.size());
			myApp.callProcedure("Insert_card", cardnumber++, daily[number_rnd10], monthly[number_rnd10], 1, 5, firstnames.get(number_rndfn)+" "+lastnames.get(number_rndln));
		}
		
		
		System.out.printf("Datenbank vorbereitet \n\n");
		
		// Transactions
		/*
		 * 
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
		 * 
		 * 
		 */
		//neue Daten
		long number_rndcard = 0;
		long amount = 0;
		String country_code = "DE";
		double lat_neu = 48.4;
		double long_neu = 13.9;
		amount = (long) (Math.random() * 1000 + 40);		
		
		
		number_rndcard = (long) (Math.random() * numberOfCards + cardnumber); //zufallszahl für card

		//hole alte daten
		final ClientResponse response_transfer = myApp.callProcedure("Select_transfer", number_rndcard);
		double lat_alt = 48.2;
		double long_alt = 15.7;
		
		//Berechne Entfernung
		double delta_lat = Math.abs(lat_neu - lat_alt);
		double delta_long = Math.abs(long_neu - long_alt);
		double distance = Math.sqrt( Math.pow(delta_lat, 2) + Math.pow(delta_long, 2) );
		
		//check if last transaction is near
		
		
		
		// *** Retrieve data ***
		final ClientResponse response = myApp.callProcedure("Select_all_countries");
		if (response.getStatus() != ClientResponse.SUCCESS) {
			System.err.println(response.getStatusString());
			System.exit(-1);
		}
		
		final VoltTable results[] = response.getResults();
		if (results.length == 0) {
			System.out.printf("DB leer");
			System.exit(-1);
		}
		
		VoltTable resultTable = results[0];
		VoltTableRow row = resultTable.fetchRow(0);
		System.out.printf("%s, %s, %s \n", row.getString("country_code"),
											row.getString("country_name"),
											row.getString("notes"));
		}
}
