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
		FileReader fr = new FileReader("country_names_and_code_elements_txt");
		BufferedReader br = new BufferedReader(fr);
			
		String zeile = "";
		String[] parts = new String[2];

		while( (zeile = br.readLine()) != null )
		{
			parts = zeile.split(";");
			myApp.callProcedure("Insert_country", parts[1],parts[0],"");
		}
		br.close();
		fr.close();
		//Load Card with random cards
					/*
			CREATE TABLE card (
				card_num BIGINT NOT NULL,
				daily_limit DECIMAL,
				monthly_limit DECIMAL,
				blocked TINYINT,
				distance_per_hour_max SMALLINT,
				customer_name VARCHAR(64),
				PRIMARY KEY (card_num)
			);
			*/
		//myApp.callProcedure("Insert_card", "long_rnd", "long_daily", "long_monthly", "blocked == 1 byte", "short entfernung in lat/long", "String rnd_name");
		//myApp.callProcedure("Insert_card", 9998887776665554L, 800 ,18000, 1, 5, "Peter Gryffin");
		
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
		//String Array aus nachamendatei laden
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
