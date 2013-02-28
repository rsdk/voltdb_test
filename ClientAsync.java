import org.voltdb.*;
import org.voltdb.client.*;
import java.io.*;
import java.util.*;

public class ClientAsync_refactored {
	
	static List<String> readNamesFromFile(String dateiname) {
		List<String> names = new ArrayList<String>();
		String zeile = "";
		
		try {
			FileReader fr = new FileReader(dateiname);
			BufferedReader br = new BufferedReader(fr);

			while( (zeile = br.readLine()) != null )
			{
				names.add(zeile);
			}
			br.close();
			fr.close();
		
		} catch (IOException e) {
			System.out.println("Dateilesefehler");
			e.printStackTrace();
			System.exit(-1);
		}
			
		return names;
	}

	static void writeCardsToDB(org.voltdb.client.Client myApp, int numberOfCards, long cardnumber_start)
	{
		long cardnumber = cardnumber_start;
		int[] daily = {100, 200, 500, 1000, 2000, 5000, 10000, 20000, 100000, 1000000};
		int[] monthly = {1000, 2000, 5000, 10000, 20000, 50000, 100000, 200000, 1000000, 10000000};
		int number_rnd10 = 0;
		int number_rndfn = 0;
		int number_rndln = 0;
		List<String> firstn = readNamesFromFile("baby-names.csv");
		List<String> lastn = readNamesFromFile("surnames.csv");
		
		for (int i = 0; i < numberOfCards; i++){
			if ( i % 10000 == 0 ) {
				System.out.println("Number of Cards: \t" + i);
			}
			number_rnd10 = (int) (Math.random()*10);  //random number from 0 till 9
			number_rndfn = (int) (Math.random()*firstn.size());  //random number for namearray
			number_rndln = (int) (Math.random()*lastn.size());   //random number for namearray
			
			try {
				myApp.callProcedure("Insert_card", cardnumber++, daily[number_rnd10], monthly[number_rnd10], 1, 5, firstn.get(number_rndfn)+" "+lastn.get(number_rndln));
			
			} catch (Exception e) {
				System.out.println("Error while executing Stored Procedure insert_card");
				e.printStackTrace();
				return;
			}
		}		
	}
	
	
	
	
	public static void main(String[] args) throws Exception {
		// *** Instantiate Client and connect to Database ***
		org.voltdb.client.Client myApp;
		myApp = ClientFactory.createClient();
		myApp.createConnection("localhost");
		
		// *** Load the DB ***
		int numberOfCards = 100000; // 100000 dauert ca. 38sec in voltdb. postgresql: ca. 15 min auf hdd und 1min 30sek auf ssd 
		long cardnumber_start = 1111222233330000L;
		
		long startTime = System.nanoTime();
		writeCardsToDB(myApp, numberOfCards, cardnumber_start);
		long endTime = System.nanoTime();
		long duration = (endTime - startTime);
		System.out.println("Karten hinzufügen hat: "+ duration/1000000000.0+" Sekunden gedauert.");
		System.out.println("Das waren: "+ (duration / (numberOfCards * 1.0))/1000000.0+" millisekunden pro Karte.");
		
		
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

		
		
		System.out.printf("Datenbank vorbereitet \n\n");
		 
		//neue Daten
		long number_rndcard = 0;
		long amount = 0;
		String country_code = "DE";
		double lat_neu = 48.4;
		double long_neu = 13.9;
		amount = (long) (Math.random() * 1000 + 40);		
		
		
		number_rndcard = (long) (Math.random() * numberOfCards + cardnumber_start); //zufallszahl für card
		
		// INSERT
		
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
