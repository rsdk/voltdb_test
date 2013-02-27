import org.voltdb.*;
import org.voltdb.types.TimestampType;
import java.math.BigDecimal;

/*
 * 1. Prüfen ob der Transfer durchgeführt werden darf
 * 
 * 
 * 
 * 2. neuen Transfer eintragen
 * 
 * 
 */
 
public class new_transfer extends VoltProcedure {
	//1 month in microseconds: 2.628×10^6 seconds = 2628000000000 useconds
	//1 day in microseconds: 86400000000 useconds
	TimestampType current = new TimestampType();
	TimestampType minusOneD = new TimestampType(current.getTime() - 86400000000L);
	TimestampType minusOneM = new TimestampType(current.getTime()-  2628000000000L);
	//Grunddaten
	final String select_card= "SELECT blocked, daily_limit, monthly_limit, distance_per_hour_max FROM card WHERE card_num = ?;";
	final String select_country= "SELECT disallowed, daily_limit FROM country_specific WHERE country_code = ?;";
	final String select_country_spec= "SELECT disallowed, daily_limit FROM country_specific_per_card WHERE card_num = ? AND country_code = ?;";
	//Daten von den Transfers
	final String select_last = "SELECT TOP 1 transfer_time, latitude, longitude FROM transfer WHERE card_num = ? ORDER BY transfer_time, card_num DESC;";
	final String select_amount_d_country = "SELECT SUM(amount) FROM transfer WHERE card_num = ? AND country_code = ? AND transfer_time > '"+ minusOneD +"';";
	final String select_amount_d = "SELECT sum(amount) FROM transfer WHERE card_num = ? AND transfer_time > '"+ minusOneD +"';";
	final String select_amount_m = "SELECT sum(amount) FROM transfer WHERE card_num = ? AND transfer_time > '"+ minusOneM +"';";

	final String insert = "INSERT INTO transfer VALUES (?,?,?,?,?,?,?);";
	
	public final SQLStmt select_card_sql = new SQLStmt(select_card);
	public final SQLStmt select_country_sql = new SQLStmt(select_country);
	public final SQLStmt select_country_spec_sql = new SQLStmt(select_country_spec);
	
	public final SQLStmt select_last_sql = new SQLStmt(select_last);
	public final SQLStmt select_amount_d_sql = new SQLStmt(select_amount_d);
	public final SQLStmt select_amount_d_country_sql = new SQLStmt(select_amount_d_country);
	public final SQLStmt select_amount_m_sql = new SQLStmt(select_amount_m);
	
	public final SQLStmt insert_sql = new SQLStmt(insert);
		
	public VoltTable[] run( long card_num, long amount, double latitude, double longitude, String country_code, String purpose) throws VoltAbortException {
			
			// *** check if transfer ist valid ***
			voltQueueSQL( select_card_sql, card_num); // is the card-number valid
			VoltTable[] queryresults_card = voltExecuteSQL();
			
			// If there is no matching record, rollback  
			if (queryresults_card[0].getRowCount() == 0 ) throw new VoltAbortException();
						
			voltQueueSQL( select_last_sql, card_num); // queue 0
			voltQueueSQL( select_country_sql, country_code); //queue 1
			voltQueueSQL( select_country_spec_sql, card_num, country_code); //queue 2
			voltQueueSQL( select_amount_d_sql, card_num); // 3
			voltQueueSQL( select_amount_d_country_sql, card_num, country_code); // 4
			voltQueueSQL( select_amount_m_sql, card_num); // 5
			
			VoltTable[] queryresults = voltExecuteSQL();

			//checks
			//is there a transfer
			if (queryresults[0].getRowCount() != 0 ) {
				
				//card blocked & general amount check
				// card blocked?      
				if (queryresults_card[0].fetchRow(0).getLong(0) != 1) {
					System.out.println("Card is Blocked");
					throw new VoltAbortException();
				}
				
				//daily?  
				if (  queryresults_card[0].fetchRow(0).getDecimalAsBigDecimal(1).compareTo( queryresults[3].fetchRow(0).getDecimalAsBigDecimal(0).add(new BigDecimal(amount)) ) < 0 ) {
					//System.out.println("Daily Limit");
					throw new VoltAbortException();
				}
				
				//monthly?
				if (  queryresults_card[0].fetchRow(0).getDecimalAsBigDecimal(2).compareTo( queryresults[5].fetchRow(0).getDecimalAsBigDecimal(0).add(new BigDecimal(amount)) ) < 0 ) {
					System.out.println("Monthly Limit");
					throw new VoltAbortException();
				}
				
				
				//general country check
				//is there data for this country? if no then don't do anything OR if this country is not allowed then do nothing OR if daily limit for this country would be exceeded
				if (queryresults[1].getRowCount() == 0 || queryresults[1].fetchRow(0).getLong(0) != 1 || queryresults[1].fetchRow(0).getDecimalAsBigDecimal(1).compareTo( queryresults[4].fetchRow(0).getDecimalAsBigDecimal(0).add(new BigDecimal(amount))) < 0 ) {
					//System.out.println("Daily Limit Country");
					throw new VoltAbortException();
				}
				
				//distance check
				double delta_lat = Math.abs(latitude - queryresults[0].fetchRow(0).getDouble(1));
				double delta_long = Math.abs(longitude - queryresults[0].fetchRow(0).getDouble(2));
				double distance = Math.sqrt( Math.pow(delta_lat, 2) + Math.pow(delta_long, 2) );
				
				double delta_time = ( current.getTime() - queryresults[0].fetchRow(0).getTimestampAsLong(0) ) / 1000*1000*60*60;
				
				if ( distance / delta_time > queryresults_card[0].fetchRow(0).getLong(3) ) {
					System.out.println("Distance");
					throw new VoltAbortException();
				} 
				
				//specific country check
				
				
			}	
			// Insert the new Transaction
			voltQueueSQL( insert_sql, this.getTransactionTime(), card_num, amount, purpose , latitude, longitude, country_code );
			
			return voltExecuteSQL();
		}
}

					
/*
CREATE TABLE transfer (
    transfer_time TIMESTAMP,
    card_num BIGINT NOT NULL,
    amount DECIMAL NOT NULL,
    purpose VARCHAR(64),
    latitude FLOAT,
    longitude FLOAT,
    country_code VARCHAR(2),
    PRIMARY KEY (card_num, transfer_time)
);
*/
