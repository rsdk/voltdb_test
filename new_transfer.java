import org.voltdb.*;


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
	//1 month in microseconds: 2.628×10^6 seconds = 2628000 seconds
	//1 day in microseconds: 86400 seconds
	long microsTime = System.currentTimeMillis() * 1000L;
	//Grunddaten
	final String select_card= "SELECT blocked, daily_limit, monthly_limit, distance_per_hour_max FROM card WHERE card_num = ?;";
	final String select_country= "SELECT disallowed, daily_limit FROM country_specific WHERE country_code = ?;";
	final String select_country_spec= "SELECT disallowed, daily_limit FROM country_specific_per_card WHERE card_num = ? AND WHERE country_code = ?;";
	//Daten von den Transfers
	final String select_last = "SELECT TOP 1 transfer_time, latitude, longitude FROM transfer WHERE card_num = ? ORDER BY transfer_time DESC;";
	final String select_amount_d_country = "SELECT sum(amount) FROM transfer WHERE card_num = ? AND WHERE country_code = ? AND WHERE transfer_time > "+ ( microsTime - 86400L );
	final String select_amount_d = "SELECT sum(amount) FROM transfer WHERE card_num = ? AND WHERE transfer_time > "+ ( microsTime - 86400L );
	final String select_amount_m = "SELECT sum(amount) FROM transfer WHERE card_num = ? AND WHERE transfer_time > "+ ( microsTime - 2628000L );

	final String insert = "INSERT INTO transfer (?,?,?,?,?,?,?,?);";
	
	public final SQLStmt select_card_sql = new SQLStmt(select_card);
	public final SQLStmt select_country_sql = new SQLStmt(select_country);
	public final SQLStmt select_country_spec_sql = new SQLStmt(select_country_spec);
	
	public final SQLStmt select_last_sql = new SQLStmt(select_last);
	public final SQLStmt select_amount_d_sql = new SQLStmt(select_amount_d);
	public final SQLStmt select_amount_d_country_sql = new SQLStmt(select_amount_d_country);
	public final SQLStmt select_amount_m_sql = new SQLStmt(select_amount_m);
	
	public final SQLStmt insert_sql = new SQLStmt(insert);
		
	public VoltTable[] run( long card_num, long amount, float latitude, float longitude, String country_code, String purpose) throws VoltAbortException {
			
			
			// *** check if transfer ist valid ***
			
			
			voltQueueSQL( select_last_sql, card_num); // last transfer
			VoltTable[] queryresults_lasttransfer = voltExecuteSQL();
			// If there is no matching record, rollback  
			if (queryresults_lasttransfer[0].getRowCount() == 0 ) throw new VoltAbortException();
						
			voltQueueSQL( select_card_sql, card_num);
			voltQueueSQL( select_country_sql, country_code);
			voltQueueSQL( select_country_spec_sql, card_num, country_code);
			voltQueueSQL( select_amount_d_sql, card_num);
			voltQueueSQL( select_amount_d_country_sql, card_num, country_code);
			voltQueueSQL( select_amount_m_sql, card_num);
			
			VoltTable[] queryresults = voltExecuteSQL();
			
			
			
			
			
			
			
			
			
			
			
			
			
			// Insert the new Transaction
			voltQueueSQL( insert_sql, card_num);
			
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
