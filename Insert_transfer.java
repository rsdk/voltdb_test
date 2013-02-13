import org.voltdb.*;

public class Insert_transfer extends VoltProcedure {
	public final SQLStmt sql = new SQLStmt(
		"INSERT INTO transfer VALUES (?, ?, ?, ?, ?, ?, ?, ?);"
		);
		
	public VoltTable[] run( long transfer_num,
							long transfer_time ,
							long card_num,
							long amount,
							String purpose,
							float latitude,
							float longitude,
							String country_code)
		throws VoltAbortException {
			voltQueueSQL( sql, transfer_num, transfer_time, card_num, amount, purpose, latitude, longitude, country_code);
			return voltExecuteSQL();
		}
}
						
/*
CREATE TABLE transfer (
    transfer_num BIGINT NOT NULL,
    transfer_time TIMESTAMP,
    card_num BIGINT NOT NULL,
    amount DECIMAL NOT NULL,
    purpose VARCHAR(64),
    latitude FLOAT,
    longtitude FLOAT,
    country_CODE VARCHAR(2),
    PRIMARY KEY (transfer_num)
);
*/
