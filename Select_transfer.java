import org.voltdb.*;

public class Select_transfer extends VoltProcedure {
	//1 month in microseconds: 2.628×10^6 seconds = 2628000 seconds
	//1 day in microseconds: 86400 seconds
	long microsTime = System.currentTimeMillis() * 1000L;
	public final SQLStmt sql = new SQLStmt(
		"SELECT transfer_time, amount, latitude, longitude, country_code FROM transfer WHERE card_num = ? AND WHERE transfer_time > "+ ( microsTime - 2628000L ) +" ORDER BY transfer_time DESC;"
		);
		
	public VoltTable[] run( long card_num
							)
		throws VoltAbortException {
			voltQueueSQL( sql, card_num);
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
