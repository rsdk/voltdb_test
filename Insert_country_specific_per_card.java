import org.voltdb.*;

public class Insert_country_specific_per_card extends VoltProcedure {
	public final SQLStmt sql = new SQLStmt(
		"INSERT INTO country_specific_per_card VALUES (?, ?, ?, ?);"
		);
		
	public VoltTable[] run( long card_num,
							String country_code,
							byte disallowed,
							long daily_limit)
		throws VoltAbortException {
			voltQueueSQL( sql, card_num, country_code, disallowed, daily_limit);
			voltExecuteSQL();
			return null;
		}
}
						
/*
CREATE TABLE country_specific_per_card (
    card_num BIGINT NOT NULL,
    country_code VARCHAR(2) NOT NULL,
    disallowed TINYINT,
    daily_limit DECIMAL,
    PRIMARY KEY (card_num, country_code)
);
*/
