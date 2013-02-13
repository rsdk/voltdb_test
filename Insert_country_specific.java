import org.voltdb.*;

public class Insert_country_specific extends VoltProcedure {
	public final SQLStmt sql = new SQLStmt(
		"INSERT INTO country_specific VALUES (?, ?, ?);"
		);
		
	public VoltTable[] run( String country_code,
							byte disallowed,
							long daily_limit)
		throws VoltAbortException {
			voltQueueSQL( sql, country_code, disallowed, daily_limit);
			voltExecuteSQL();
			return null;
		}
}
						
/*
CREATE TABLE country_specific (
    country_code VARCHAR(2) NOT NULL,
    disallowed TINYINT,
    daily_limit DECIMAL,
    PRIMARY KEY (country_code)
);
*/
