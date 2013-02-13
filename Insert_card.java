import org.voltdb.*;

public class Insert_card extends VoltProcedure {
	public final SQLStmt sql = new SQLStmt(
		"INSERT INTO card VALUES (?, ?, ?, ?, ?, ?);"
		);
		
	public VoltTable[] run( long card_num,
							long daily_limit,
							long monthly_limit,
							byte blocked,
							short distance_per_hour_max,
							String customer_name )
		throws VoltAbortException {
			voltQueueSQL( sql, card_num, daily_limit, monthly_limit, blocked, distance_per_hour_max, customer_name);
			voltExecuteSQL();
			return null;
		}
}
						

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
