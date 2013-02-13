import org.voltdb.*;

public class Insert_country extends VoltProcedure {
	public final SQLStmt sql = new SQLStmt(
		"INSERT INTO COUNTRIES VALUES (?, ?, ?);"
		);
		
	public VoltTable[] run( String country_code,
							String country_name,
							String country_note )
		throws VoltAbortException {
			voltQueueSQL( sql, country_code, country_name, country_note);
			voltExecuteSQL();
			return null;
		}
}
						
