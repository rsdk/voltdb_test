import org.voltdb.*;

public class Select_all_countries extends VoltProcedure {
	public final SQLStmt sql = new SQLStmt(
		"SELECT country_code, country_name, notes FROM countries ORDER BY country_code;"
		);
		
	public VoltTable[] run()
		throws VoltAbortException {
			voltQueueSQL( sql);
			return voltExecuteSQL();
		}
}
						
