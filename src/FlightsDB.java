import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Allows clients to query and update the database in order to log in, search
 * for flights, reserve seats, show reservations, and cancel reservations.
 */
public class FlightsDB {

  /** Maximum number of reservations to allow on one flight. */
  private static int MAX_FLIGHT_BOOKINGS = 3;

  private static final String SEARCH_ONE_HOP_SQL	=
		  "SELECT TOP (99) fid, name, flight_num, origin_city, dest_city, " +
				  "  actual_time\n" +
				  "FROM Flights F1, Carriers\n" +
			      "WHERE carrier_id = cid AND actual_time IS NOT NULL AND " +
			      "    year = ? AND month_id = ? AND day_of_month = ? AND " + 
			      "    origin_city = ? AND dest_city = ?\n" +
			      "ORDER BY actual_time ASC";
  
  private static final String SEARCH_TWO_HOP_SQL =
		  "SELECT TOP (99) F1.fid as fid1, C1.name as name1, " +
			        "    F1.flight_num as flight_num1, F1.origin_city as origin_city1, " +
			        "    F1.dest_city as dest_city1, F1.actual_time as actual_time1, " +
			        "    F2.fid as fid2, C2.name as name2, " +
			        "    F2.flight_num as flight_num2, F2.origin_city as origin_city2, " +
			        "    F2.dest_city as dest_city2, F2.actual_time as actual_time2\n" +
			        "FROM Flights F1, Flights F2, Carriers C1, Carriers C2\n" +
			        "WHERE F1.carrier_id = C1.cid AND F1.actual_time IS NOT NULL AND " +
			        "    F2.carrier_id = C2.cid AND F2.actual_time IS NOT NULL AND " +
			        "    F1.year = ? AND F1.month_id = ? AND F1.day_of_month = ? AND " +
			        "    F2.year = ? AND F2.month_id = ? AND F2.day_of_month = ? AND " +
			        "    F1.origin_city = ? AND F2.dest_city = ? AND" +
			        "    F1.dest_city = F2.origin_city\n" +
			        "ORDER BY F1.actual_time + F2.actual_time ASC";
  
  private static final String LOGIN_SQL =  "SELECT * FROM Customer c WHERE c.username = ? AND c.password = ?";
  private static final String RESERVATIONS_SQL =  "SELECT * FROM Reservations r, Flights f WHERE r.userID = ? AND r.fid = f.fid";
  private static final String DELETE_SQL = "DELETE FROM Reservations WHERE userID = ? AND fid = ?";
  private static final String COUNT = "SELECT count(*) as cnt FROM Reservations WHERE fid = ?";
  private static final String CHECK = "SELECT count(*) as cnt FROM Reservations r, Flights f WHERE r.fid = f.fid AND r.userID = ? AND f.year = ? AND f.month_id = ? AND f.day_of_month = ?";
  private static final String INSERT = "INSERT INTO Reservations VALUES (?, ?)";
  
  /** Holds the connection to the database. */
  private Connection conn;

  /** Opens a connection to the database using the given settings. */
  public void open(Properties settings) throws Exception {
    // Make sure the JDBC driver is loaded.
    String driverClassName = settings.getProperty("flightservice.jdbc_driver");
    Class.forName(driverClassName).newInstance();

    // Open a connection to our database.
    conn = DriverManager.getConnection(
        settings.getProperty("flightservice.url"),
        settings.getProperty("flightservice.sqlazure_username"),
        settings.getProperty("flightservice.sqlazure_password"));
  }

  /** Closes the connection to the database. */
  public void close() throws SQLException {
    conn.close();
    conn = null;
  }

  // SQL statements with spaces left for parameters:
  private PreparedStatement beginTxnStmt;
  private PreparedStatement commitTxnStmt;
  private PreparedStatement abortTxnStmt;
  private PreparedStatement stmt;
  private PreparedStatement stmt2;
  private PreparedStatement login;
  private PreparedStatement res;
  private PreparedStatement deleteRes;
  private PreparedStatement countRes;
  private PreparedStatement add;
  private PreparedStatement dayCheck;

  /** Performs additional preparation after the connection is opened. */
  public void prepare() throws SQLException {
    // NOTE: We must explicitly set the isolation level to SERIALIZABLE as it
    //       defaults to allowing non-repeatable reads.
    beginTxnStmt = conn.prepareStatement(
        "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; BEGIN TRANSACTION;");
    commitTxnStmt = conn.prepareStatement("COMMIT TRANSACTION");
    abortTxnStmt = conn.prepareStatement("ROLLBACK TRANSACTION");
    // TODO: create more prepared statements here
    stmt = conn.prepareStatement(SEARCH_ONE_HOP_SQL);
    stmt2 = conn.prepareStatement(SEARCH_TWO_HOP_SQL);
    login = conn.prepareStatement(LOGIN_SQL);
    res = conn.prepareStatement(RESERVATIONS_SQL);
    deleteRes = conn.prepareStatement(DELETE_SQL);
    countRes = conn.prepareStatement(COUNT);
    add = conn.prepareStatement(INSERT);
    dayCheck = conn.prepareStatement(CHECK);
  }

  /**
   * Tries to log in as the given user.
   * @returns The authenticated user or null if login failed.
   */
  public User logIn(String handle, String password) throws SQLException {
    // TODO: implement this properly
	login.clearParameters();
	login.setString(1, handle);
	login.setString(2, password);
	ResultSet loginResults = login.executeQuery();
	
	if (loginResults.next()) {
		return new User(loginResults.getInt("userID"), loginResults.getString("username"), loginResults.getString("firstname") + " " + loginResults.getString("lastname"));
    } else {
    	return null;
    }
  }

  /**
   * Returns the list of all flights between the given cities on the given day.
   */
  public List<Flight[]> getFlights(
      int year, int month, int dayOfMonth, String originCity, String destCity)
      throws SQLException {

    List<Flight[]> results = new ArrayList<Flight[]>();
    
    stmt.clearParameters();
    stmt.setInt(1, year);
    stmt.setInt(2, month);
    stmt.setInt(3, dayOfMonth);
    stmt.setString(4, originCity);
    stmt.setString(5, destCity);
    ResultSet directResults = stmt.executeQuery();
    
    while (directResults.next()) {
      results.add(new Flight[] {
          new Flight(directResults.getInt("fid"), year, month, dayOfMonth,
              directResults.getString("name"),
              directResults.getString("flight_num"),
              directResults.getString("origin_city"),
              directResults.getString("dest_city"),
              (int)directResults.getFloat("actual_time"))
        });
    }
    directResults.close();

    stmt2.clearParameters();
    stmt2.setInt(1, year);
    stmt2.setInt(2, month);
    stmt2.setInt(3, dayOfMonth);
    stmt2.setInt(4, year);
    stmt2.setInt(5, month);
    stmt2.setInt(6, dayOfMonth);
    stmt2.setString(7, originCity);
    stmt2.setString(8, destCity);
    ResultSet twoHopResults = stmt2.executeQuery();
    
    while (twoHopResults.next()) {
      results.add(new Flight[] {
          new Flight(twoHopResults.getInt("fid1"), year, month, dayOfMonth,
              twoHopResults.getString("name1"),
              twoHopResults.getString("flight_num1"),
              twoHopResults.getString("origin_city1"),
              twoHopResults.getString("dest_city1"),
              (int)twoHopResults.getFloat("actual_time1")),
          new Flight(twoHopResults.getInt("fid2"), year, month, dayOfMonth,
              twoHopResults.getString("name2"),
              twoHopResults.getString("flight_num2"),
              twoHopResults.getString("origin_city2"),
              twoHopResults.getString("dest_city2"),
              (int)twoHopResults.getFloat("actual_time2"))
        });
    }
    twoHopResults.close();

    return results;
  }

  /** Returns the list of all flights reserved by the given user. */
  public List<Flight> getReservations(int userid) throws SQLException {
    // TODO: implement this properly
	List<Flight> results = new ArrayList<Flight>();
	res.clearParameters();
	res.setInt(1, userid);
	ResultSet allResults = res.executeQuery();
	while(allResults.next()) {
		results.add(new Flight(allResults.getInt("fid"), allResults.getInt("year"), allResults.getInt("month_id"),
					allResults.getInt("day_of_month"), allResults.getString("carrier_id"), 
					allResults.getInt("flight_num") + "", allResults.getString("origin_city"), 
					allResults.getString("dest_city"), allResults.getInt("actual_time")));
	}
	allResults.close();
    return results;
  }

  /** Indicates that a reservation was added successfully. */
  public static final int RESERVATION_ADDED = 1;

  /**
   * Indicates the reservation could not be made because the flight is full
   * (i.e., 3 users have already booked).
   */
  public static final int RESERVATION_FLIGHT_FULL = 2;

  /**
   * Indicates the reservation could not be made because the user already has a
   * reservation on that day.
   */
  public static final int RESERVATION_DAY_FULL = 3;

  /**
   * Attempts to add a reservation for the given user on the given flights, all
   * occurring on the given day.
   * @returns One of the {@code RESERVATION_*} codes above.
   */
  public int addReservations(
      int userid, int year, int month, int dayOfMonth, List<Flight> flights)
      throws SQLException {

    // TODO: implement this in a transaction (see beginTransaction etc. below)
	beginTransaction();
	for (int i = 0; i < flights.size(); i++) {
		dayCheck.clearParameters();
		dayCheck.setInt(1, userid);
		dayCheck.setInt(2, year);
		dayCheck.setInt(3, month);
		dayCheck.setInt(4, dayOfMonth);
		ResultSet dayResult = dayCheck.executeQuery();
		int daytotal = 0;
		if (dayResult.next()) {
			daytotal = dayResult.getInt("cnt");
		}
		if (daytotal > 0) {
			rollbackTransaction();
			return RESERVATION_DAY_FULL;			
		}
		countRes.clearParameters();
		countRes.setInt(1, flights.get(i).id);
		ResultSet result = countRes.executeQuery();
		int total = 0;
		if(result.next()) {
			total = result.getInt("cnt");
		}
		if (total < 3) {
			add.clearParameters();
			add.setInt(1, userid);
			add.setInt(2, flights.get(i).id);
			add.executeUpdate();
			commitTransaction();
			return RESERVATION_ADDED;
		} 
	}
	rollbackTransaction();
    return RESERVATION_FLIGHT_FULL;
  }

  /** Cancels all reservations for the given user on the given flights. */
  public void removeReservations(int userid, List<Flight> flights)
      throws SQLException {

    // TODO: implement this in a transaction (see beginTransaction etc. below)
	beginTransaction();
	for (int i = 0; i < flights.size(); i++) {
		deleteRes.clearParameters();
		deleteRes.setInt(1, userid);
		deleteRes.setInt(2, flights.get(i).id);
		deleteRes.executeUpdate();
	}
	commitTransaction();
  }

  /** Puts the connection into a new transaction. */    
  public void beginTransaction() throws SQLException {
    conn.setAutoCommit(false);  // do not commit until explicitly requested
    beginTxnStmt.executeUpdate();  
  }

  /** Commits the current transaction. */
  public void commitTransaction() throws SQLException {
    commitTxnStmt.executeUpdate(); 
    conn.setAutoCommit(true);  // go back to one transaction per statement
  }

  /** Aborts the current transaction. */
  public void rollbackTransaction() throws SQLException {
    abortTxnStmt.executeUpdate();
    conn.setAutoCommit(true);  // go back to one transaction per statement
  } 
}
