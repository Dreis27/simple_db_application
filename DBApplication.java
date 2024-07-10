import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

// StudentID: 38880059

// Class that creates and populates database tables with data from csv files,
// and performs sql queries on those tables

// Using commas within the values in csv files is not supported
// That will result in wrong readings from those files
// consequently messsing up the operations

public class DBApplication {
    private Connection DBconnection = null;

    // create database connection
    private Connection connect() {
        Connection conn = null;
        try {
            conn = DriverManager.getConnection("jdbc:sqlite:dbApp.db");
            System.out.println("connection to SQLite established");
        } catch (SQLException e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
        return conn;
    }

    // close database connection
    private void closeConnection(){
        try{
            if(DBconnection!=null) DBconnection.close();
            System.out.println("database connection closed");
        } catch (SQLException e){
            System.out.println(e.getMessage());
        }
    }

    // statements for creating tables
    private String playersTableStatement=
            "CREATE TABLE players ("
            + " player_id integer PRIMARY KEY,"
            + " name text NOT NULL,"
            + " position text NOT NULL,"
            + " team_id integer,"
            + " FOREIGN KEY(team_id) REFERENCES teams(team_id) ON DELETE SET NULL"
            + ");";
    
    private String teamsTableStatement =
            "CREATE TABLE teams ("
            + " team_id integer PRIMARY KEY,"
            + " name text NOT NULL,"
            + " location text NOT NULL,"
            + " nba_titles integer NOT NULL"
            + ");";
    
    private String gamesTableStatement = 
            "CREATE TABLE games ("
            + " game_id integer PRIMARY KEY,"
            + " date text NOT NULL,"
            + " home_team_id integer NOT NULL,"
            + " away_team_id integer NOT NULL,"
            + " home_team_score integer NOT NULL,"
            + " away_team_score integer NOT NULL,"
            + " FOREIGN KEY(home_team_id) REFERENCES teams(team_id),"
            + " FOREIGN KEY(away_team_id) REFERENCES teams(team_id)"
            + ");";
    

    private String dropTable(String tableName){
        return "DROP TABLE IF EXISTS " +tableName +";";
    }

    // function to create tables
    private void createTable(String name) {
        String sql = null;
        // sellect appropriate table creation query
        if(name.equals("players")) sql = playersTableStatement;
        else if(name.equals("teams")) sql = teamsTableStatement;
        else if(name.equals("games")) sql = gamesTableStatement;
    
        try (Statement stmt = DBconnection.createStatement()) {
            // delete table if already exists and create new table
            stmt.executeUpdate(dropTable(name));
            stmt.executeUpdate(sql);
            System.out.println("table "+name+" created");
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    // Elements of this method adapted from Dr. Guido Schmitz's lab materials for week17 (LZSCC201)
    // (NativeCSVReader.java)
    public void populateTableFromFile(String fileName, String tableName) {
        String insertStmtStart = "INSERT INTO " + tableName + " (";
        String insertStmtEnd = ") VALUES (";
        try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
            String line;
            List<String[]> allRows = new ArrayList<>();

            // read each line from the csv file
            while ((line = br.readLine()) != null) {
                // split the line
                String[] row = line.split(","); 
                allRows.add(row);
            }

            String[] headers = allRows.get(0); //headers in the first row
            // modify inser statement by adding header data
            for (String header : headers) {
                insertStmtStart += header + ",";
                insertStmtEnd += "?,";
            }
            // remove commas at the end
            insertStmtStart = insertStmtStart.substring(0, insertStmtStart.length() - 1);
            insertStmtEnd = insertStmtEnd.substring(0, insertStmtEnd.length() - 1);
            String insertStmt = insertStmtStart + insertStmtEnd + ")";

            try (PreparedStatement statement = DBconnection.prepareStatement(insertStmt)) {
                DBconnection.setAutoCommit(false); // disable autocommit

                // creating insert statements for each row of data to populate the table
                for (int i = 1; i < allRows.size(); i++) {
                    String[] rowData = allRows.get(i);
                    for (int j = 0; j < headers.length; j++) {
                        statement.setString(j + 1, rowData[j]);
                    }
                    statement.addBatch();
                }
                statement.executeBatch(); // execute the batch of prepared insert statements
                DBconnection.commit(); // commit changes to the db
                System.out.println("reading from " + fileName + " and populating " + tableName + " completed");
            } catch (SQLException e) {
                e.printStackTrace();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    // function to print the result of a query
    private void printSelectQueryOutput(String query, int colWidth){
        try(Statement statement = DBconnection.createStatement();
            ResultSet result = statement.executeQuery(query)){
            
            ResultSetMetaData metaData = result.getMetaData();
            int colCount = metaData.getColumnCount();

            // print column names
            for (int i = 1; i <= colCount; i++) {
                String colName = metaData.getColumnName(i);
                System.out.print(String.format("%-" + colWidth + "s", colName));
            }
            System.out.println();
            
            // separate column names from rest of the rows
            for (int i = 1; i<=colCount; i++){
                System.out.print("---------------------------");
            }
            System.out.println();

            // print rows
            while (result.next()) {
                for (int i = 1; i <= colCount; i++) {
                    String colValue = result.getString(i);
                    System.out.print(String.format("%-" + colWidth + "s", colValue));
                }
                System.out.print("\n");
            }
        } catch (SQLException e){
                System.out.println(e.getMessage());
        }
    }

    public static void main(String[] args){
        DBApplication app = new DBApplication();
        System.out.println("Connecting to the database...\n");
        app.DBconnection = app.connect();

        System.out.println("\nCreating tables...\n");
        app.createTable("players");
        app.createTable("teams");
        app.createTable("games");

        System.out.println("\n\nTransferring data from CSV into the tables...\n");
        app.populateTableFromFile("teams.csv", "teams");
        app.populateTableFromFile("players.csv", "players");
        app.populateTableFromFile("games.csv", "games");

        System.out.println("\n\nPerforming SELECT queries...\n");

        System.out.println("displaying all players whose team has won more than 5 NBA titles\n");

        app.printSelectQueryOutput("SELECT players.name AS player_name, teams.name AS team_name,"
                                        +" teams.nba_titles AS nba_titles"
                                        +" FROM players"
                                        +" INNER JOIN teams ON players.team_id = teams.team_id"
                                        +" WHERE teams.nba_titles > 5",
                                        27);
        

        System.out.println("\ndisplaying all games where the player by the name of Davis Bertans has played...\n");

        app.printSelectQueryOutput("SELECT games.date, home_teams.name AS home_team, away_teams.name AS away_team"
                                        + " FROM games"
                                        + " INNER JOIN teams home_teams ON games.home_team_id = home_teams.team_id"
                                        + " INNER JOIN teams away_teams ON games.away_team_id = away_teams.team_id"
                                        + " INNER JOIN players ON players.team_id = home_teams.team_id"
                                        + " OR players.team_id = away_teams.team_id"
                                        + " WHERE players.name = 'Davis Bertans'",
                                        25);

        System.out.println("\ndisplaying all players whose team has participated in more than one game...\n");

        app.printSelectQueryOutput("SELECT players.name, COUNT(games.game_id) AS games_played"
                                        + " FROM players"
                                        + " INNER JOIN teams on players.team_id = teams.team_id"
                                        + " LEFT JOIN games ON games.home_team_id = teams.team_id"
                                        + " OR games.away_team_id = teams.team_id"
                                        + " GROUP BY players.player_id"
                                        + " HAVING COUNT(games.game_id)>1",
                                        30);

        System.out.println("\ndisplaying teams sorted in descending order by the total amount of points they have accumulated in their home games...\n");

        app.printSelectQueryOutput("SELECT teams.name, SUM(games.home_team_score) AS total_home_points"
                                        + " FROM teams"
                                        + " LEFT JOIN games ON teams.team_id = games.home_team_id"
                                        + " GROUP BY teams.team_id"
                                        + " ORDER BY SUM(games.home_team_score) DESC",
                                        30);

        System.out.println("\ndisplaying top 10 players sorted in descending order by the amount of points their team has accumulated in all games...\n");

        app.printSelectQueryOutput("SELECT players.name AS player, teams.name AS team, SUM(games.home_team_score) + SUM(games.away_team_score) AS total_points"
                                        + " FROM players"
                                        + " INNER JOIN teams ON players.team_id = teams.team_id"
                                        + " LEFT JOIN games ON teams.team_id = games.home_team_id OR teams.team_id = games.away_team_id"
                                        + " GROUP BY players.player_id"
                                        + " ORDER BY total_points DESC"
                                        + " LIMIT 10",
                                        30);

        System.out.println("\n\nClosing database connection...");
        app.closeConnection();
    }
}
