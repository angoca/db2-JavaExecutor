/*
 * ExecJDBC - Command line program to process SQL DDL statements, from   
 *             a text input file, to any JDBC Data Source
 *
 * Copyright (C) 2004-2014, Denis Lussier
 *
 */

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * Executes the content of a SQL file in the database.<br/>
 * This program is based on one file of BenchmarkSQL.<br/>
 * Licensed under GPL v3.<br/>
 * 
 * This program should be called:<br/>
 * 
 * <source>java -cp .:db2jcc4.jar -Dprop=DB2props -Dscript=myScript.sql
 * ExecuteScript</source>
 * 
 * The properties file should have the following elements (It is processed like
 * a properties file):
 * <ul>
 * <li>driver=</li>
 * <li>conn=</li>
 * <li>user=</li>
 * <li>password=</li>
 * <li>separator=</li>
 * </ul>
 * 
 * @author Andres Gomez Casanova (AngocA)
 */
public class ExecuteScript {

    /**
     * Executes the given statement in the database.
     * 
     * @param stmt
     *            Prepared statement.
     * @param sql
     *            Statement to executed.
     * @throws SQLException
     *             If there is problem executing the statement.
     */
    private static void execSQL(final Statement stmt, final StringBuffer sql,
            final char separator) throws SQLException {
        System.out.println(sql);
        stmt.execute(sql.toString().replace(separator, ' '));
    }

    /**
     * Reads the configuration and establishes the connection.
     * 
     * @return An open connection to the database.
     * @throws ClassNotFoundException
     *             If the driver is not found.
     * @throws SQLException
     *             If there is a problem establishing the connection.
     */
    private static Connection getConnectionFromConfiguration(
            final Properties ini) throws ClassNotFoundException, SQLException {
        final Connection conn;

        // Register jdbcDriver
        Class.forName(ini.getProperty("driver"));

        // Establish connection
        conn = DriverManager.getConnection(ini.getProperty("conn"),
                ini.getProperty("user"), ini.getProperty("password"));
        return conn;
    }

    /**
     * Main method of the program.
     * 
     * @param args
     *            Not used.
     */
    public static void main(final String[] args) {
        // Call the method that performs all the logic.
        try {
            runProcess();
        } catch (final Exception e) {
            e.printStackTrace();
        }
        ;
    }

    /**
     * Prepares the environment and then executes the script.
     * 
     * @throws IOException
     *             If there is a problem reading the properties file or the
     *             script file.
     * @throws FileNotFoundException
     *             If the properties file is not found.
     */
    private static void runProcess() throws FileNotFoundException, IOException {
        // Open inputFile.[
        final String filenameProperty = "script";
        final BufferedReader in = readFile(filenameProperty);

        // Reads the properties file.
        final Properties ini = new Properties();
        ini.load(new FileInputStream(System.getProperty("prop")));

        // File separator
        final char separator = ini.getProperty("separator").charAt(0);

        // Performs the database logic.
        executeScriptInDB(ini, in, separator);
    }

    /**
     * Process the file by executing each statement in the database.
     * 
     * @param stmt
     *            Established statement to the database.
     * @param sql
     *            Read statement from the file.
     * @param in
     *            Opened buffer to read the file.
     * @throws IOException
     *             If there is a problem reading the file.
     * @throws SQLException
     *             If there is a problem executing in the database.
     */
    private static void processFile(final Statement stmt, StringBuffer sql,
            final BufferedReader in, final char separator) throws IOException,
            SQLException {
        String rLine;
        // Loop thru input file and concatenate SQL statement fragments
        rLine = in.readLine();
        while (rLine != null) {

            String line = rLine.trim();

            if (line.length() != 0) {
                if (line.startsWith("--")) {
                    System.out.println(line); // print comment line
                } else {
                    sql.append(line);
                    if (line.endsWith(separator + "")) {
                        execSQL(stmt, sql, ';');
                        sql = new StringBuffer();
                    } else {
                        sql.append("\n");
                    }
                }
            }
            rLine = in.readLine();
        }
        in.close();
    }

    /**
     * Reads the given file and returns a buffer.
     * 
     * @param filenameProperty
     *            Name of the file to read.
     * @return Opened buffer to the file.
     * @throws FileNotFoundException
     *             If the file cannot be found.
     */
    private static BufferedReader readFile(final String filenameProperty)
            throws FileNotFoundException {
        final String filename = System.getProperty(filenameProperty);
        final BufferedReader in = new BufferedReader(new FileReader(filename));
        return in;
    }

    /**
     * Runs the process: Establishes the connection, processes the file, execute
     * each statement in the database.
     * 
     * @param ini
     *            Init properties to connect to the database.
     * @param in
     *            Buffer to read the file.
     */
    private static void executeScriptInDB(final Properties ini,
            final BufferedReader in, final char separator) {
        Connection conn = null;
        Statement stmt = null;
        StringBuffer sql = new StringBuffer();

        try {

            // Reads the conf and establishes the connection.
            conn = getConnectionFromConfiguration(ini);

            // Create Statement.
            stmt = conn.createStatement();

            processFile(stmt, sql, in, separator);

        } catch (final IOException ie) {
            System.out.println(ie.getMessage());
        } catch (final SQLException se) {
            System.out.println(se.getMessage());
            if (se.getNextException() != null) {
                System.out.println(se.getNextException().getMessage());
            }
        } catch (final Exception e) {
            e.printStackTrace();

            // Exit cleanly
        } finally {
            try {
                if (stmt != null) {
                    stmt.close();
                }
            } catch (final SQLException se) {
                se.printStackTrace();
            }
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (final SQLException se) {
                se.printStackTrace();
            }
        }
    }

}
