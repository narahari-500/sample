package com.serverless;
import java.sql.*;

public class DBConn {

    private static Connection conn = null;

    private String url =
            "jdbc:mysql://finderio-dev-cluster-id.cluster-cxp9nggxovkq.us-east-1.rds.amazonaws.com:3306/finderio?rewriteBatchedStatements=true";
    private String username = "finderiodev";
    private String password = "samba123";

    static{
        try {
            new DBConn();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private DBConn() throws ClassNotFoundException, SQLException {
        System.out.println("Creating connection object");
        Class.forName("com.mysql.cj.jdbc.Driver");
        conn = DriverManager.getConnection(url, username, password);
        System.out.println("Creation of connection completed!!!");
    }

    public static Connection getConnection() {
        return conn;
    }
}
