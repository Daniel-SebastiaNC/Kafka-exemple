package br.com.alura;

import java.sql.*;

public class LocalDatabase {

    private final Connection connection;

    public LocalDatabase(String name) throws SQLException {
        String url = "jdbc:sqlite:target/"+ name +".db";
        this.connection = DriverManager.getConnection(url);
    }

    public void createIfNotExists(String sql) {
        try {
            connection.createStatement().execute(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void update(String statement, String ... params) throws SQLException {
        var preparedStatement = prepare(this.connection, statement, params);
        preparedStatement.execute();
    }

    public ResultSet query(String statement, String ... params) throws SQLException {
        var preparedStatement = prepare(connection, statement, params);

        return preparedStatement.executeQuery();

    }

    private PreparedStatement prepare(Connection connection, String statement, String[] params) throws SQLException {
        var preparedStatement = connection.prepareStatement(statement);
        for (int i = 0; i < params.length; i++) {
            preparedStatement.setString(i + 1, params[i]);
        }
        return preparedStatement;
    }

    public void close() throws SQLException {
        connection.close();
    }
}
