package fr.isen.java2.db.daos;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import fr.isen.java2.db.entities.Genre;

public class GenreDao {

	// list all genres from the database
	public List<Genre> listGenres() {
		List<Genre> listOfGenres = new ArrayList<>();
		
		try (Connection connection = DataSourceFactory.getDataSource().getConnection()){
			try (Statement statement = connection.createStatement()){
				try (ResultSet results = statement.executeQuery("SELECT * FROM genre")){
					while(results.next()) {
						Genre genre = new Genre(results.getInt("idgenre"), results.getString("name"));
						listOfGenres.add(genre);
					}
				}
			}
			return listOfGenres;
		} catch(SQLException e) {
			e.printStackTrace();
			return new ArrayList<>(); // return an empty list if there's an error
		}
	}

	// get a genre by name from the database
	public Genre getGenre(String name) {
		
		Genre genre = new Genre();
				
		try (Connection connection = DataSourceFactory.getDataSource().getConnection()){
			try (PreparedStatement statement = connection.prepareStatement("SELECT * FROM genre WHERE name = ?")){
				statement.setString(1, name);
				try (ResultSet results = statement.executeQuery()){
					if (results.next()) {
						genre.setId(results.getInt("idgenre"));
						genre.setName(results.getString("name"));
						return genre;
					} else {
						return null; // return null if the genre is not found
					}
				}
			}
		} catch(SQLException e) {
			e.printStackTrace();
			return null; // return null if there's an error
		}
	}

	// add a genre to the database
	public void addGenre(String name) {
		try (Connection connection = DataSourceFactory.getDataSource().getConnection()){
			String sqlQuery = "INSERT INTO genre(name) VALUES(?)";
			try (PreparedStatement statement = connection.prepareStatement(sqlQuery, Statement.RETURN_GENERATED_KEYS)){
				statement.setString(1, name);
				statement.executeUpdate();
			}
		} catch(SQLException e) {
			e.printStackTrace();
		}
	}
}
