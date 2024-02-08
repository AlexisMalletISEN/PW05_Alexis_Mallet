package fr.isen.java2.db.daos;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import fr.isen.java2.db.entities.Genre;
import fr.isen.java2.db.entities.Movie;

public class MovieDao {

	// list all movies from the database
	public List<Movie> listMovies() {
		List<Movie> listOfMovies = new ArrayList<>();
		
		try (Connection connection = DataSourceFactory.getDataSource().getConnection()){
			try (Statement statement = connection.createStatement()){
				try (ResultSet results = statement.executeQuery("SELECT * FROM movie JOIN genre ON movie.genre_id = genre.idgenre")){
					while(results.next()) {
						Genre movieGenre = new Genre(results.getInt("genre_id"), results.getString("name"));
						
						Movie movie = new Movie(results.getInt("idmovie"), 
												results.getString("title"), 
												results.getDate("release_date").toLocalDate(),
												movieGenre,
												results.getInt("duration"),
												results.getString("director"),
												results.getString("summary"));
						listOfMovies.add(movie);
					}
				}
			}
			return listOfMovies;
		} catch(SQLException e) {
			e.printStackTrace();
			return new ArrayList<>(); // return an empty list if there's an error
		}
		
	}

	// list all movies from the database by genre
	public List<Movie> listMoviesByGenre(String genreName) {
		List<Movie> listOfMovies = new ArrayList<>();
		
		try (Connection connection = DataSourceFactory.getDataSource().getConnection()){
			try (PreparedStatement statement = connection.prepareStatement("SELECT * FROM movie JOIN genre ON movie.genre_id = genre.idgenre WHERE genre.name = ?")){
				statement.setString(1, genreName);
				try (ResultSet results = statement.executeQuery()){
					while(results.next()) {
						Genre movieGenre = new Genre(results.getInt("genre_id"), results.getString("name"));
						
						Movie movie = new Movie(results.getInt("idmovie"), 
												results.getString("title"), 
												results.getDate("release_date").toLocalDate(),
												movieGenre,
												results.getInt("duration"),
												results.getString("director"),
												results.getString("summary"));
						listOfMovies.add(movie);
					}
				}
			}
			return listOfMovies;
		} catch(SQLException e) {
			e.printStackTrace();
			return new ArrayList<>(); // return an empty list if there's an error
		}
	}

	// add a movie to the database
	public Movie addMovie(Movie movie) {
		try (Connection connection = DataSourceFactory.getDataSource().getConnection()){
			String sqlQuery = "INSERT INTO movie(title,release_date,genre_id,duration,director,summary) VALUES(?,?,?,?,?,?)";
			try (PreparedStatement statement = connection.prepareStatement(sqlQuery, Statement.RETURN_GENERATED_KEYS)){
				statement.setString(1, movie.getTitle());
				statement.setDate(2, java.sql.Date.valueOf(movie.getReleaseDate()));
				statement.setInt(3, movie.getGenreId());
				statement.setInt(4, movie.getDuration());
				statement.setString(5, movie.getDirector());
				statement.setString(6, movie.getSummary());
				statement.executeUpdate();
				
				// Get the id generated and put in the object movie
				try(ResultSet generatedKey = statement.getGeneratedKeys()){
					if(generatedKey.next()) {
						int id = generatedKey.getInt(1);
						movie.setId(id);
					}
				}
			}
			return movie; // return the movie with the id
		} catch(SQLException e) {
			e.printStackTrace();
			return null; // return null if there's an error
		}
	}
}
