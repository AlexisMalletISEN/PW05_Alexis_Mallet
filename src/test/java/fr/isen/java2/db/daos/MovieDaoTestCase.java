package fr.isen.java2.db.daos;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import fr.isen.java2.db.entities.Genre;
import fr.isen.java2.db.entities.Movie;

public class MovieDaoTestCase {
	
	private MovieDao movieDao = new MovieDao();
	
	@Before
	public void initDb() throws Exception {
		Connection connection = DataSourceFactory.getDataSource().getConnection();
		Statement stmt = connection.createStatement();
		stmt.executeUpdate(
				"CREATE TABLE IF NOT EXISTS genre (idgenre INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT , name VARCHAR(50) NOT NULL);");
		stmt.executeUpdate(
				"CREATE TABLE IF NOT EXISTS movie (\r\n"
				+ "  idmovie INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,\r\n" + "  title VARCHAR(100) NOT NULL,\r\n"
				+ "  release_date DATETIME NULL,\r\n" + "  genre_id INT NOT NULL,\r\n" + "  duration INT NULL,\r\n"
				+ "  director VARCHAR(100) NOT NULL,\r\n" + "  summary MEDIUMTEXT NULL,\r\n"
				+ "  CONSTRAINT genre_fk FOREIGN KEY (genre_id) REFERENCES genre (idgenre));");
		stmt.executeUpdate("DELETE FROM movie");
		stmt.executeUpdate("DELETE FROM genre");
		stmt.executeUpdate("INSERT INTO genre(idgenre,name) VALUES (1,'Drama')");
		stmt.executeUpdate("INSERT INTO genre(idgenre,name) VALUES (2,'Comedy')");
		stmt.executeUpdate("INSERT INTO movie(idmovie,title, release_date, genre_id, duration, director, summary) "
				+ "VALUES (1, 'Title 1', '2015-11-26 12:00:00.000', 1, 120, 'director 1', 'summary of the first movie')");
		stmt.executeUpdate("INSERT INTO movie(idmovie,title, release_date, genre_id, duration, director, summary) "
				+ "VALUES (2, 'My Title 2', '2015-11-14 12:00:00.000', 2, 114, 'director 2', 'summary of the second movie')");
		stmt.executeUpdate("INSERT INTO movie(idmovie,title, release_date, genre_id, duration, director, summary) "
				+ "VALUES (3, 'Third title', '2015-12-12 12:00:00.000', 2, 176, 'director 3', 'summary of the third movie')");
		stmt.close();
		connection.close();
	}
	
	 @Test
	 public void shouldListMovies() {
		// WHEN
		List<Movie> movies = movieDao.listMovies();
		// THEN
		assertThat(movies).hasSize(3);
		
			// Test the returned movies are the expected ones
		assertThat(movies.stream().allMatch(movie ->
        	movie.getId() == 1 && 
        	movie.getTitle().equals("Title 1") && 
        	movie.getReleaseDate().equals(LocalDate.of(2015, 11, 26)) && 
        	movie.getGenreId() == 1 && 
        	movie.getGenreName().equals("Drama") && 
        	movie.getDuration() == 120 && 
        	movie.getDirector().equals("director 1") && 
        	movie.getSummary().equals("summary of the first movie")
        	||
        	movie.getId() == 2 && 
        	movie.getTitle().equals("My Title 2") && 
        	movie.getReleaseDate().equals(LocalDate.of(2015, 11, 14)) && 
        	movie.getGenreId() == 2 && 
        	movie.getGenreName().equals("Comedy") && 
        	movie.getDuration() == 114 && 
        	movie.getDirector().equals("director 2") && 
        	movie.getSummary().equals("summary of the second movie")
            ||
            movie.getId() == 3 && 
            movie.getTitle().equals("Third title") && 
            movie.getReleaseDate().equals(LocalDate.of(2015, 12, 12)) && 
            movie.getGenreId() == 2 && 
            movie.getGenreName().equals("Comedy") && 
            movie.getDuration() == 176 && 
            movie.getDirector().equals("director 3") && 
            movie.getSummary().equals("summary of the third movie")));
	 }
	
	 @Test
	 public void shouldListMoviesByGenre() {
		 // WHEN
		 List<Movie> movies = movieDao.listMoviesByGenre("Comedy");
		 // THEN
		 assertThat(movies).hasSize(2);
		
			// Test the returned movies are the expected ones
		 assertThat(movies.stream().allMatch(movie ->
        	movie.getId() == 2 && 
        	movie.getTitle().equals("My Title 2") && 
        	movie.getReleaseDate().equals(LocalDate.of(2015, 11, 14)) && 
        	movie.getGenreId() == 2 && 
        	movie.getGenreName().equals("Comedy") && 
        	movie.getDuration() == 114 && 
        	movie.getDirector().equals("director 2") && 
        	movie.getSummary().equals("summary of the second movie")
            ||
            movie.getId() == 3 && 
            movie.getTitle().equals("Third title") && 
            movie.getReleaseDate().equals(LocalDate.of(2015, 12, 12)) && 
            movie.getGenreId() == 2 && 
            movie.getGenreName().equals("Comedy") && 
            movie.getDuration() == 176 && 
            movie.getDirector().equals("director 3") && 
            movie.getSummary().equals("summary of the third movie")));
	 }
	
	 @Test
	 public void shouldAddMovie() throws Exception {
		 // WHEN 
		 Movie myMovie = new Movie(null, "Title 4", LocalDate.of(2015, 11, 26), new Genre(1, "Drama"), 120, "director 4", "summary of the fourth movie");
		 Movie myMovieWithId = movieDao.addMovie(myMovie);
		 
		 // THEN
		 	// Test the returned movie ID
		 assertThat(myMovieWithId.getId()).isNotNull();
		 	// Test if the movie is in database
		 Connection connection = DataSourceFactory.getDataSource().getConnection();
		 Statement statement = connection.createStatement();
		 ResultSet resultSet = statement.executeQuery("SELECT * FROM movie WHERE title='Title 4'");
		 assertThat(resultSet.next()).isTrue();
		 assertThat(resultSet.getInt("idmovie")).isNotNull();
		 assertThat(resultSet.getString("title")).isEqualTo("Title 4");
		 assertThat(resultSet.getDate("release_date").toLocalDate()).isEqualTo(LocalDate.of(2015, 11, 26));
		 assertThat(resultSet.getInt("genre_id")).isEqualTo(1);
		 assertThat(resultSet.getInt("duration")).isEqualTo(120);
		 assertThat(resultSet.getString("director")).isEqualTo("director 4");
		 assertThat(resultSet.getString("summary")).isEqualTo("summary of the fourth movie");
		 assertThat(resultSet.next()).isFalse();
		 resultSet.close();
		 statement.close();
		 connection.close();
	 }
}
