import java.sql.*;
import java.util.ArrayList;

import java.util.List;

public class DB {
    public static final String CONNECTION = "jdbc:mysql://localhost:3306/newdb";
    private static Connection conn;

    public static final String TABLE_ALBUM = "albums";
    public static final String COLUMN_ALBUM_ID = "id";
    public static final String COLUMN_ALBUM_NAME = "name";
    public static final String COLUMN_ALBUM_ARTIST = "artist";
    public static final int INDEX_ALBUM_ID = 1;
    public static final int INDEX_ALBUM_NAME = 2;
    public static final int INDEX_ALBUM_ARTIST = 3;
    public static final String TABLE_ARTIST = "artists";
    public static final String COLUMN_ARTIST_ID = "id";
    public static final String COLUMN_ARTIST_NAME = "name";
    public static final int ARTIST_ARTIST_ID = 1;
    public static final int ARTIST_ARTIST_NAME = 2;

    public static final String TABLE_SONGS = "songs";
    public static final String COLUMN_SONGS_TRACK = "track";
    public static final String COLUMN_SONGS_TITLE = "name";
    public static final String COLUMN_SONGS_ALBUM = "albumID";
    public static final int ARTIST_SONG_ID = 1;
    public static final int ARTIST_SONG_TRACK = 2;
    public static final int ARTIST_SONG_TITLE = 3;
    public static final int ARTIST_SONG_ALBUM = 4;

    public static final int ORDER_BY_NONE = 1;
    public static final int ORDER_BY_ASC = 2;
    public static final int ORDER_BY_DESC = 3;

    public static final String TABLE_ARTIST_SONG_VIEW = "artist_list";
    public static final String QUERY_ALBUMS_BY_ARTIST_SORT =
            " ORDER BY " + TABLE_ALBUM + "." + COLUMN_ALBUM_NAME + " COLLATE NOCASE ";
    public static final String QUERY_ALBUMS_BY_ARTIST_START =
            "SELECT " + TABLE_ALBUM + '.' + COLUMN_ALBUM_NAME + " FROM " + TABLE_ALBUM +
                    " INNER JOIN " + TABLE_ARTIST + " ON " + TABLE_ALBUM + "." + COLUMN_ALBUM_ARTIST +
                    " = " + TABLE_ARTIST + "." + COLUMN_ARTIST_ID +
                    " WHERE " + TABLE_ARTIST + "." + COLUMN_ARTIST_NAME + " = \"";

    public static final String CREATE_ARTIST_FOR_SONG_VIEW = "CREATE VIEW IF NOT EXISTS " +
            TABLE_ARTIST_SONG_VIEW + " AS SELECT " + TABLE_ARTIST + "." + COLUMN_ARTIST_NAME + ", " +
            TABLE_ALBUM + "." + COLUMN_ALBUM_NAME + " AS " + COLUMN_SONGS_ALBUM + ", " +
            TABLE_SONGS + "." + COLUMN_SONGS_TRACK + ", " + TABLE_SONGS + "." + COLUMN_SONGS_TITLE +
            " FROM " + TABLE_SONGS +
            " INNER JOIN " + TABLE_ALBUM + " ON " + TABLE_SONGS +
            "." + COLUMN_SONGS_ALBUM + " = " + TABLE_ALBUM + "." + COLUMN_ALBUM_ID +
            " INNER JOIN " + TABLE_ARTIST + " ON " + TABLE_ALBUM + "." + COLUMN_ALBUM_ARTIST +
            " = " + TABLE_ARTIST + "." + COLUMN_ARTIST_ID +
            " ORDER BY " +
            TABLE_ARTIST + "." + COLUMN_ARTIST_NAME + ", " +
            TABLE_ALBUM + "." + COLUMN_ALBUM_NAME + ", " +
            TABLE_SONGS + "." + COLUMN_SONGS_TRACK;

    public boolean open() throws ClassNotFoundException {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            conn = DriverManager.getConnection(CONNECTION, "root", "");

            return true;
        } catch (SQLException e) {
            System.out.println("could not connection");
            return false;
        }
    }

    public void close() {
        try {
            if (conn != null)
                conn.close();
        } catch (SQLException e) {
            System.out.println("could not close connection");
        }
    }

    public List<Artist> getArtists() {
        List<Artist> list = new ArrayList<>();
        String s = ("SELECT * FROM " + TABLE_ARTIST);
        try (Statement statement = conn.createStatement();
             ResultSet resultSet = statement.executeQuery(s);
        ) {
            while (resultSet.next()) {
                Artist artist = new Artist();
                artist.setId(resultSet.getInt(ARTIST_ARTIST_ID));
                artist.setName(resultSet.getString(ARTIST_ARTIST_NAME));
                list.add(artist);
            }

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        return list;
    }


    public List<Song> allSongs() {
        List<Song> songs = new ArrayList<>();
        String str = "SELECT * FROM " + TABLE_SONGS;
        try (Statement statement = conn.createStatement();
             ResultSet resultSet = statement.executeQuery(str);
        ) {
            while (resultSet.next()) {
                Song song = new Song();
                song.setId(resultSet.getInt(ARTIST_SONG_ID));
                song.setName(resultSet.getString(COLUMN_SONGS_TITLE));
                song.setTrack(resultSet.getInt(ARTIST_SONG_TRACK));
                song.setAlbumId(resultSet.getInt(COLUMN_SONGS_ALBUM));
                songs.add(song);

            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());

        }

        return songs;
    }
    public List<Song> getArtistSongs(String s){
        List<Song>songs=new ArrayList<>();
        String s1="SELECT  * FROM songs" +
                " INNER JOIN artists ON artists.name='"+s+"'" +
                " INNER JOIN albums ON  albums.artistID = artists.id "
                +" WHERE  songs.albumID =albums.id";

        try (Statement statement = conn.createStatement();
             ResultSet resultSet = statement.executeQuery(s1);
        ) {
            while (resultSet.next()) {
                Song song = new Song();
                song.setId(resultSet.getInt(ARTIST_SONG_ID));
                song.setName(resultSet.getString(COLUMN_SONGS_TITLE));
                song.setTrack(resultSet.getInt(ARTIST_SONG_TRACK));
                song.setAlbumId(resultSet.getInt(COLUMN_SONGS_ALBUM));
                songs.add(song);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return songs;
    }
    public List<String> queryAlbumsForArtist(String artistName, int sortOrder) {

        StringBuilder sb = new StringBuilder(QUERY_ALBUMS_BY_ARTIST_START);
        sb.append(artistName);
        sb.append("\"");

        if (sortOrder != ORDER_BY_NONE) {
            sb.append(QUERY_ALBUMS_BY_ARTIST_SORT);
            if (sortOrder == ORDER_BY_DESC) {
                sb.append("DESC");
            } else {
                sb.append("ASC");
            }
        }

        System.out.println("SQL statement = " + sb.toString());

        try (Statement statement = conn.createStatement();
             ResultSet results = statement.executeQuery(sb.toString())) {

            List<String> albums = new ArrayList<>();
            while (results.next()) {
                albums.add(results.getString(1));
            }

            return albums;

        } catch (SQLException e) {
            System.out.println("Query failed: " + e.getMessage());
            return null;
        }
    }

public void querySongMetaData(){
String sql="SELECT * FROM "+TABLE_SONGS;
try(Statement statement=conn.createStatement();
ResultSet resultSet=statement.executeQuery(sql)) {
      ResultSetMetaData metaData=resultSet.getMetaData();
      int numColumns=metaData.getColumnCount();
    for (int i = 1; i <=numColumns ; i++) {
System.out.format("Column %d in the songs table is names %s\n",
        i,metaData.getColumnName(i));

    }
} catch (SQLException e) {
    System.out.println(e.getMessage());
}
}


    public void insertArtist(String name) {
        if (name != null) {
            String s = "INSERT INTO " + TABLE_ARTIST +
                    " VALUES ( " + null + ",'" + name + "' )";
            try (Statement statement = conn.createStatement()) {
                statement.execute(s);
                System.out.println("have added");
            } catch (SQLException e) {
                System.out.println(e.getMessage());

            }
        }
    }

    public void insertAlbum(String name, int artistID) {
        if (checkArtist(artistID) && name != null) {
            String s = "INSERT INTO " + TABLE_ALBUM +
                    " VALUES ( " + null + ",'" + name + "' , " + artistID + " )";
            try (Statement statement = conn.createStatement()) {
                statement.execute(s);
                System.out.println("Added");
            } catch (SQLException e) {
                System.out.println(e.getMessage());

            }
        }
    }

    public void insertSong(int track, String name, int albumID) {
        if (checkAlbum(albumID) && name != null) {
            String s = "INSERT INTO " + TABLE_SONGS +
                    " VALUES ( " + null + ", '" + track + "', '" + name + "','" + albumID + "' )";
            try (Statement statement = conn.createStatement()) {
                statement.execute(s);
                System.out.println("added");
            } catch (SQLException e) {
                System.out.println(e.getMessage());

            }
        }
    }

    public <T>boolean checkAlbum(T id) {
        if (id instanceof Integer) {
            String s = "SELECT * FROM " + TABLE_ALBUM + " where id= " + id;
            try (Statement statement = conn.createStatement();
                 ResultSet resultSet = statement.executeQuery(s)) {
                while (resultSet.next())
                    if (resultSet.getString(COLUMN_ALBUM_NAME) != null)
                        return true;
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }else{
            if (id instanceof String){
                String s = "SELECT * FROM " + TABLE_ALBUM + " where name= '" + id+"'";
                try (Statement statement = conn.createStatement();
                     ResultSet resultSet = statement.executeQuery(s)) {
                    while (resultSet.next())
                        if (resultSet.getString(COLUMN_ALBUM_NAME) != null)
                            return true;
                } catch (SQLException e) {
                    System.out.println(e.getMessage());

                }
            }
        }
        return false;
    }

    public <T> boolean checkArtist(T id) {
        if (id instanceof Integer) {
            String s = "SELECT * FROM " + TABLE_ARTIST + " where id= " + id;
            try (Statement statement = conn.createStatement();
                 ResultSet resultSet = statement.executeQuery(s)) {
                while (resultSet.next())
                    if (resultSet.getString(ARTIST_ARTIST_NAME) != null)
                        return true;
            } catch (SQLException e) {
                System.out.println(e.getMessage());

            }
        } else if (id instanceof String) {
            String s = "SELECT * FROM " + TABLE_ARTIST + " where name= '" + id + "'";
            try (Statement statement = conn.createStatement();
                 ResultSet resultSet = statement.executeQuery(s)) {
                while (resultSet.next())
                    if (resultSet.getString(ARTIST_ARTIST_NAME) != null)
                        return true;
            } catch (SQLException e) {
                System.out.println(e.getMessage());

            }
        }
        return false;

    }
    public <T> boolean checkSong(T id) {
        if (id instanceof Integer) {
            String s = "SELECT * FROM " + TABLE_SONGS + " where id= " + id;
            try (Statement statement = conn.createStatement();
                 ResultSet resultSet = statement.executeQuery(s)) {
                while (resultSet.next())
                    if (resultSet.getString(ARTIST_ARTIST_NAME) != null)
                        return true;
            } catch (SQLException e) {
                System.out.println(e.getMessage());

            }
        } else if (id instanceof String) {
            String s = "SELECT * FROM " + TABLE_SONGS + " where name= '" + id + "'";
            try (Statement statement = conn.createStatement();
                 ResultSet resultSet = statement.executeQuery(s)) {
                while (resultSet.next())
                    if (resultSet.getString(COLUMN_SONGS_TITLE) != null)
                        return true;
            } catch (SQLException e) {
                System.out.println(e.getMessage());

            }
        }
        return false;

    }


    public <T> void deleteArtist(T t) {
        if (t instanceof Integer) {
            if (checkArtist((Integer) t)) {
                String str = "DELETE FROM " + TABLE_ARTIST +
                        " WHERE id =" + t;
                try {
                    Statement statement = conn.createStatement();
                    statement.execute(str);
                    System.out.println("deleted");
                } catch (SQLException e) {
                    System.out.println(e.getMessage());

                }

            }
        } else if (t instanceof String) {
            String str = "DELETE FROM " + TABLE_ARTIST +
                    " WHERE name = '" + t + "'";
            if (checkArtist((String) t)) {
                try {
                    Statement statement = conn.createStatement();
                    statement.execute(str);
                    System.out.println("deleted");
                } catch (SQLException e) {
                    System.out.println(e.getMessage());
                }
            }
        } else {
            System.out.println("nothing");
        }

    }

    public <T> void  deleteAlbum(T t){
if (checkAlbum(t)){
    if (t instanceof Integer){
        String str = "DELETE FROM " + TABLE_ALBUM +
                " WHERE id =" + t;
        try {
            Statement statement = conn.createStatement();
            statement.execute(str);
            System.out.println("deleted");
        } catch (SQLException e) {
            System.out.println(e.getMessage());

        }
    }else {
        String str = "DELETE FROM " + TABLE_ARTIST +
                " WHERE name = '" + t + "'";
        try {
            Statement statement = conn.createStatement();
            statement.execute(str);
            System.out.println("deleted");
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }

}}else
    System.out.println("nothing");
    }
    public <T> void deleteSong(T t){
        if (checkSong(t)){
            if (t instanceof Integer){
                String str = "DELETE FROM " + TABLE_SONGS +
                        " WHERE id =" + t;
                try {
                    Statement statement = conn.createStatement();
                    statement.execute(str);
                    System.out.println("deleted");
                } catch (SQLException e) {
                    System.out.println(e.getMessage());

                }
            }else {
                String str = "DELETE FROM " + TABLE_SONGS +
                        " WHERE name = '" + t + "'";
                try {
                    Statement statement = conn.createStatement();
                    statement.execute(str);
                    System.out.println("deleted");
                } catch (SQLException e) {
                    System.out.println(e.getMessage());

                }

            }}else
            System.out.println("nothing");
    }
    public void test(){
        String s="SELECT * FROM "+TABLE_SONGS +
                " LIMIT 2";
        try (Statement statement = conn.createStatement();
             ResultSet resultSet = statement.executeQuery(s)) {
            while (resultSet.next()){
                System.out.println(resultSet.getString(COLUMN_SONGS_TITLE));
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());


        }


    }
    public  boolean createViewForSongArtists(){
        try(Statement statement=conn.createStatement();) {
            statement.execute(CREATE_ARTIST_FOR_SONG_VIEW);
            return true;
        } catch (SQLException e) {
return false;
        }
    }


}