public class Song {
    private int id;
    private int track;
    private String name;
    private int albumId;

    public int id() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int track() {
        return track;
    }

    public void setTrack(int track) {
        this.track = track;
    }

    public String name() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int albumId() {
        return albumId;
    }

    public void setAlbumId(int albumId) {
        this.albumId = albumId;
    }
}
