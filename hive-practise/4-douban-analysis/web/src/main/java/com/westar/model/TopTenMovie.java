package com.westar.model;

public class TopTenMovie {
    //电影主键
    private String movieId;
    //电影名称
    private String movieName;
    //电影地址
    private String url;
    //电影评分
    private float commentScore;

    public String getMovieId() {
        return movieId;
    }

    public void setMovieId(String movieId) {
        this.movieId = movieId;
    }

    public String getMovieName() {
        return movieName;
    }

    public void setMovieName(String movieName) {
        this.movieName = movieName;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public float getCommentScore() {
        return commentScore;
    }

    public void setCommentScore(float commentScore) {
        this.commentScore = commentScore;
    }
}
