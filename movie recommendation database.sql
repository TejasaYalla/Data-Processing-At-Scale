CREATE TABLE users(
userid INT PRIMARY KEY,
 name TEXT);

CREATE TABLE movies(
movieid INT PRIMARY KEY, 
title TEXT);

CREATE TABLE taginfo(
tagid INT PRIMARY KEY,
 content TEXT);

CREATE TABLE genres(
genreid INT PRIMARY KEY,
 name TEXT);
 
CREATE TABLE ratings(userid INT, 
movieid INT, 
PRIMARY KEY(userid,movieid), 
FOREIGN KEY(userid) REFERENCES users, 
FOREIGN KEY(movieid) REFERENCES movies, 
rating NUMERIC, 
CHECK (0<=rating AND rating<=5), 
timestamp BIGINT NOT NULL);
 
CREATE TABLE tags(userid INT, 
movieid INT, 
tagid INT, 
PRIMARY KEY(userid, movieid, tagid), 
FOREIGN KEY (userid) REFERENCES users, 
FOREIGN KEY(movieid) REFERENCES movies, 
FOREIGN KEY(tagid) REFERENCES taginfo, 
timestamp BIGINT NOT NULL);
 
CREATE TABLE hasagenre(movieid INT, 
genreid INT, 
PRIMARY KEY(movieid, genreid), 
FOREIGN KEY (movieid) REFERENCES movies, 
FOREIGN KEY(genreid) REFERENCES genres);
