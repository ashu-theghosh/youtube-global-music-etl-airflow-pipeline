show databases;

use global_youtube_music_details;

create database global_youtube_music_details
CHARACTER SET = utf8mb4
COLLATE = utf8mb4_unicode_ci;

show tables;

create table video_details(
video_details_id int auto_increment Primary key,
processed_at DATE DEFAULT (CURRENT_DATE),
video_id varchar(500),
published_at datetime,
channel_id varchar(500),
song_title longtext,
song_description longtext,
song_thumbnail longtext,
channel_title varchar(500))
CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci;

CREATE USER 'appuser'@'%' IDENTIFIED BY 'ashpika';
GRANT ALL PRIVILEGES ON global_youtube_music_details.* TO 'appuser'@'%';
FLUSH PRIVILEGES;

describe video_details;


SHOW VARIABLES LIKE 'character_set%';
SHOW VARIABLES LIKE 'collation%';

select * from video_details;

drop table video_details;

drop table video_region;

delete from video_details where video_details_id=1;

SELECT * FROM video_details WHERE video_id='MA1qUaupvYA';

create table video_regions(
video_region_id int auto_increment primary key,
processed_at DATE DEFAULT (CURRENT_DATE),
video_details_id int,
country_name varchar(300),
country_code varchar(200),
continent_region varchar(300),
FOREIGN KEY (video_details_id) REFERENCES video_details(video_details_id)
)CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci;


show tables;

select * from video_regions;

describe video_regions;
drop table video_regions;

truncate video_regions;
truncate video_details;
truncate tags_table;

drop table video_details;
drop table video_regions;
drop table tags_table;

show tables;

CREATE TABLE tags_table (
    video_tag_id INT AUTO_INCREMENT PRIMARY KEY,
    video_details_id INT,
    processed_at DATE DEFAULT (CURRENT_DATE),
    tag TEXT,
FOREIGN KEY (video_details_id) REFERENCES video_details(video_details_id))
CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci;

describe tags_table;

select * from tags_table;

select * from video_details;

select * from video_regions;

select * from video_metrics;

SELECT tag FROM tags_table where video_details_id=409 and video_details_id=396;

show tables;

select country_code, country_name from video_regions where video_region_id in (1,48,89,119,405);

create table video_metrics(
 video_metrics_id int auto_increment Primary key,
 video_details_id int,
 video_region_id int,
 processed_at DATE DEFAULT (CURRENT_DATE),
 view_count bigint,
 like_count bigint,
 favourite_count bigint,
 comment_count bigint,
 FOREIGN KEY (video_details_id) REFERENCES video_details(video_details_id),
 FOREIGN KEY (video_region_id) REFERENCES video_regions(video_region_id))
 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci;
 