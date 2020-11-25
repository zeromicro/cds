create database if not exists galaxy;
use galaxy;
CREATE TABLE if not exists `user` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `group_id` bigint(20) NOT NULL,
  `name` varchar(36) NOT NULL,
  `email` varchar(36) NOT NULL,
  `password` varchar(36) NOT NULL,
  `token` varchar(36) NOT NULL,
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `group_id_idx` (`group_id`),
  KEY `email_idx` (`email`),
  KEY `token_idx` (`token`)
) ENGINE=InnoDB AUTO_INCREMENT=20 DEFAULT CHARSET=utf8mb4;

INSERT INTO galaxy.user (group_id, name, email, password, token) VALUES (0, 'admin', 'admin@email.com', '123456', '');

CREATE TABLE if not exists `dm` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `name` varchar(64) NOT NULL,
  `source_type` varchar(64) NOT NULL,
  `source_dsn` text NOT NULL,
  `source_db` varchar(32) NOT NULL,
  `source_table` varchar(64) NOT NULL,
  `source_query_key` varchar(64) NOT NULL,
  `target_type` text NOT NULL,
  `target_shards` text NOT NULL,
  `target_db` varchar(64) NOT NULL,
  `target_ch_proxy` text NOT NULL,
  `target_table` varchar(64) NOT NULL,
  `window_start_hour` bigint(20) NOT NULL,
  `window_end_hour` bigint(20) NOT NULL,
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `suffix` varchar(45) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `target_db` (`target_db`,`target_table`),
  KEY `name_idx` (`name`)
) ENGINE=InnoDB AUTO_INCREMENT=195 DEFAULT CHARSET=utf8mb4;

CREATE TABLE if not exists `rtu` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `name` varchar(64) NOT NULL,
  `source_type` varchar(64) NOT NULL,
  `source_dsn` text NOT NULL,
  `source_table` varchar(64) NOT NULL,
  `source_db` varchar(64) NOT NULL,
  `source_query_key` varchar(64) NOT NULL,
  `source_topic` text NOT NULL,
  `target_type` text NOT NULL,
  `target_shards` text NOT NULL,
  `target_db` varchar(64) NOT NULL,
  `target_ch_proxy` text NOT NULL,
  `target_table` varchar(64) NOT NULL,
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `status` text NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `target_db` (`target_db`,`target_table`),
  KEY `name_idx` (`name`)
) ENGINE=InnoDB AUTO_INCREMENT=100 DEFAULT CHARSET=utf8mb4;

CREATE TABLE if not exists `connector` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `source_type` varchar(64) NOT NULL,
  `source_table` varchar(64) NOT NULL,
  `source_db` varchar(64) NOT NULL,
  `source_id` varchar(64) NOT NULL,
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `db_idx` (`source_db`)
) ENGINE=InnoDB AUTO_INCREMENT=123 DEFAULT CHARSET=utf8mb4;

create user canal@'%' IDENTIFIED by 'canal';
GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT,SUPER ON *.* TO 'canal'@'%';
FLUSH PRIVILEGES;