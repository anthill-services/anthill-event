CREATE TABLE `events` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `status` varchar(64) NOT NULL DEFAULT 'none',
  `category_id` int(11) unsigned NOT NULL,
  `category_name` varchar(155) DEFAULT NULL,
  `gamespace_id` int(11) unsigned NOT NULL,
  `start_dt` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `end_dt` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `data_json` json NOT NULL,
  `enabled` enum('true','false') NOT NULL DEFAULT 'false',
  `tournament` enum('true','false') NOT NULL DEFAULT 'false',
  PRIMARY KEY (`id`),
  KEY `category_id` (`category_id`),
  CONSTRAINT `events_ibfk_1` FOREIGN KEY (`category_id`) REFERENCES `category_scheme` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;