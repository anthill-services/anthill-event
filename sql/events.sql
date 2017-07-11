CREATE TABLE `events` (
  `event_id` int(11) NOT NULL AUTO_INCREMENT,
  `event_status` varchar(64) NOT NULL DEFAULT 'none',
  `event_processing` tinyint(1) NOT NULL DEFAULT '0',
  `category_id` int(11) unsigned NOT NULL,
  `category_name` varchar(155) DEFAULT NULL,
  `gamespace_id` int(11) unsigned NOT NULL,
  `event_start_dt` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `event_end_dt` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `event_payload` json NOT NULL,
  `event_enabled` tinyint(1) NOT NULL DEFAULT '1',
  `event_flags` set('CLUSTERED','TOURNAMENT','GROUP') NOT NULL DEFAULT '',
  `event_end_action` enum('NONE','MESSAGE','EXEC') NOT NULL DEFAULT 'NONE',
  PRIMARY KEY (`event_id`),
  KEY `category_id` (`category_id`),
  CONSTRAINT `events_ibfk_1` FOREIGN KEY (`category_id`) REFERENCES `category_scheme` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
