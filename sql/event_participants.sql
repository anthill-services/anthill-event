CREATE TABLE `event_participants` (
  `event_id` int(11) NOT NULL,
  `gamespace_id` int(11) NOT NULL,
  `account_id` int(11) NOT NULL,
  `score` float NOT NULL DEFAULT '0',
  `status` enum('NONE','JOINED','LEFT') NOT NULL DEFAULT 'NONE',
  `custom` json NOT NULL,
  PRIMARY KEY (`gamespace_id`,`account_id`,`event_id`),
  KEY `fk_event_id_idx` (`event_id`),
  CONSTRAINT `fk_event_id` FOREIGN KEY (`event_id`) REFERENCES `events` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE=InnoDB DEFAULT CHARSET=utf8;