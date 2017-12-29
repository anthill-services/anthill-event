CREATE TABLE `event_participants` (
  `event_id` int(11) NOT NULL,
  `gamespace_id` int(11) NOT NULL,
  `account_id` int(11) NOT NULL,
  `group_id` int(11) NOT NULL DEFAULT '0',
  `participation_score` float NOT NULL DEFAULT '0',
  `participation_status` enum('NONE','JOINED','LEFT') NOT NULL DEFAULT 'NONE',
  `participation_profile` json NOT NULL,
  `participation_tournament_result` int(11) DEFAULT NULL,
  PRIMARY KEY (`gamespace_id`,`account_id`,`event_id`),
  KEY `fk_event_id_idx` (`event_id`),
  CONSTRAINT `fk_event_id` FOREIGN KEY (`event_id`) REFERENCES `events` (`event_id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
