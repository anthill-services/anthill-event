CREATE TABLE `event_group_participants` (
  `event_id` int(11) NOT NULL,
  `gamespace_id` int(11) NOT NULL,
  `group_id` int(11) NOT NULL,
  `group_participation_score` float NOT NULL DEFAULT '0',
  `group_participation_status` enum('NONE','JOINED','LEFT') NOT NULL DEFAULT 'NONE',
  `group_participation_profile` json NOT NULL,
  `group_participation_tournament_result` int(11) DEFAULT NULL,
  PRIMARY KEY (`gamespace_id`,`group_id`,`event_id`),
  KEY `fk_event_group_id_idx` (`event_id`),
  CONSTRAINT `fk_event_group_id` FOREIGN KEY (`event_id`) REFERENCES `events` (`event_id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
