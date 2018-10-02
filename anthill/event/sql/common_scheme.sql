CREATE TABLE `common_scheme` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `gamespace_id` int(11) NOT NULL,
  `scheme_json` json DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;