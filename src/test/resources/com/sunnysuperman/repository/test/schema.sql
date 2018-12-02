CREATE TABLE `test_device` (
  `id` varchar(45) NOT NULL,
  `created_at` bigint NOT NULL,
  `name` varchar(100) NOT NULL,
  `notes` varchar(500),
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;