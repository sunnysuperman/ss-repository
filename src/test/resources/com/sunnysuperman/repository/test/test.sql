CREATE TABLE `test_insert` (
  `id` BIGINT NOT NULL,
  `val` VARCHAR(100) NOT NULL COMMENT '测试值',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='测试插入数据';

CREATE TABLE `test_insert_generate_key` (
  `id` BIGINT AUTO_INCREMENT NOT NULL,
  `val` VARCHAR(100) NOT NULL COMMENT '测试值',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='测试插入数据生成自增ID';

CREATE TABLE `test_versioning_int` (
  `id` BIGINT AUTO_INCREMENT NOT NULL,
  `version` int NOT NULL COMMENT '版本号',
  `val` VARCHAR(100) NOT NULL COMMENT '测试值',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='测试版本管理(int)';

CREATE TABLE `test_versioning_long` (
  `id` BIGINT NOT NULL,
  `version` bigint NOT NULL COMMENT '版本号',
  `val` VARCHAR(100) NOT NULL COMMENT '测试值',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='测试版本管理(long+非自增ID)';

CREATE TABLE `test_insert_update` (
  `id` BIGINT NOT NULL,
  `v1` VARCHAR(100) DEFAULT NULL,
  `v2` VARCHAR(100) DEFAULT NULL,
  `v3` VARCHAR(100) DEFAULT NULL,
  `v4` VARCHAR(100) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='测试插入及修改';
