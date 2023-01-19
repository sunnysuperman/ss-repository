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