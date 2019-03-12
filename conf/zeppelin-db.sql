
DROP TABLE users;
CREATE TABLE users (
  id int(11) NOT NULL AUTO_INCREMENT,
  username varchar(100) NOT NULL,
  password varchar(100) NOT NULL,
  db_create_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  db_update_time timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
  PRIMARY KEY (id),
  UNIQUE KEY username_UNIQUE (username)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;


DROP TABLE user_roles;
CREATE TABLE user_roles (
  id int(11) NOT NULL AUTO_INCREMENT,
  username varchar(100) NOT NULL,
  role_name varchar(100) NOT NULL,
  db_create_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  db_update_time timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
  PRIMARY KEY (id),
  UNIQUE KEY username_UNIQUE (username)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;

DROP TABLE user_kerberos;
create table user_kerberos
(
  id int(11) NOT NULL AUTO_INCREMENT,
  username varchar(100) NOT NULL,
  principal varchar(100) NOT NULL,
  keytab blob,
  db_create_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  email varchar(100) NOT NULL,
  `clusters` mediumtext,
  primary key (id),
  UNIQUE KEY username_UNIQUE (username)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;


insert into users(username, password) values('hzliuxun', '1');
insert into user_roles(username, role_name) values('hzliuxun', 'admin');


insert into users(username, password) values('hzluzhonghao', '1');
insert into user_roles(username, role_name) values('hzluzhonghao', 'bi');
