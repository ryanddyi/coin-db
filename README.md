# coin-db

create mysql database in the format:
create table coindb.core (date DATE, asset VARCHAR(10), variable VARCHAR(10), value DOUBLE, PRIMARY KEY (date, asset, variable))
