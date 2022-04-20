create table if not exists binance_blotter (
id serial,
trxn_time varchar(20) not null,
symbol varchar(10) not null,
trxn_price double precision not null,
trxn_size double precision not null,
constraint pk_binancebl_id primary key (id)
);

create table if not exists binance_L1 (
id serial,
trxn_time varchar(20) not null,
symbol varchar(10) not null,
bid double precision not null,
bid_size double precision not null,
ask double precision not null,
ask_size double precision not null,
constraint pk_binancebook_id primary key (id)
);

create table if not exists dydx_blotter (
id serial,
symbol varchar(10) not null,
trxn_size double precision not null,
side varchar(5) not null,
trxn_price double precision not null,
trxn_time timestamptz not null,
constraint pk_dydxbl_id primary key (id)
);

create table if not exists dydx_L1 (
id serial,
trxn_time varchar(20) not null,
symbol varchar(10) not null,
best_bid double precision not null,
bid_size double precision not null,
best_ask double precision not null,
ask_size double precision not null,
constraint pk_dydxbook_id primary key (id)
);

create table if not exists ftx_blotter (
id serial,
symbol varchar(10) not null,
trxn_price double precision not null,
trxn_size double precision not null,
side varchar(5) not null,
liquidation bool not null,
trxn_time timestamptz not null,
constraint pk_ftxbl_id primary key (id)
);

create table if not exists ftx_orderbook (
id serial,
symbol varchar(10) not null,
best_bid double precision not null,
bid_size double precision not null,
best_ask double precision not null,
ask_size double precision not null,
last_trxn_price double precision,
trxn_time varchar(30) not null,
best_bid_timestamp double precision,
best_ask_timestamp double precision,
bids text,
asks text;
constraint pk_ftxbook_id primary key (id)
);

create table if not exists ftxus_blotter (
id serial,
symbol varchar(10) not null,
trxn_price double precision not null,
trxn_size double precision not null,
side varchar(5) not null,
liquidation bool not null,
trxn_time timestamptz not null,
constraint pk_ftxusbl_id primary key (id)
);

create table if not exists ftxus_orderbook (
id serial,
symbol varchar(10) not null,
best_bid double precision not null,
bid_size double precision not null,
best_ask double precision not null,
ask_size double precision not null,
last_trxn_price double precision,
trxn_time varchar(30) not null,
constraint pk_ftxusbook_id primary key (id)
);

create table if not exists kraken_blotter (
id serial,
symbol varchar(10) not null,
trxn_price double precision not null,
volume double precision not null,
trxn_time varchar(20) not null,
side varchar(5) not null,
order_type varchar(5) not null,
constraint pk_krakenbl_id primary key (id)
);

create table if not exists kraken_L1 (
id serial,
trxn_time varchar(20) not null,
symbol varchar(10) not null,
best_bid double precision not null,
bid_size double precision not null,
best_ask double precision not null,
ask_size double precision not null,
constraint pk_krakenbook_id primary key (id)
);

create table if not exists kraken_orderbook (
id serial,
symbol varchar(10) not null,
best_bid double precision not null,
bid_size double precision not null,
best_ask double precision not null,
ask_size double precision not null,
last_trxn_price double precision,
trxn_time varchar(30) not null,
best_bid_timestamp double precision,
best_ask_timestamp double precision,
bids text,
asks text;
constraint pk_ftxbook_id primary key (id)
);


drop table if exists binance_blotter;
drop table if exists binance_l1;
drop table if exists dydx_L1;
drop table if exists dydx_blotter;
drop table if exists ftx_blotter;
drop table if exists ftx_L1;
drop table if exists kraken_blotter;
drop table if exists kraken_L1;