postgresql에 대용량의 narrow type의 table이 있는데 이 데이터를 wide type의 table로 Migration하는 프로젝트를 생성해야 한다.
narrow type의 table schema는 아래와 같다.

CREATE TABLE tenant.tbl_data_timeseries (
	id bigserial NOT NULL,
	ship_id text NOT NULL,
	data_channel_id text NOT NULL,
	created_time timestamp NOT NULL,
	server_created_time timestamp NULL,
	bool_v bool NULL,
	str_v text NULL,
	long_v int8 NULL,
	double_v float8 NULL,
	value_format varchar(50) NULL,
	CONSTRAINT data_timeseries_pk PRIMARY KEY (ship_id, data_channel_id, created_time)
)
WITH (
	autovacuum_vacuum_scale_factor=0.0,
	autovacuum_vacuum_threshold=100000
);
CREATE INDEX tbl_data_timeseries_created_time_idx ON tenant.tbl_data_timeseries USING btree (created_time DESC);

wide type의 데이블을 만들어야 되는데
테이블명 규칙은 tbl_data_timeseries_에다가 ship_id를 붙여서 만들면 된다. 예를 들면 아래와 같아.
tbl_data_timeseries_IMO9976903
tbl_data_timeseries_IMO9976915
tbl_data_timeseries_IMO9976927
tbl_data_timeseries_IMO9976939
tbl_data_timeseries_IMO9986051
tbl_data_timeseries_IMO9986063
tbl_data_timeseries_IMO9986087
tbl_data_timeseries_IMO9986104
컬럼은 created_time이 들어가야하고 나머지는 data_channel_id의 값이 테이블의 컬럼으로 생성이 되어야 한다. 이 컬럼들은 text type으로 하자.
value_format에 따라서 bool_v, str_v, long_v, double_v의 값을 넣어 주면 된다.
규칙은 value_format은 
Decimal이면 dobule_v컬럼의 값
Integer이면 long_v컬럼의 값
String이면 str_v컬럼의 값
Boolean이면 bool_v컬럼의 값이다.

index는 당연히 created_time이 되어야한다.

현재 실시간(1분 간격으로 15초 데이터가 들어오고 있어)으로 데이터가 쌓이고 있으니 
특정 시같을 잡아서 그 이전의 데이터를 Migration하는 python이 있어서 수작업을 진행하고
이때 wide type의 테이블을 작업전에 생성을 해야한다. 전체 데이터를 다 조회를 하기에는 시간이 너무 오래 걸리니
10분간이 데이터를 먼저 찾아서 테이블 구성하자.
그리고 실시간으로 데이터를 처리하는 python을 작성해서 데이터를 반영하게 하자.
 
 postgreSQL의 접속 정보는 아래와 같다.
 jdbc:postgresql://222.99.122.73:25432/tenant_builder
 ID와 암호는 tapp / tapp.123 이다

이런 대용량 Postgresql의 Migration 구현할때 고려해야 하는 트러블슈팅을 검색하여 시행착오를 미리 알아내보자.

 먼저 PRD.md를 작성해보자