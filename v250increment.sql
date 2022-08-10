-- V2.5.0
CREATE SEQUENCE cw_data_platform_dev.cw_stream_connector_id_seq
        INCREMENT BY 1
        MINVALUE 1
        MAXVALUE 2147483647
        START 1
        NO CYCLE;


CREATE TABLE cw_data_platform_dev.cw_stream_connector (
	id int4 NOT NULL DEFAULT nextval('cw_data_platform_dev.cw_stream_connector_id_seq'::regclass),
	connector_name varchar(64) NULL,
	instance_id int4 NULL,
	flink_create_sql varchar(512) NULL,
	param_config text NULL,
	consume_strategy varchar(20) NULL,
	out_format varchar(6) NULL,
	status int4 NULL DEFAULT 1,
	is_delete int4 NULL DEFAULT 0,
	create_user varchar(32) NULL,
	create_time timestamp NULL,
	update_user varchar(32) NULL,
	update_time timestamp NULL,
	CONSTRAINT cw_stream_connector_pk PRIMARY KEY (id)
);


ALTER TABLE "cw_data_platform_dev"."cw_server_manager" ALTER COLUMN "type" type varchar(20) USING  "type"::varchar;



-- 高斯专用
insert into cw_data_platform_dev.cw_scheduler_ip_port
    values('default',8082,'gsfs','enable',now(),now());



# kafka SASL_SSL证书配置
# 配置服务端提供的 truststore (CA 证书) 的文件
kafka_truststore_path = "/dhw_home/conf/user_config/phy_ca.rt" 
# 配置 keystore (私钥) 的文件
kafka_keystore_path = "/dhw_home/conf/user_config/phy_ca.rt"


# flink服务
flink_server_host=10.128.174.197
flink_server_port=28081
flink_sink_file_path=/home