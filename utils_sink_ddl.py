
# sink: csv
def get_sink_csv_ddl(connectSinkInfo, file_path) -> str:
    sink_ddl = (
        "{sink_flink_sql} \n"
        "WITH ( \n"
        "   'connector.type' = 'filesystem', \n"
        "   'connector.path' = '{file_path}', \n"
        "   'format.type' = '{format_type}', \n"
        "   'format.write-mode' = 'OVERWRITE' \n"
        ")".format(
            sink_flink_sql=connectSinkInfo["flink_create_sql"],
            file_path="{}/{}".format(file_path, connectSinkInfo["file_name"]),
            format_type=connectSinkInfo["out_format"]
        )
    )
    return sink_ddl


# sink: greenplum
def get_sink_gp_ddl(connectSinkInfo, to_schema, to_table_name) -> str:
    to_table_name = "{}.{}".format(to_schema, to_table_name)
    sink_ddl = (
        "{sink_flink_sql} \n"
        "WITH ( \n"
        "   'connector' = 'jdbc', \n"
        "   'url' = 'jdbc:postgresql://{db_host}:{db_port}/{db_name}?characterEncoding=UTF-8', \n"
        "   'table-name' = '{table_name}', \n"
        "   'username' = '{username}', \n"
        "   'password' = '{password}' \n"
        ")".format(
            sink_flink_sql=connectSinkInfo["flink_create_sql"],
            db_host=connectSinkInfo["db_host"],
            db_port=connectSinkInfo["db_port"],
            db_name=connectSinkInfo["db_name"],
            username=connectSinkInfo["db_user"],
            password=connectSinkInfo["db_passwd"],
            table_name=to_table_name
        )
    )
    return sink_ddl


# sink: kafka
def get_sink_kafka_ddl(connectSinkInfo, args) -> str:
    sink_ddl = (
        "{sink_connect_sql} \n"
        "WITH ( \n"
        "   'connector' = '{db_type}', \n"
        "   'topic' = '{topic_name}',  \n"
        "   'properties.bootstrap.servers' = '{cluster_address}', \n"
        "   'properties.group.id' = '{consumer_group}',  \n"
        "   'format' = '{format_type}'  "
        ")".format(
            sink_connect_sql=args.sink_connect_sql,
            db_type=connectSinkInfo["db_type"],
            topic_name=connectSinkInfo["topic_name"],
            cluster_address=connectSinkInfo["cluster_address"],
            consumer_group=connectSinkInfo["consumer_group"],
            format_type=args.sink_format
        )
    )
    return sink_ddl


# sink: kafka
def get_sink_sasl_kafka_ddl(connectSinkInfo, args) -> str:
    from dhw3.configs.config import get_settings

    sink_ddl = (
        "{sink_connect_sql} \n"
        "WITH ( \n"
        "   'connector' = '{db_type}', \n"
        "   'topic' = '{topic_name}',  \n"
        "   'properties.bootstrap.servers' = '{cluster_address}', \n"
        "   'properties.group.id' = '{consumer_group}',  \n"
        "   'format' = '{format_type}',  \n"
        "   'properties.security.protocol' = 'SASL_PLAINTEXT',  \n"
        "   'properties.ssl.truststore.location' = '{truststore_path}',  \n"
        "   'properties.ssl.truststore.password' = '{db_passwd}',  \n"
        "   'properties.sasl.mechanism' = 'SCRAM-SHA-256',  \n"
        "   'properties.sasl.jaas.config' = 'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"{user_name}\" password=\"{db_passwd}\";' \n"
        ")".format(
            sink_connect_sql=args.sink_connect_sql,
            db_type=connectSinkInfo["db_type"],
            topic_name=connectSinkInfo["topic_name"],
            cluster_address=connectSinkInfo["cluster_address"],
            consumer_group=connectSinkInfo["consumer_group"],
            format_type=args.sink_format,
            truststore_path=get_settings().kafka_truststore_path,
            keystore_path=get_settings().kafka_keystore_path,
            user_name=connectSinkInfo["db_user"],
            db_passwd=connectSinkInfo["db_passwd"]
        )
    )
    print(sink_ddl)
    return sink_ddl


class UTILS_SINK_DDL:
    def get_sink_ddl(self, args) -> str:
        connectSinkInfo = args.connectSinkInfo
        sink_type = connectSinkInfo["db_type"]
        if sink_type == "csv":
            return get_sink_csv_ddl(connectSinkInfo, args.file_path)
        if sink_type == "greenplum":
            return get_sink_gp_ddl(connectSinkInfo, to_schema=args.to_schema, to_table_name=args.to_table_name)
        if sink_type == "kafka":
            if connectSinkInfo["auth_type"] == "none":
                return get_sink_kafka_ddl(connectSinkInfo, args)
            elif connectSinkInfo["auth_type"] == "pwd":
                return get_sink_sasl_kafka_ddl(connectSinkInfo, args)