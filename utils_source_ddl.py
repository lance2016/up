
def get_sr_kafka_ddl(sourceConnectInfo, args) -> str:
    source_ddl = (
        "{source_connect_sql} \n"
        "WITH ( \n"
        "   'connector' = '{db_type}', \n"
        "   'topic' = '{topic_name}',  \n"
        "   'properties.bootstrap.servers' = '{cluster_address}', \n"
        "   'properties.group.id' = '{consumer_group}',  \n"
        "   'scan.startup.mode' = '{consume_strategy}',  \n"
        "   'format' = '{format_type}'  "
        ")".format(
            source_connect_sql=args.source_connect_sql,
            db_type=sourceConnectInfo["db_type"],
            topic_name=sourceConnectInfo["topic_name"],
            cluster_address=sourceConnectInfo["cluster_address"],
            consumer_group=sourceConnectInfo["consumer_group"],
            consume_strategy=args.source_consume_strategy,
            format_type=args.source_format
        )
    )
    return source_ddl


def get_sr_sasl_kafka_ddl(sourceConnectInfo, args) -> str:
    from dhw3.configs.config import get_settings

    source_ddl = (
        "{source_connect_sql} \n"
        "WITH ( \n"
        "   'connector' = '{db_type}', \n"
        "   'topic' = '{topic_name}',  \n"
        "   'properties.bootstrap.servers' = '{cluster_address}', \n"
        "   'properties.group.id' = '{consumer_group}',  \n"
        "   'scan.startup.mode' = '{consume_strategy}',  \n"
        "   'format' = '{format_type}', \n"
        "   'properties.security.protocol' = 'SASL_SSL',  \n"
        "   'properties.ssl.truststore.location' = '{truststore_path}',  \n"
        "   'properties.ssl.truststore.password' = '{db_passwd}',  \n"
        "   'properties.ssl.keystore.location' = '{keystore_path}',  \n"
        "   'properties.ssl.keystore.password' = '{db_passwd}',  \n"
        "   'properties.sasl.mechanism' = 'SCRAM-SHA-256',  \n"
        "   'properties.sasl.jaas.config' = 'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"{user_name}\" password=\"{db_passwd}\";' \n"
        ")".format(
            source_connect_sql=args.source_connect_sql,
            db_type=sourceConnectInfo["db_type"],
            topic_name=sourceConnectInfo["topic_name"],
            cluster_address=sourceConnectInfo["cluster_address"],
            consumer_group=sourceConnectInfo["consumer_group"],
            consume_strategy=args.source_consume_strategy,
            format_type=args.source_format,
            truststore_path=get_settings().kafka_truststore_path,
            keystore_path=get_settings().kafka_keystore_path,
            user_name=sourceConnectInfo["db_user"],
            db_passwd=sourceConnectInfo["db_passwd"]
        )
    )
    return source_ddl


class UTILS_SR_DDL:
    def get_source_ddl(self, args) -> str:
        connectSourceInfo = args.connectSourceInfo
        source_type = connectSourceInfo["db_type"]
        if source_type == "kafka":
            if connectSourceInfo["auth_type"] == "none":
                return get_sr_kafka_ddl(connectSourceInfo, args)
            elif connectSourceInfo["auth_type"] == "pwd":
                return get_sr_sasl_kafka_ddl(connectSourceInfo, args)
