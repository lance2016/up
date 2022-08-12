import time
import json
import datetime
from kafka import KafkaProducer
from kafka import KafkaConsumer
from multiprocessing import Process
import ssl


def produce_message(bootstrap_servers, topic, data):
    print("start send message")
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda m: json.dumps(m).encode("utf-8"),
    )
    # 参数bootstrap_servers：指定kafka连接地址
    # 参数value_serializer：指定序列化的方式，我们定义json来序列化数据，当字典传入kafka时自动转换成bytes
    # 用户密码登入参数
    # security_protocol="SASL_PLAINTEXT"
    # sasl_mechanism="PLAIN"
    # sasl_plain_username="maple"
    # sasl_plain_password="maple"
    producer.send(topic, data)
    producer.close()


def comsume_message(process, group_id, *topics):
    print("start receive message")
    consumer = KafkaConsumer(
        *topics,
        bootstrap_servers=["127.0.0.1:9092"],
        group_id=group_id,
        auto_offset_reset="earliest"
    )
    # 参数bootstrap_servers：指定kafka连接地址
    # 参数group_id：如果2个程序的topic和group_id相同，那么他们读取的数据不会重复，2个程序的topic相同，group_id不同，那么他们各自消费相同的数据，互不影响
    # 参数auto_offset_reset：默认为latest表示offset设置为当前程序启动时的数据位置，earliest表示offset设置为0，在你的group_id第一次运行时，还没有offset的时候，给你设定初始offset。一旦group_id有了offset，那么此参数就不起作用了

    for msg in consumer:
        print(msg)
        recv = "%s:%d:%d: key=%s value=%s process=%s" % (
            msg.topic,
            msg.partition,
            msg.offset,
            msg.key,
            msg.value,
            process,
        )
        print(recv)
        str = msg.value.decode("UTF-8")
        res_list = json.loads(str)
        for res_obj in res_list:
            print(res_obj)
            # print(type(res_obj))
            # # for key, val in res_obj.items():
            # #     print(group_id, key, val)
        time.sleep(1)
        consumer.close()


if __name__ == "__main__":
    bootstrap_servers = ["127.0.0.1:9092"]
    topic = "test"
    send_data = []
    for i in range(10):
        send_data.append(
            {"num": i, "ts": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
        )

    p = Process(target=produce_message, args=(bootstrap_servers, topic, send_data))
    p.start()

    # p1 = Process(target=comsume_message, args=('进程1', 'group3', topic))
    # p2 = Process(target=comsume_message, args=('进程2', 'group1', topic))
    # p1.start()
    # p2.start()

    p.join()
    # p1.join()
    # p2.join()
    print("main process end")


def send_kafka_info(self, topic_name, content):
    self.context.verify_mode = ssl.CERT_REQUIRED
    self.context.load_verify_locations(self.phy_path)
    producer = KafkaProducer(
        bootstrap_servers=self.bootstrap_servers,
        sasl_mechanism="PLAIN",
        ssl_context=self.context,
        security_protocol="SASL_SSL",
        sasl_plain_username=self.sasl_plain_username,
        sasl_plain_password=self.sasl_plain_password,
    )
    content = {"1": "2"}
    producer.send(topic_name, content)
