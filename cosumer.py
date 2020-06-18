
from kafka import KafkaConsumer
# from pyhive import hive
from impala.dbapi import connect
from mrjob.job import MRJob

consumer = KafkaConsumer('test2', bootstrap_servers=['192.168.18.99:9092'], auto_offset_reset='earliest')


conn = connect(host='192.168.18.99', port=9999, auth_mechanism='PLAIN', database='default')
cursor = conn.cursor()


def mapper_reducer():
    for message in consumer:
        # print(message)
        # # print("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
        #                                      message.offset, message.key,
        #                                      message.value))
        # print(message.value)
        time = str(message.value).split(',')[2]
        num = str(message.value).split(',')[3]
        print(time, num)
        # sql = "insert into table test(time, num) values(%s, %s)" % (time, num)
        # print(sql)
        # cursor.execute(sql)
        # cursor.close()
        # conn.close()


if __name__ == '__main__':
    mapper_reducer()
