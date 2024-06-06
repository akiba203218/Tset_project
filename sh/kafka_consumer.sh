#!/bin/bash

# Kafka 安装目录
KAFKA_HOME=/opt/module/kafka

# Kafka 服务器地址和端口
KAFKA_SERVER=hadoop-single:9092

# 提示用户输入主题
echo "请输入要消费的主题："
read TOPIC

# 使用 kafka-console-consumer.sh 从指定主题消费消息
$KAFKA_HOME/bin/kafka-console-consumer.sh --bootstrap-server $KAFKA_SERVER --topic $TOPIC --from-beginning

