#!/bin/bash

# Kafka 安装目录
KAFKA_HOME=/opt/module/kafka

# Kafka 服务器地址和端口
KAFKA_SERVER=hadoop-single:9092

# 提示用户输入主题
echo "请输入要发送消息的主题："
read TOPIC

# 发送消息
echo "请输入消息，按 Ctrl+D 结束："
while read MESSAGE; do
  # 使用 kafka-console-producer.sh 发送消息到指定主题
  echo "$MESSAGE" | $KAFKA_HOME/bin/kafka-console-producer.sh --broker-list $KAFKA_SERVER --topic $TOPIC
done

