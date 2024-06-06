fka安装目录
KAFKA_DIR="/opt/module/kafka"

# 检查Kafka目录是否存在
if [ ! -d "$KAFKA_DIR" ]; then
  echo "Kafka目录 $KAFKA_DIR 不存在"
  exit 1
fi

# 进入Kafka目录
cd "$KAFKA_DIR" || exit

# 获取所有主题列表
topics=$(bin/kafka-topics.sh --list --zookeeper hadoop-single:2181)

# 检查是否存在主题
if [ -z "$topics" ]; then
  echo "没有找到主题"
  exit 1
fi

# 清除每个主题的缓存
for topic in $topics; do
  echo "清除主题 $topic 的缓存"
  bin/kafka-topics.sh --zookeeper hadoop-single:2181 --alter --topic "$topic" --config retention.ms=1000
done

echo "所有主题缓存已清除"

