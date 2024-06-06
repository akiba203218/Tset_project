#!/bin/sh

case $1 in
"start"){

        for i in hadoop-single
        do
                echo "******** $i --> kafka-server-start.sh **********"
                ssh $i 'source /etc/profile;sh /opt/module/kafka/bin/kafka-server-start.sh -daemon /opt/module/kafka/config/server.properties'
        done

};;

"stop"){

        for i in hadoop-single
        do
                echo "******** $i --> kafka-server-stop.sh **********"
                ssh $i 'source /etc/profile; /opt/module/kafka/bin/kafka-server-stop.sh /opt/module/kafka/config/server.properties;exit'
        done

};;
esac

