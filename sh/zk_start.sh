#!/bin/sh

case $1 in
"start"){

	for i in hadoop-single
	do
		echo "********$i --> zkServer.sh start **********"
		ssh $i 'source /etc/profile; /opt/module/zookeeper/bin/zkServer.sh start;exit'
	done
};;

"stop"){

    for i in hadoop-single
    do
        echo "********$i --> zkServer.sh stop **********"
        ssh $i 'source /etc/profile;  /opt/module/zookeeper/bin/zkServer.sh stop;exit'
    done
};;

"status"){

    for i in hadoop-single
    do
        echo "********$i --> zkServer.sh status **********"
        ssh $i 'source /etc/profile; /opt/module/zookeeper/bin/zkServer.sh status;exit'
    done
};;
"restart"){

    for i in hadoop-single
    do
        echo "********$i --> zkServer.sh restart **********"
        ssh $i 'source /etc/profile; /opt/module/zookeeper/bin/zkServer.sh restart;exit'
    done
};;
esac

