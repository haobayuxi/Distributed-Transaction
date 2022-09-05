#!/yuxi/bash
rm -rf *throughput
ps -ef|grep yuxi_client|grep -v grep|awk '{print $2}'|xargs kill -9
