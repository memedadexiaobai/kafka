#!/bin/bash

# 以kraft模式进行运行
# 如果首次运行，即 metadata.log.dir 不包含已有的元数据，需要先初始化数据
# 格式化存储目录
# bin/kafka-storage.sh format -t <cluster-id> -c config/kraft/server.properties
if [ ! -d ../logs/kraft-combined-logs  ]; then
    echo 'metadata.log.dir non exit, start init'
    /bin/bash /Users/a58/github_workspace/kafka/bin/kafka-storage.sh format -t _b1d-2a1l-3x4y -c ../config/kraft/server.properties
    echo 'metadata.log.dir non exit, end init'
fi

# 使用配置文件启动
/bin/bash /Users/a58/github_workspace/kafka/bin/kafka-server-start.sh ../config/kraft/server.properties

# 后台运行
# /bin/bash /Users/a58/github_workspace/kafka/bin/kafka-server-start.sh -daemon ../config/kraft/server.properties
