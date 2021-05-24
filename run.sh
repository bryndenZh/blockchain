# ./run_node.sh
# sleep 15 
# ./run_client.sh
# 
# sleep 50
# pkill -9 python

while [ -n "$1" ]  
do  
  case "$1" in   
    -node)  
        rm *.blockchain
        rm *node*.log
        node_id=0
        while [ $node_id -lt $2 ]; do
	        python ./node.py -i $node_id &
          echo "运行 node $node_id"
          node_id=$(($node_id+1)) 
        done
        shift
        ;;  
    -client)  
        rm *client*.log
        echo "运行 client, 发送 $2 条消息" 
        python3 ./client_app.py -id 0 -nm $2 & 
        shift  
        ;;  
    stop)  
        ps -aux | grep "python3 ./node" | awk '{print $2}' | xargs kill -9 
        echo "停止 node"
        ps -aux | grep "python3 ./client" | awk '{print $2}' | xargs kill -9 
        echo "停止 client"  
        ;;  
    -save)
        mkdir $2
        echo "mkdir $2"
        mv *.log $2
        mv *.blockchain $2
  esac  
  shift  
done

