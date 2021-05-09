### requirements: 
- aiohttp
- pyyaml

### run 4 nodes 

./run.sh -node 4

### run client, send 10 messages

./run.sh -client 10

### stop all

./run.sh stop

### save logs

./run.sh -save dir_name

### to use initial pbft instead of simplified

modify pbtf_handler.preprepare() to
```
await self._post(self._nodes, PBFTHandler.PREPARE, preprepare_msg)
# await self._post(self._nodes, PBFTHandler.FEEDBACK, preprepare_msg)
```
