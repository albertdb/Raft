# Raft
```
/* 
* Copyright 2015 Albert Duato Botam
*  
* Licensed under the latest draft of EUPL, Version 1.2 or
-as soon they will be approved by the European Commission-
the final approved text of the EUPL v1.2 (the "Licence");
* You may not use this work except in compliance with the
Licence.
* You may obtain a copy of the Licence at:
*  
* https://github.com/albertdb/Raft/blob/master/LICENCE.pdf
*  
* Unless required by applicable law or agreed to in
writing, software distributed under the Licence is
distributed on an "AS IS" basis,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
express or implied.
* See the Licence for the specific language governing
permissions and limitations under the Licence.
*/ 
```
## Deployment
### Install dependencies (Debian)
`sudo apt-get install nodejs nodejs-legacy npm libzmq-dev git`
### Clone repository
`git clone https://github.com/albertdb/Raft`
### Install dependencies (npm)
`cd Raft`

`npm install zmq level`
## Execution
### Router
`node router.js 12345`
### Standalone Server node
`node server.js <id> <router address> <cluster members IDs> <debug server>`
#### Node 0
`node server.js 0 tcp://localhost:12345 '["0","1","2"]' false`
#### Node 1
`node server.js 1 tcp://localhost:12345 '["0","1","2"]' false`
#### Node 2
`node server.js 2 tcp://localhost:12345 '["0","1","2"]' false`
### Client+Server node (for testing purposes)
`node client.js <id> <router address> <cluster members IDs> <debug client> <debug server>`
### High-level interface (requiring client.js as a module)
#### Initialization
**Before** doing the require of client.js, you should export, in your own application, Raft parameters:
```
module.exports.clientId='<id>';
module.exports.routerAddress=<router address>;
module.exports.clusterMembers=<cluster members IDs>;
module.exports.debugServer=<debug client>;
module.exports.debugServer=<debug server>;
```
Continuing the example:
```
module.exports.clientId='0';
module.exports.routerAddress='tcp://localhost:12345';
module.exports.clusterMembers=['0','1','2'];
module.exports.debugServer=false;
module.exports.debugServer=false;
```
#### Basic API
##### client.put(key, value[, callback])
`put()` is the primary method for inserting data into the store. Both the `key` and `value` can be arbitrary data objects.

The callback argument is optional but if you don't provide one and an error occurs then expect the error to be discarded.
```
client.put('foo', 'bar', function (err) {
  if (err)
    // handle I/O or other error
});
```
##### client.get(key[, callback])
`get()` is the primary method for fetching data from the store. The `key` can be an arbitrary data object. If it doesn't exist in the store then the callback will receive an error as its first argument. A not-found err object will be of type `'NotFoundError'` so you can `err.type == 'NotFoundError'` or you can perform a truthy test on the property `err.notFound`.
```
client.get('foo', function (err, value) {
  if (err) {
    if (err.notFound) {
      // handle a 'NotFoundError' here
      return
    }
    // I/O or other error, pass it up the callback chain
    return callback(err)
  }

  // .. handle `value` here
})
```
##### client.del(key[, callback])
`del()` is the primary method for removing data from the store.
```
client.del('foo', function (err) {
  if (err)
    // handle I/O or other error
});
```
#### Cluster membership reconfiguration
##### Adding nodes to the cluster
- Before issuing a new configuration that includes new members and in order to avoid availability gaps, it is advisable to launch new cluster members so that they can receive an updated snapshot of the underlying database.
- Any new cluster member must have a unique and never before used ID. Otherwise, the preceding cluster member may ignore its messages and, in that case, it won't receive the snapshot.
- To prevent new cluster members from constituting an independent majority, they must be launched with the old configuration, that is, setting the `<cluster members IDs>` parameter to the IDs of the current cluster members, not including the new ones.
- Old reconfiguration requests not yet applied will cause the new cluster member to shutdown if any of those entries didn't include the new cluster members, so be sure that the callback of previous reconfigurations requests have been executed before issuing a new one.
- Likewise, no reconfiguration request not including new cluster members should be issued after the snapshot is sent to the first of the new cluster members, as it would also cause the new cluster members to shutdown.
- Once the new cluster members have finished receiving the snapshot from the leader, the reconfiguration can proceed as described above.
- Please nota that **all** of the new cluster members should be online and up-to-date to allow the new configuration to be applied. This ensures that fault tolerance is not weakened during transitions.

##### client.newConfig(clusterMembers[, callback])
newConfig() is the method for issuing a cluster membership reconfiguration request.
```
client.newConfig(['0','1','2','3','4'], function (err) {
  if (err)
      // request not executed
  // reconfiguration applied
});
```