# Raft
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
### Node
`node server.js <id> <router address> <number of nodes> <debug server>`
#### Node 0
`node server.js 0 tcp://localhost:12345 3 false`
#### Node 1
`node server.js 1 tcp://localhost:12345 3 false`
#### Node 2
`node server.js 2 tcp://localhost:12345 3 false`
### Client
`node client.js <id> <router address> <number of nodes> <debug client> <debug server>`
### Interface (requiring client.js as a module)
**Before** doing the require of client.js, you should export, in your own application, Raft parameters:
```
module.exports.clientId=<id>;
module.exports.routerAddress=<router address>;
module.exports.numNodes=<number of nodes>;
module.exports.debugServer=<debug client>;
module.exports.debugServer=<debug server>;
```
Continuing the example:
```
module.exports.clientId=0;
module.exports.routerAddress='tcp://localhost:12345';
module.exports.numNodes=3;
module.exports.debugServer=false;
module.exports.debugServer=false;
```
#### client.put(key, value[, callback])
put() is the primary method for inserting data into the store. Both the key and value can be arbitrary data objects.

The callback argument is optional but if you don't provide one and an error occurs then expect the error to be discarded.
#### client.get(key[, callback])
get() is the primary method for fetching data from the store. The key can be an arbitrary data object. If it doesn't exist in the store then the callback will receive an error as its first argument. A not-found err object will be of type 'NotFoundError' so you can err.type == 'NotFoundError' or you can perform a truthy test on the property err.notFound.
```
db.get('foo', function (err, value) {
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
#### client.del(key[, callback])
del() is the primary method for removing data from the store.
```
db.del('foo', function (err) {
  if (err)
    // handle I/O or other error
});
```