# Raft
## Deployment
### Install dependencies (Debian)
`sudo apt-get install nodejs nodejs-legacy npm libzmq-dev git`
### Clone repository
`git clone https://github.com/albertdb/Raft`
### Install dependencies (npm)
`cd Raft`

`npm install zmq`
## Execution
### Router
`node router.js 12345`
### Node 0
`node server.js 0 tcp://localhost:12345 1`
### Node 1
`node server.js 1 tcp://localhost:12345 1`
### Node 2
`node server.js 2 tcp://localhost:12345 1`
### Node 3
`node server.js 3 tcp://localhost:12345 1`