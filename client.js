var id=process.argv[2],
    routerAddress=process.argv[3],
    numNodes=process.argv[4],
    clientSeqNum=1;
    zmq=require('zmq'),
    clientSocket = zmq.socket('dealer');
    
module.exports.clientId=id;
module.exports.routerAddress=routerAddress;
module.exports.numNodes=numNodes;

clientSocket['identity']='c'+id;
clientSocket.connect(routerAddress);
function sendMessageToServer(destination,message){
    console.log('Client: ',message);
    clientSocket.send(['','s'+destination,'',message]);
}
clientSocket.on('message',function(){
    var args = Array.apply(null, arguments);
    showArguments(args);
    var message=JSON.parse(args[3]);
    if(message.rpc=='replyNewEntry') replyNewEntry(message.clientSeqNum,message.success,message.leaderId);
});

var server=require('./server');
server.on('result',function(err,clientSeqNum,value){
    if(value) console.log(clientSeqNum,' ',value);
});

function replyNewEntry(clientSeqNum,success,leaderId){
    if(success&&clientSeqNum%2!=0){
        var command2={type: 'GET', key: 'a'};
        var message=JSON.stringify({rpc: 'newEntry', clientId: id, clientSeqNum: clientSeqNum++, command: command2});
        sendMessageToServer(leaderId,message);
    }
}

var autoPutGetRequestInterval=setInterval(autoPutGetRequest,1000);

function autoPutGetRequest(){
    var command={type: 'PUT', key: 'a', value: (new Date()).toISOString()};
    var command2={type: 'GET', key: 'a'};
    var leaderId;
    if(leaderId=server.newEntry(id,clientSeqNum++,command)){
        var message=JSON.stringify({rpc: 'newEntry', clientId: id, clientSeqNum: clientSeqNum, command: command});
        sendMessageToServer(leaderId,message);
    }
    else if(leaderId=server.newEntry(id,clientSeqNum++,command2)){
        var message=JSON.stringify({rpc: 'newEntry', clientId: id, clientSeqNum: clientSeqNum, command: command2});
        sendMessageToServer(leaderId,message);
    }
}

//Aux functions
function showArguments(a) {
for(var k in a)
console.log('\tClient: Part', k, ':', a[k].toString());
};