var id=process.argv[2],
    routerAddress=process.argv[3],
    numNodes=process.argv[4],
    seqNum=1,
    lastKnownLeaderId=id,
    dispatchQueue=[],
    callbackQueue=[],
    replyNewEntryTimer,
    replyNewEntryTimeLimit=100,
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
    if(message.rpc=='replyNewEntry') replyNewEntry(message.initialClientSeqNum,message.success,message.leaderId,message.numEntries);
});

var server=require('./server');
server.on('result',function(err,clientSeqNum,value){
    if(value) console.log('Client: ',clientSeqNum,' ',value);
});

function replyNewEntry(initialClientSeqNum,success,leaderId,numEntries){
    if(dispatchQueue.length>0&&dispatchQueue[0].seqNum==initialClientSeqNum){
        clearTimeout(replyNewEntryTimer);
        if(success){
            for(var i=0;i<numEntries;i++) dispatchQueue.shift();
            if(dispatchQueue.length>0) setImmediate(dispatch);
        }
        else{
            lastKnownLeaderId=leaderId;
            setImmediate(dispatch);
        }
    }
}

function put(key,value,callback){
    var command={type: 'PUT', key: key, value: value};
    var request=new Request(seqNum++,command,callback);
    if(dispatchQueue.length==0) setImmediate(dispatch,1);
    dispatchQueue.push(request);
    callbackQueue.push(request);
}

function get(key,callback){
    var command={type: 'GET', key: key};
    var request=new Request(seqNum++,command,callback);
    if(dispatchQueue.length==0) setImmediate(dispatch,1);
    dispatchQueue.push(request);
    callbackQueue.push(request);
}

function del(key,callback){
    var command={type: 'DEL', key: key};
    var request=new Request(seqNum++,command,callback);
    if(dispatchQueue.length==0) setImmediate(dispatch,1);
    dispatchQueue.push(request);
    callbackQueue.push(request);
}

function dispatch(numEntries){
    numEntries=numEntries||dispatchQueue.length;
    //numEntries=Math.min(numEntries,dispatchQueue.length);
    var leaderId;
    var commands=[];
    for(var i=0;i<numEntries;i++) commands.push(dispatchQueue[i].command);
    if(numEntries>0) if(lastKnownLeaderId!=id || (leaderId=server.newEntries(id,dispatchQueue[0].seqNum,commands))){
        if(leaderId) lastKnownLeaderId=leaderId;
        var message=JSON.stringify({rpc: 'newEntries', clientId: id, initialClientSeqNum: dispatchQueue[0].seqNum, commands: commands});
        sendMessageToServer(lastKnownLeaderId,message);
        replyNewEntryTimer=setTimeout(replyNewEntryTimeout,replyNewEntryTimeLimit,numEntries);
    }
    else{
        for(var i=0;i<numEntries;i++) dispatchQueue.shift();
        if(dispatchQueue.length>0) setImmediate(dispatch);
    }
}

module.exports.put=put;
module.exports.get=get;
module.exports.del=del;

function replyNewEntryTimeout(numEntries){
    lastKnownLeaderId=id;
    dispatch(numEntries);
}

var autoPutGetRequestInterval=setInterval(autoPutGetRequest,1000);

function autoPutGetRequest(){
    put('a',(new Date()).toISOString());
    get('a');
}

//Internal classes

function Request(seqNum,command,callback){
    this.seqNum=seqNum;
    this.command=command;
    this.callback=callback;
}

//Aux functions
function showArguments(a) {
for(var k in a)
console.log('\tClient: Part', k, ':', a[k].toString());
};