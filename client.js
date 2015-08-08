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
var id=process.argv[2] || module.parent.exports.clientId,
    routerAddress=process.argv[3] || module.parent.exports.routerAddress,
    clusterMembers=module.parent?module.parent.exports.clusterMembers:JSON.parse(process.argv[4]),
    seqNum=Date.now(),
    lastKnownLeaderId=id,
    dispatchQueue=[],
    callbackQueue=[],
    replyNewEntriesTimer,
    replyNewEntriesTimeLimit=100,
    debug=(process.argv[5]=="true" || (module.parent && module.parent.exports.debugClient==true)),
    zmq=require('zmq'),
    clientSocket = zmq.socket('dealer');
    
module.exports.clientId=id;
module.exports.routerAddress=routerAddress;
module.exports.clusterMembers=clusterMembers;
module.exports.debugServer=(process.argv[6]=="true" || (module.parent && module.parent.exports.debugServer==true));

clientSocket['identity']='c'+id;
clientSocket.connect(routerAddress);
function sendMessageToServer(destination,message){
    if(debug) console.log('Client: ',message);
    clientSocket.send(['','s'+destination,'',message]);
}
clientSocket.on('message',function(){
    var args = Array.apply(null, arguments);
    if(debug) showArguments(args);
    var message=JSON.parse(args[3]);
    if(message.rpc=='replyNewEntries') replyNewEntries(message.initialClientSeqNum,message.success,message.leaderId,message.numEntries);
});

var server=require('./server');
server.on('result',function(err,clientSeqNum,value){
    //if(value) console.log('Client: ',clientSeqNum,' ',value);
    while(callbackQueue.length>0&&clientSeqNum>callbackQueue[0].seqNum){
        if(callbackQueue[0].callback) callbackQueue.shift().callback(new Error('Not executed'));
        else callbackQueue.shift();
    }
    if(callbackQueue.length>0&&clientSeqNum==callbackQueue[0].seqNum){
        if(callbackQueue[0].callback) callbackQueue.shift().callback(err,value);
        else callbackQueue.shift();
    }
});

function replyNewEntries(initialClientSeqNum,success,leaderId,numEntries){
    if(dispatchQueue.length>0&&dispatchQueue[0].seqNum==initialClientSeqNum){
        clearTimeout(replyNewEntriesTimer);
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

function newConfig(clusterMembers,callback){
    var command={type: 'CFG', clusterMembers: clusterMembers};
    var request=new Request(seqNum++,command,callback);
    if(dispatchQueue.length==0) setImmediate(dispatch,1);
    dispatchQueue.push(request);
    callbackQueue.push(request);
}

function dispatch(numEntries){
    numEntries=numEntries||dispatchQueue.length;
    numEntries=Math.min(numEntries,dispatchQueue.length); //PARCHE. No tiene efectos secundarios, pero no soluciona el origen del problema. 
    var leaderId;
    var commands=[];
    for(var i=0;i<numEntries;i++) commands.push(dispatchQueue[i].command);
    if(numEntries>0) if(lastKnownLeaderId!=id || (leaderId=server.newEntries(id,dispatchQueue[0].seqNum,commands))){
        if(leaderId) lastKnownLeaderId=leaderId;
        var message=JSON.stringify({rpc: 'newEntries', clientId: id, initialClientSeqNum: dispatchQueue[0].seqNum, commands: commands});
        sendMessageToServer(lastKnownLeaderId,message);
        replyNewEntriesTimer=setTimeout(replyNewEntriesTimeout,replyNewEntriesTimeLimit,numEntries);
    }
    else{
        for(var i=0;i<numEntries;i++) dispatchQueue.shift();
        if(dispatchQueue.length>0) setImmediate(dispatch);
    }
}

module.exports.put=put;
module.exports.get=get;
module.exports.del=del;
module.exports.newConfig=newConfig;

function replyNewEntriesTimeout(numEntries){
    lastKnownLeaderId=id;
    dispatch(numEntries);
}

var autoPutGetRequestInterval;

if(!module.parent){
    var stdin = process.stdin;
    stdin.setRawMode(true);
    stdin.resume();
    stdin.setEncoding('utf8');
    stdin.on('data', function(key) {
        if (key==='\u0003') {
            process.exit();
        }
        if(key=='0') clearInterval(autoPutGetRequestInterval);
        if(key=='1') autoPutGetRequestInterval=setInterval(autoPutGetRequest,50,1,true);
        if(key=='2') autoPutGetRequestInterval=setInterval(autoPutGetRequest,50,5,false);
        if(key=='3') autoPutGetRequestInterval=setInterval(autoPutGetRequest,50,50,false);
        if(key=='4') for(var i=0;i<3333;i++){
            put('a',(new Date()).toISOString(),function(err){
                if(err) console.log('Client error: ',err);
            });
            get('a',function(err,value){ 
                if(err) console.log('Client error: ',err);
            });
            del('a',function(err){ 
                if(err) console.log('Client error: ',err);
            });
            if(i==3332) get('a',function(err,value){
                if(err) console.log('Client error: ',err);
                console.log('Client: Request bombing finished.');
            });
        }
    });
}

function autoPutGetRequest(num,verbose){
  for(var i=0;i<num;i++){
    put('a',(new Date()).toISOString(),function(err){
        if(err) console.log('Client error: ',err);
    });
    get('a',function(err,value){ 
        if(err) console.log('Client error: ',err);
        if(verbose) if(value) console.log('Client value: a=',value);
    });
  }
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