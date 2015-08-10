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
    clusterMembers=module.parent?module.parent.exports.clusterMembers:JSON.parse(process.argv[4]),
    currentTerm=0,
    state='f',
    votedFor=null,
    lastKnownLeaderId=null,
    log=newLog(),
    commitIndex=0,
    maybeNeedToCommit=false,
    lastApplied=0,
    maxAppliedEntriesInLog=10000,
    nextIndex=Object.create(null),
    matchIndex=Object.create(null),
    recoveryMode=false,
    recoveryPrevLogIndex=0,
    grantedVotes=0,
    electionTime=randomInt(150, 300),
    heartbeatTime=25,
    commitTime=50,
    debug=(process.argv[5]=="true" || (module.parent && module.parent.exports.debugServer==true)),
    zmq=require('zmq'),
    socket = zmq.socket('dealer'),
    clientSocket = zmq.socket('dealer'),
    levelup = require('level'),
    db = levelup('./'+id+'.db'),
    snappy = require('snappy'),
    EventEmitter = require('events').EventEmitter;
    
socket['identity']=id;
socket.connect(process.argv[3] || module.parent.exports.routerAddress);
function sendMessage(destination,message){
    if(debug) console.log(message);
    message=snappy.compressSync(message);
    socket.send(['',destination,'',message]);
}

clientSocket['identity']='s'+id;
clientSocket.connect(process.argv[3] || module.parent.exports.routerAddress);
function sendMessageToClient(destination,message){
    if(debug) console.log(message);
    message=snappy.compressSync(message);
    clientSocket.send(['','c'+destination,'',message]);
}

var electionTimer=setTimeout(electionTimeout,electionTime);
var heartbeatTimer;
var commitInterval;
if(commitTime) commitInterval=setInterval(commitEntries,commitTime);

for(var i in clusterMembers){
    if(clusterMembers[i]!=id) nextIndex[clusterMembers[i]]=1;
    if(clusterMembers[i]!=id) matchIndex[clusterMembers[i]]=0;
}

socket.on('message',function(){
    var args = Array.apply(null, arguments);
    args[3]=snappy.uncompressSync(args[3]);
    if(debug) showArguments(args);
    var message=JSON.parse(args[3]);
    if(message.rpc=='appendEntries') appendEntries(message.term,message.leaderId,message.prevLogIndex,message.prevLogTerm,message.entries,message.leaderCommit);
    else if(message.rpc=='replyAppendEntries') replyAppendEntries(message.term,message.followerId,message.entriesToAppend,message.success);
    else if(message.rpc=='requestVote') requestVote(message.term,message.candidateId,message.lastLogIndex,message.lastLogTerm);
    else if(message.rpc=='replyVote') replyVote(message.term,message.voteGranted);
    else if(message.rpc=='installSnapshot') installSnapshot(message.term,message.leaderId,message.lastIncludedIndex,message.lastIncludedTerm,message.offset,message.data,message.done);
});

clientSocket.on('message',function(){
    var args = Array.apply(null, arguments);
    args[3]=snappy.uncompressSync(args[3]);
    if(debug) showArguments(args);
    var message=JSON.parse(args[3]);
    if(message.rpc=='newEntries') newEntries(message.clientId,message.initialClientSeqNum,message.commands);
});

//RPCs

function appendEntries(term,leaderId,prevLogIndex,prevLogTerm,entries,leaderCommit){
    var message;
    if(term>=currentTerm){
        clearTimeout(electionTimer);
        if(lastKnownLeaderId!=leaderId) console.log('Election result: ',leaderId,' is the new leader.');
        lastKnownLeaderId=leaderId;
        if(term>currentTerm){
            currentTerm=term;
        }
        if(prevLogIndex<log.length && (log.length==log.firstIndex || log[prevLogIndex].term==prevLogTerm)){
            if(recoveryMode) console.log('Last matching entry found. Exiting recovery mode.');
            recoveryMode=false;
            for(var entry in entries){
                if(entries[entry].command.type=='CFG'){
                    clusterMembers=clusterMembers.concat(entries[entry].command.clusterMembers);
                    clusterMembers.sort();
                    for (var i in clusterMembers) if(clusterMembers[i]==clusterMembers[i-1]) clusterMembers.splice(i,1);
                } 
                log.push(entries[entry]);
            } 
            message=JSON.stringify({rpc: 'replyAppendEntries', term: currentTerm, followerId: id, entriesToAppend: entries.length, success: true});
            if(leaderCommit>commitIndex){
                commitIndex=Math.min(leaderCommit,log.length-1);
                setImmediate(processEntries,commitIndex+1);
            }
            electionTimer=setTimeout(electionTimeout,electionTime);
        }
        else if(!recoveryMode || (recoveryMode && prevLogIndex<recoveryPrevLogIndex)){
            while(prevLogIndex<log.length) log.pop();
            if(!recoveryMode) console.log('Log is outdated. Entering recovery mode.');
            recoveryMode=true;
            recoveryPrevLogIndex=prevLogIndex;
            message=JSON.stringify({rpc: 'replyAppendEntries', term: currentTerm, followerId: id, entriesToAppend: prevLogIndex, success: false});
        }
    }
    else message=JSON.stringify({rpc: 'replyAppendEntries', term: currentTerm, followerId: id, entriesToAppend: prevLogIndex, success: false});
    if(message) sendMessage(leaderId,message);
}

function replyAppendEntries(term,followerId,entriesToAppend,success){
    if(state=='l' && term>=currentTerm){
        if(term>currentTerm){
            currentTerm=term;
            state='f';
            grantedVotes=0;
            votedFor=null;
            clearTimeout(heartbeatTimer);
        }
        else if(success){
            matchIndex[followerId]+=entriesToAppend;
            maybeNeedToCommit=true;
            if(!commitTime) setImmediate(commitEntries);
            if(nextIndex[followerId]<log.length){
                var message=JSON.stringify({rpc: 'appendEntries', term: currentTerm, leaderId: id, prevLogIndex: nextIndex[followerId]-1, prevLogTerm: log[nextIndex[followerId]-1].term,entries: log.slice(nextIndex[followerId],Math.min(log.length,nextIndex[followerId]+100)), leaderCommit: commitIndex});
                nextIndex[followerId]+=Math.min(log.length,nextIndex[followerId]+100)-nextIndex[followerId];
                if(nextIndex[followerId]==log.length){
                    if(recoveryMode) console.log('Follower ',followerId,' log should be now in sync. Exiting recovery mode.');
                    recoveryMode=false;
                }
                sendMessage(followerId,message);
            }
        }
        else{
            if(!recoveryMode) console.log('Follower ',followerId,' log is outdated. Entering recovery mode.');
            recoveryMode=true;
            nextIndex[followerId]=entriesToAppend;
            matchIndex[followerId]=nextIndex[followerId]-1;
            if(log[nextIndex[followerId]-1]!=undefined){
                var message=JSON.stringify({rpc: 'appendEntries', term: currentTerm, leaderId: id, prevLogIndex: nextIndex[followerId]-1, prevLogTerm: log[nextIndex[followerId]-1].term,entries: [log[nextIndex[followerId]]], leaderCommit: commitIndex});
                nextIndex[followerId]+=1;
                sendMessage(followerId,message);
            }
            else{
                console.log('Follower ',followerId,' last log entry is behind oldest entry still stored in the log. Sending DB snapshot.');
                var dataArray=[];
                var dataOffset=0;
                var lastIncludedIndex=lastApplied;
                var lastIncludedTerm=log[lastApplied].term;
                var actualTerm=currentTerm;
                db.createReadStream()
                    .on('data', function (data) {
                        dataArray.push({type: 'put', key: data.key, value: data.value});
                        if(dataArray.length>99){
                            var message=JSON.stringify({rpc: 'installSnapshot', term: actualTerm, leaderId: id, lastIncludedIndex: lastIncludedIndex, lastIncludedTerm: lastIncludedTerm, offset: dataOffset, data: dataArray, done: false});
                            dataOffset+=dataArray.length;
                            dataArray=[];
                            sendMessage(followerId,message);
                        }
                })
                    .on('error', function (err) {
                        console.log('Oh my!', err);
                })
                    .on('close', function () {
                })
                    .on('end', function () {
                        var message=JSON.stringify({rpc: 'installSnapshot', term: actualTerm, leaderId: id, lastIncludedIndex: lastIncludedIndex, lastIncludedTerm: lastIncludedTerm, offset: dataOffset, data: dataArray, done: true});
                        delete dataOffset;
                        delete dataArray;
                        if(log.length==lastIncludedIndex+1 && state=='l') newNullEntry();
                        nextIndex[followerId]=lastIncludedIndex+1;
                        matchIndex[followerId]=nextIndex[followerId]-1;
                        console.log('Finished sending DB snapshot to follower ',followerId,'.');
                        sendMessage(followerId,message);
                });
            }
        }
    }
}

function requestVote(term,candidateId,lastLogIndex,lastLogTerm){
    if(clusterMembers.indexOf(candidateId)>=0){
        var message;
        if(term>=currentTerm){
            if(term>currentTerm){
                console.log('Election in progress.');
                currentTerm=term;
                if(state=='l') console.log('Demoting to follower state.');
                state='f';
                votedFor=null;
                recoveryMode=false;
                clearTimeout(heartbeatTimer);
            }
            if((votedFor==null || votedFor==candidateId) && (log.length==log.firstIndex || lastLogTerm>log[log.length-1].term || lastLogTerm==log[log.length-1].term && lastLogIndex>=log.length-1)){
                votedFor=candidateId;
                console.log('Vote granted to candidate ',candidateId,'.');
                message=JSON.stringify({rpc: 'replyVote', term: currentTerm, voteGranted: true});
            }
            else message=JSON.stringify({rpc: 'replyVote', term: currentTerm, voteGranted: false});
        }
        else message=JSON.stringify({rpc: 'replyVote', term: currentTerm, voteGranted: false});
        sendMessage(candidateId,message);
    }
    else if(!nextIndex[candidateId]) replyAppendEntries(currentTerm,candidateId,0,false)
}

function replyVote(term,voteGranted){
    if(term>currentTerm){
        currentTerm=term;
        state='f';
        grantedVotes=0;
        votedFor=null;
    }
    else if(voteGranted && term==currentTerm && state=='c'){
        grantedVotes++;
        console.log('Received vote grant. ',grantedVotes,' of ',Math.ceil(clusterMembers.length/2+0.01),' required to win.');
        if(grantedVotes>clusterMembers.length/2){
            console.log("Election win. Say 'Hi' to the new almighty leader.");
            state='l';
            lastKnownLeaderId=id;
            grantedVotes=0;
            for (var i in clusterMembers) {
                if(clusterMembers[i]!=id) (function(serverId){
                    nextIndex[serverId]=log.length;
                    matchIndex[serverId]=log.length-1;
                })(clusterMembers[i]);
            }
            newNullEntry();
            heartbeatTimer=setTimeout(heartbeatTimeout,0);
            //NO! votedFor=null;
        }
    }
}

function installSnapshot(term,leaderId,lastIncludedIndex,lastIncludedTerm,offset,data,done){
    if(term>currentTerm){
        currentTerm=term;
    }
    if(term==currentTerm) if(offset==0){
        if(lastKnownLeaderId==null) clearTimeout(electionTimer);
        console.log('Snapshot install request received. Closing current DB.');
        installSnapshot.pendingBatches=1;
        db.close(function(err){
            if(err) throw err;
            console.log('Destroying current DB.');
            levelup.destroy('./'+id+'.db',function(err){
                if(err) throw err;
                console.log('Creating new DB.');
                db=levelup('./'+id+'.db');
                db.batch(data, function (err) {
                    console.log('Installing data...');
                      if(err) throw err;
                      installSnapshot.pendingBatches--;
                      if(done){
                          var message=JSON.stringify({rpc: 'replyAppendEntries', term: currentTerm, followerId: id, entriesToAppend: 0, success: true});
                          log=newLog();
                          log.shift();
                          log.firstIndex=lastIncludedIndex+1;
                          log.length=log.firstIndex;
                          commitIndex=lastIncludedIndex;
                          lastApplied=lastIncludedIndex;
                          recoveryMode=false;
                          console.log('Finished installing DB snapshot. Exiting recovery mode.');
                          sendMessage(leaderId,message);
                      }
                })
            });
        });
    }
    else if(offset>0){
        if(db.isOpen()){
            installSnapshot.pendingBatches++;
            db.batch(data, function (err) {
                if(err) throw err;
                installSnapshot.pendingBatches--;
                if(done){
                    if(installSnapshot.pendingBatches==0){
                        var message=JSON.stringify({rpc: 'replyAppendEntries', term: currentTerm, followerId: id, entriesToAppend: 0, success: true});
                        log=newLog();
                        log.shift();
                        log.firstIndex=lastIncludedIndex+1;
                        log.length=log.firstIndex;
                        commitIndex=lastIncludedIndex;
                        lastApplied=lastIncludedIndex;
                        recoveryMode=false;
                        console.log('Finished installing DB snapshot. Exiting recovery mode.');
                        sendMessage(leaderId,message);
                    }
                    else setImmediate(installSnapshot,term,leaderId,lastIncludedIndex,lastIncludedTerm,offset,[],done);
                }
            });
        }
        else setImmediate(installSnapshot,term,leaderId,lastIncludedIndex,lastIncludedTerm,offset,data,done);
    }
}

function newEntries(clientId,initialClientSeqNum,commands){
    if(state=='l'){
        var entries=[]
        for(var i in commands) entries.push(new LogEntry(clientId,initialClientSeqNum+parseInt(i),commands[i],currentTerm));
        var found=false;
        for(var i=log.length-1;i>=log.firstIndex&&!found;i--) 
            if(log[i].clientId==clientId)
                if(log[i].clientSeqNum>=initialClientSeqNum) found=true;
                else break;
        if(!found){
            for (var i in clusterMembers) {
                if(clusterMembers[i]!=id) (function(serverId){
                    if(nextIndex[serverId]==log.length){
                        var message=JSON.stringify({rpc: 'appendEntries', term: currentTerm, leaderId: id, prevLogIndex: log.length-1, prevLogTerm: log[log.length-1].term,entries: entries, leaderCommit: commitIndex});
                        nextIndex[serverId]+=entries.length;
                        sendMessage(serverId,message);
                    }
                })(clusterMembers[i]);
            }
            for(var entry in entries){
                if(entries[entry].command.type=='CFG'){
                    clusterMembers=clusterMembers.concat(entries[entry].command.clusterMembers);
                    clusterMembers.sort();
                    for (var i in clusterMembers) if(clusterMembers[i]==clusterMembers[i-1]) clusterMembers.splice(i,1);
                    for (var i in clusterMembers) if(!nextIndex[clusterMembers[i]]){
                        nextIndex[clusterMembers[i]]=log.length;
                        matchIndex[clusterMembers[i]]=log.length-1;
                    }
                } 
                log.push(entries[entry]);
            } 
        }
        clearTimeout(heartbeatTimer);
        heartbeatTimer=setTimeout(heartbeatTimeout,heartbeatTime);
        clearTimeout(electionTimer);
        electionTimer=setTimeout(electionTimeout,electionTime);
        if(clientId!=id){
            var message=JSON.stringify({rpc: 'replyNewEntries', initialClientSeqNum: initialClientSeqNum, success: true, leaderId: id, numEntries: entries.length});
            sendMessageToClient(clientId,message);
        }
    } else if(clientId==id) return lastKnownLeaderId;
    else{
        var message=JSON.stringify({rpc: 'replyNewEntries', initialClientSeqNum: initialClientSeqNum, success: false, leaderId: lastKnownLeaderId});
        sendMessageToClient(clientId,message);
    }
}

function newNullEntry(){
    var entry=new LogEntry(id,0,{type: 'NUL'},currentTerm);
    for (var i in clusterMembers) {
        if(clusterMembers[i]!=id) (function(serverId){
            if(nextIndex[serverId]==log.length){
                var message=JSON.stringify({rpc: 'appendEntries', term: currentTerm, leaderId: id, prevLogIndex: log.length-1, prevLogTerm: log[log.length-1].term,entries: [entry], leaderCommit: commitIndex});
                nextIndex[serverId]+=1;
                sendMessage(serverId,message);
            }
        })(clusterMembers[i]);
    }
    log.push(entry);
}

//Timeout functions

function electionTimeout(){
    if(lastKnownLeaderId!=null) console.log('No heartbeat received from leader within time limit. Starting election.');
    else console.log('No known leader for me. Starting election.');
	currentTerm++;
	state='c';
	votedFor=id;
	grantedVotes=1
	for (var i in clusterMembers) {
        if(clusterMembers[i]!=id) (function(serverId){
            var message=JSON.stringify({rpc: 'requestVote', term: currentTerm, candidateId: id, lastLogIndex: log.length-1, lastLogTerm: log[log.length-1].term});
            sendMessage(serverId,message);
        })(clusterMembers[i]);
	}
	clearTimeout(electionTimer);
	electionTimer=setTimeout(electionTimeout,electionTime);
}

function heartbeatTimeout(){
	for (var i in clusterMembers) {
        if(clusterMembers[i]!=id) (function(serverId){
            if(nextIndex[serverId]==log.length){
                var message=JSON.stringify({rpc: 'appendEntries', term: currentTerm, leaderId: id, prevLogIndex: log.length-1, prevLogTerm: log[log.length-1].term,entries: [], leaderCommit: commitIndex});
                sendMessage(serverId,message);
            }
        })(clusterMembers[i]);
	}
	clearTimeout(heartbeatTimer);
	heartbeatTimer=setTimeout(heartbeatTimeout,heartbeatTime);
	clearTimeout(electionTimer);
    electionTimer=setTimeout(electionTimeout,electionTime);
}

function commitEntries(){
    if(maybeNeedToCommit){
        var newCommitIndex=commitIndex-1;
        var numReplicas, numReplicasNewCfg;
        do{
            newCommitIndex++;
            numReplicas=1;
            for (var i in clusterMembers) {
                if(clusterMembers[i]!=id) (function(serverId){
                    if(matchIndex[serverId]>=newCommitIndex+1) numReplicas++;
                })(clusterMembers[i]);
            }
            if(numReplicas>1 && log[newCommitIndex+1].term==currentTerm && log[newCommitIndex+1].command.type=='CFG' || commitEntries.newCfg){
                if(!commitEntries.newCfg) commitEntries.newCfg=log[newCommitIndex+1].command.clusterMembers;
                numReplicasNewCfg=0;
                for (var i in commitEntries.newCfg) {
                    if(commitEntries.newCfg[i]!=id) (function(serverId){
                        if(matchIndex[serverId]>=newCommitIndex+1) numReplicasNewCfg++;
                    })(commitEntries.newCfg[i]);
                    else numReplicasNewCfg++;
                }
            }
        } while (numReplicas>(Object.keys(matchIndex).length+1)/2 && (!commitEntries.newCfg || commitEntries.newCfg && numReplicasNewCfg==commitEntries.newCfg.length));
        delete commitEntries.newCfg;
        if(log[newCommitIndex].term==currentTerm){
            commitIndex=newCommitIndex;
            setImmediate(processEntries,commitIndex+1);
        }
        maybeNeedToCommit=false;
    }
}

function processEntries(upTo){
    if (processEntries.upTo == undefined){
        var entryIndex=lastApplied+1;
        if(entryIndex<upTo){
            processEntries.upTo = upTo;
            switch(log[entryIndex].command.type) {
                case 'GET':
                    if(log[entryIndex].clientId==id){
                        db.get(log[entryIndex].command.key, function (err, value) {
                            if (err) {
                                if (err.notFound) {
                                    // handle a 'NotFoundError' here
                                    //return
                                }
                                // I/O or other error, throw it
                                else throw err;
                            }
                            module.exports.emit('result',err,log[entryIndex].clientSeqNum,value);
                            lastApplied=entryIndex;
                            setImmediate(processEntries,processEntries.upTo);
                            processEntries.upTo=undefined;
                        });
                    }
                    else{
                        lastApplied=entryIndex;
                        setImmediate(processEntries,processEntries.upTo);
                        processEntries.upTo=undefined;
                    }
                    break;
                case 'PUT':
                    db.put(log[entryIndex].command.key,log[entryIndex].command.value, function (err) {
                        if (err){
                            // I/O or other error, throw it
                            throw err;
                        }
                        if(log[entryIndex].clientId==id) module.exports.emit('result',err,log[entryIndex].clientSeqNum);
                        lastApplied=entryIndex;
                        setImmediate(processEntries,processEntries.upTo);
                        processEntries.upTo=undefined;
                    });
                    break;
                case 'DEL':
                    db.del(log[entryIndex].command.key, function (err) {
                        if (err){
                            // I/O or other error, throw it
                            throw err;
                        }
                        if(log[entryIndex].clientId==id) module.exports.emit('result',err,log[entryIndex].clientSeqNum);
                        lastApplied=entryIndex;
                        setImmediate(processEntries,processEntries.upTo);
                        processEntries.upTo=undefined;
                    });
                    break;
                case 'CFG':
                    if(state=='l') heartbeatTimeout();
                    clusterMembers=log[entryIndex].command.clusterMembers;
                    if(clusterMembers.indexOf(id)<0) process.exit();
                    if(log[entryIndex].clientId==id) module.exports.emit('result',undefined,log[entryIndex].clientSeqNum);
                default:
                    lastApplied=entryIndex;
                    setImmediate(processEntries,processEntries.upTo);
                    processEntries.upTo=undefined;
                    break;
            }
        }
    }
    else if(upTo > processEntries.upTo) processEntries.upTo = upTo;
    if(!recoveryMode) while(lastApplied-log.firstIndex>maxAppliedEntriesInLog) log.shift();
}

//Internal classes

function LogEntry(clientId,clientSeqNum,command,term){
    this.clientId=clientId;
    this.clientSeqNum=clientSeqNum;
    this.command=command;
    this.term=term;
}

//Internal functions

function newLog(){
    var log=Object.create(null);
    log[0]=new LogEntry(id,0,null,0);
    log.firstIndex=0;
    log.length=1;
    log.push=function(value){ this[this.length++]=value; };
    log.pop=function(){ delete this[--this.length]; };
    log.shift=function(){ delete this[this.firstIndex++]; };
    log.slice=function(from,to){
        var array=[];
        for(var i=from;i<to;i++) array.push(this[i]);
        return array;
    }
    return log;
}


//Aux functions
function showArguments(a) {
for(var k in a)
console.log('\tPart', k, ':', a[k].toString());
};

function randomInt (low, high) {
    return Math.floor(Math.random() * (high - low) + low);
}

//Exports

module.exports=new EventEmitter();
module.exports.newEntries=newEntries;