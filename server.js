var id=process.argv[2],
    currentTerm=0,
    state='f',
    votedFor=null,
    log=[new LogEntry(id,null,0)],
    commitIndex=0,
    maybeNeedToCommit=false;
    lastApplied=0,
    nextIndex=Object.create(null),
    matchIndex=Object.create(null),
    recoveryMode=false,
    recoveryPrevLogIndex=0,
    grantedVotes=0,
    electionTime=randomInt(1500, 3000),
    heartbeatTime=750,
    commitTime=1000,
    zmq=require('zmq'),
    socket = zmq.socket('dealer'),
    levelup = require('level'),
    db = levelup('./'+id+'.db');
    
socket['identity']=id;
socket.connect(process.argv[3]);
function sendMessage(destination,message){
    console.log(message);
    socket.send(['',destination,'',message]);
}

var electionTimer=setTimeout(electionTimeout,electionTime);
var heartbeatTimer;
var newEntryInterval=setInterval(newEntry,1000);
var commitInterval=setInterval(commitEntries,commitTime);

for(var i=0; i<4; i++){
    if(i!=id) nextIndex[i]=1;
    if(i!=id) matchIndex[i]=0;
}

socket.on('message',function(){
    var args = Array.apply(null, arguments);
    if(args[3]=='Hola'); //showArguments(args);
    else{
    showArguments(args);
    var message=JSON.parse(args[3]);
    if(message.rpc=='appendEntries') appendEntries(message.term,message.leaderId,message.prevLogIndex,message.prevLogTerm,message.entries,message.leaderCommit);
    else if(message.rpc=='replyAppendEntries') replyAppendEntries(message.term,message.followerId,message.entriesToAppend,message.success);
    else if(message.rpc=='requestVote') requestVote(message.term,message.candidateId,message.lastLogIndex,message.lastLogTerm);
    else if(message.rpc=='replyVote') replyVote(message.term,message.voteGranted);
    }
});

sendMessage(process.argv[4],'Hola');

//RPCs

function appendEntries(term,leaderId,prevLogIndex,prevLogTerm,entries,leaderCommit){
    var message;
    if(term>=currentTerm){
        clearTimeout(electionTimer);
        if(term>currentTerm){
            /*Term evolution
            process.stdout.write(state);
            for(var i=currentTerm+1;i<term;i++) process.stdout.write(' ');*/
            currentTerm=term;
        }
        if(prevLogIndex<log.length && log[prevLogIndex].term==prevLogTerm){
            recoveryMode=false;
            for(var entry in entries) log.push(entries[entry]);
            message=JSON.stringify({rpc: 'replyAppendEntries', term: currentTerm, followerId: id, entriesToAppend: entries.length, success: true});
            if(leaderCommit>commitIndex){
                commitIndex=Math.min(leaderCommit,log.length-1);
                setImmediate(processEntries,commitIndex+1);
            }
            electionTimer=setTimeout(electionTimeout,electionTime);
        }
        else if(!recoveryMode || (recoveryMode && prevLogIndex<recoveryPrevLogIndex)){
            if(prevLogIndex<log.length) while(prevLogIndex<log.length) log.pop();
            recoveryMode=true;
            recoveryPrevLogIndex=prevLogIndex;
            message=JSON.stringify({rpc: 'replyAppendEntries', term: currentTerm, followerId: id, entriesToAppend: prevLogIndex, success: false});
            sendMessage(leaderId,message);
        }
    }
    else message=JSON.stringify({rpc: 'replyAppendEntries', term: currentTerm, followerId: id, entriesToAppend: entries.length, success: false});
    if(!recoveryMode) sendMessage(leaderId,message);
}

function replyAppendEntries(term,followerId,entriesToAppend,success){
    if(state=='l'){
        if(term>currentTerm){
            /*Term evolution
            process.stdout.write(state);
            for(var i=currentTerm+1;i<term;i++) process.stdout.write(' ');*/
            currentTerm=term;
            state='f';
            grantedVotes=0;
            votedFor=null;
        }
        else if(success){
            matchIndex[followerId]+=entriesToAppend;
            maybeNeedToCommit=true;
            if(nextIndex[followerId]<log.length){
                var message=JSON.stringify({rpc: 'appendEntries', term: currentTerm, leaderId: id, prevLogIndex: nextIndex[followerId]-1, prevLogTerm: log[nextIndex[followerId]-1].term,entries: log.slice(nextIndex[followerId],log.length), leaderCommit: commitIndex});
                sendMessage(followerId,message);
                nextIndex[followerId]+=log.length-nextIndex[followerId];
            }
        }
        else{
            nextIndex[followerId]=entriesToAppend;
            matchIndex[followerId]=nextIndex[followerId]-1;
            var message=JSON.stringify({rpc: 'appendEntries', term: currentTerm, leaderId: id, prevLogIndex: nextIndex[followerId]-1, prevLogTerm: log[nextIndex[followerId]-1].term,entries: [log[nextIndex[followerId]]], leaderCommit: commitIndex});
            sendMessage(followerId,message);
            nextIndex[followerId]+=1;
        }
    }
}

function requestVote(term,candidateId,lastLogIndex,lastLogTerm){
    var message;
    if(term>=currentTerm){
        if(term>currentTerm){
            /*Term evolution
            process.stdout.write(state);
            for(var i=currentTerm+1;i<term;i++) process.stdout.write(' ');*/
            currentTerm=term;
            state='f';
            votedFor=null;
            recoveryMode=false;
            clearTimeout(heartbeatTimer);
        }
        if((votedFor==null || votedFor==candidateId) && (lastLogTerm>log[log.length-1].term || lastLogTerm==log[log.length-1].term && lastLogIndex>=log.length-1)){
            votedFor=candidateId;
            message=JSON.stringify({rpc: 'replyVote', term: currentTerm, voteGranted: true});
        }
        else message=JSON.stringify({rpc: 'replyVote', term: currentTerm, voteGranted: false});
    }
    else message=JSON.stringify({rpc: 'replyVote', term: currentTerm, voteGranted: false});
    sendMessage(candidateId,message);
}

function replyVote(term,voteGranted){
    if(term>currentTerm){
        /*Term evolution
        process.stdout.write(state);
        for(var i=currentTerm+1;i<term;i++) process.stdout.write(' ');*/
        currentTerm=term;
        state='f';
        grantedVotes=0;
        votedFor=null;
    }
    else if(voteGranted && term==currentTerm){
        grantedVotes++;
        if(grantedVotes>(Object.keys(nextIndex).length+1)/2){
            //console.log("Election win");
            state='l';
            grantedVotes=0;
            for (var i in nextIndex) {
                (function(serverId){
                    nextIndex[serverId]=log.length;
                    matchIndex[serverId]=log.length-1;
                })(i);
            }
            heartbeatTimer=setTimeout(heartbeatTimeout,0);
            //NO! votedFor=null;
        }
    }
}

//Timeout functions

function electionTimeout(){
    /*Term evolution
    process.stdout.write(state);*/
		currentTerm++;
		state='c';
		votedFor=id;
		grantedVotes=1
		for (var i in nextIndex) {
        (function(serverId){
            var message=JSON.stringify({rpc: 'requestVote', term: currentTerm, candidateId: id, lastLogIndex: log.length-1, lastLogTerm: log[log.length-1].term});
            sendMessage(serverId,message);
        })(i);
		}
		clearTimeout(electionTimer);
		electionTimer=setTimeout(electionTimeout,electionTime);
}

function heartbeatTimeout(){
		for (var i in nextIndex) {
        (function(serverId){
            if(nextIndex[serverId]==log.length){
                var message=JSON.stringify({rpc: 'appendEntries', term: currentTerm, leaderId: id, prevLogIndex: log.length-1, prevLogTerm: log[log.length-1].term,entries: [], leaderCommit: commitIndex});
                sendMessage(serverId,message);
            }
        })(i);
		}
		clearTimeout(heartbeatTimer);
		heartbeatTimer=setTimeout(heartbeatTimeout,heartbeatTime);
		clearTimeout(electionTimer);
    electionTimer=setTimeout(electionTimeout,electionTime);
}

function newEntry(){
    if(state=='l'){
        var entry=new LogEntry(id,{type: 'PUT', key: log.length, value: (new Date()).toISOString()},currentTerm);
        var entry2=new LogEntry(id,{type: 'GET', key: log.length},currentTerm);
        for (var i in nextIndex) {
            (function(serverId){
                if(nextIndex[serverId]==log.length){
                    var message=JSON.stringify({rpc: 'appendEntries', term: currentTerm, leaderId: id, prevLogIndex: log.length-1, prevLogTerm: log[log.length-1].term,entries: [entry,entry2], leaderCommit: commitIndex});
                    sendMessage(serverId,message);
                    nextIndex[serverId]+=2;
                }
            })(i);
        }
        log.push(entry);
        log.push(entry2);
        clearTimeout(heartbeatTimer);
        heartbeatTimer=setTimeout(heartbeatTimeout,heartbeatTime);
        clearTimeout(electionTimer);
        electionTimer=setTimeout(electionTimeout,electionTime);
    }
}

function commitEntries(){
    if(maybeNeedToCommit){
        var newCommitIndex=commitIndex-1;
        var numReplicas;
        do{
            newCommitIndex++;
            numReplicas=1;
            for (var i in matchIndex) {
                (function(serverId){
                    if(matchIndex[serverId]>=newCommitIndex+1) numReplicas++;
                })(i);
            }
        } while (numReplicas>(Object.keys(matchIndex).length+1)/2);
        if(log[newCommitIndex].term==currentTerm){
            commitIndex=newCommitIndex;
            setImmediate(processEntries,commitIndex+1);
        }
        maybeNeedToCommit=false;
    }
}

function processEntries(upTo){
    for(var i=lastApplied+1;i<upTo;i++){
        (function(entryIndex){
            switch(log[entryIndex].command.type) {
                case 'GET':
                    if(log[entryIndex].clientId==id){
                        db.get(log[entryIndex].command.key, function (err, value) {
                            if (err) {
                              if (err.notFound) {
                                // handle a 'NotFoundError' here
                                return
                              }
                              // I/O or other error, pass it up the callback chain
                              return callback(err)
                            }
                            console.log(log[entryIndex].command.key, '=', value);
                          });
                    }
                    break;
                case 'PUT':
                    db.put(log[entryIndex].command.key,log[entryIndex].command.value);
                    break;
                case 'DEL':
                    db.del(log[entryIndex].command.key);
                    break;
            }
            lastApplied=entryIndex;
        })(i);
    }
}

//Internal classes

function LogEntry(clientId,command,term){
    this.clientId=clientId;
    this.command=command;
    this.term=term;
}


//Aux functions
function showArguments(a) {
for(var k in a)
console.log('\tPart', k, ':', a[k].toString());
};

function randomInt (low, high) {
    return Math.floor(Math.random() * (high - low) + low);
}
