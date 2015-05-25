var id=process.argv[2];
    currentTerm=0,
    state='f',
    votedFor=null,
    log=[null],
    commitIndex=0,
    lastApplied=0,
    nextIndex=Object.create(null),
    matchIndex=Object.create(null),
    grantedVotes=0,
    electionTime=randomInt(1500, 3000),
    heartbeatTime=750,
    zmq=require('zmq'),
    socket = zmq.socket('dealer');
    
socket['identity']=id;
socket.connect(process.argv[3]);
function sendMessage(destination,message){
    socket.send(['',destination,'',message]);
}

var electionTimer=setTimeout(electionTimeout,electionTime);
var heartbeatTimer;
var newEntryInterval=setInterval(newEntry,1000);

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
        if(term>currentTerm){
            /*Term evolution
            process.stdout.write(state);
            for(var i=currentTerm+1;i<term;i++) process.stdout.write(' ');*/
            currentTerm=term;
        }
        if(log[prevLogIndex].term==prevLogTerm){
            for(var entry in entries) log.push(entry);
            message=JSON.stringify({rpc: 'replyAppendEntries', term: currentTerm, followerId: id, entriesToAppend: entries.length, success: true});
            if(leaderCommit>commitIndex) commitIndex=Math.min(leaderCommit,log.length-1);
        }
        else{
            while(prevLogIndex<log.length) log.pop();
            message=JSON.stringify({rpc: 'replyAppendEntries', term: currentTerm, followerId: id, entriesToAppend: entries.length, success: false});
        }
        clearTimeout(electionTimer);
        electionTimer=setTimeout(electionTimeout,electionTime);
    }
    else message=JSON.stringify({rpc: 'replyAppendEntries', term: currentTerm, followerId: id, entriesToAppend: entries.length, success: false});
    sendMessage(leaderId,message);
}

function replyAppendEntries(term,followerId,entriesToAppend,success){
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
        if(nextIndex[followerId]<log.length){
            var message=JSON.stringify({rpc: 'appendEntries', term: currentTerm, leaderId: id, prevLogIndex: nextIndex[followerId]-1, prevLogTerm: log[nextIndex[followerId]-1].term,entries: log.slice(nextIndex[followerId],log.length), leaderCommit: commitIndex});
            sendMessage(followerId,message);
            nextIndex[followerId]+=log.length-nextIndex[followerId];
        }
    }
    else{
        nextIndex[followerId]-=entriesToAppend+1;
        var message=JSON.stringify({rpc: 'appendEntries', term: currentTerm, leaderId: id, prevLogIndex: nextIndex[followerId]-1, prevLogTerm: log[nextIndex[followerId]-1].term,entries: [log[nextIndex[followerId]]], leaderCommit: commitIndex});
        sendMessage(followerId,message);
        nextIndex[followerId]+=1;
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
            clearTimeout(heartbeatTimer);
        }
        if(votedFor==null || votedFor==candidateId){
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
            var message=JSON.stringify({rpc: 'requestVote', term: currentTerm, candidateId: id});
            sendMessage(serverId,message);
        })(i);
		}
		clearTimeout(electionTimer);
		electionTimer=setTimeout(electionTimeout,electionTime);
}

function heartbeatTimeout(){
		for (var i in nextIndex) {
        (function(serverId){
            var message=JSON.stringify({rpc: 'appendEntries', term: currentTerm, leaderId: id, prevLogIndex: log.length-1, prevLogTerm: log[log.length-1].term,entries: null, leaderCommit: commitIndex});
            sendMessage(serverId,message);
        })(i);
		}
		clearTimeout(heartbeatTimer);
		heartbeatTimer=setTimeout(heartbeatTimeout,heartbeatTime);
		clearTimeout(electionTimer);
    electionTimer=setTimeout(electionTimeout,electionTime);
}

function newEntry(){
    if(state=='l'){
      var entry=new LogEntry((new Date()).toISOString(),currentTerm);
      for (var i in nextIndex) {
          (function(serverId){
              if(nextIndex[serverId]==log.length){
                  var message=JSON.stringify({rpc: 'appendEntries', term: currentTerm, leaderId: id, prevLogIndex: log.length-1, prevLogTerm: log[log.length-1].term,entries: [entry], leaderCommit: commitIndex});
                  sendMessage(serverId,message);
                  nextIndex[serverId]+=1;
              }
          })(i);
      }
      log.push(entry);
      clearTimeout(heartbeatTimer);
      heartbeatTimer=setTimeout(heartbeatTimeout,heartbeatTime);
      clearTimeout(electionTimer);
      electionTimer=setTimeout(electionTimeout,electionTime);
    }
}

//Internal classes

function LogEntry(command,term){
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
