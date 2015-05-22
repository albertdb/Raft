var id=process.argv[2];
    currentTerm=0,
    state='f',
    votedFor=null,
    grantedVotes=0,
    electionTime=randomInt(1500, 3000),
    heartbeatTime=750,
    serversIDs=Object.create(null),
    zmq=require('zmq'),
    socket = zmq.socket('dealer');
    
socket['identity']=id;
socket.connect(process.argv[3]);
function sendMessage(destination,message){
    socket.send(['',destination,'',message]);
}

socket.on('message',function(){
    var args = Array.apply(null, arguments);
    if(args[3]=='Hola'); //showArguments(args);
    else{
    showArguments(args);
    var message=JSON.parse(args[3]);
    if(message.rpc=='appendEntries') appendEntries(message.term,message.leaderId,message.prevLogIndex,message.prevLogTerm,message.entries,message.leaderCommit);
    else if(message.rpc=='requestVote') requestVote(message.term,message.candidateId,message.lastLogIndex,message.lastLogTerm);
    else if(message.rpc=='replyVote') replyVote(message.term,message.voteGranted);
    }
});

//RPCs

function appendEntries(term,leaderId,prevLogIndex,prevLogTerm,entries,leaderCommit){
    if(term>=currentTerm){
        if(term>currentTerm){
            /*Term evolution
            process.stdout.write(state);
            for(var i=currentTerm+1;i<term;i++) process.stdout.write(' ');*/
            currentTerm=term;
        }
        clearTimeout(electionTimer);
        electionTimer=setTimeout(electionTimeout,electionTime);
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
        if(grantedVotes>(Object.keys(serversIDs).length+1)/2){
            //console.log("Election win");
            state='l';
            grantedVotes=0;
            heartbeatTimer=setTimeout(heartbeatTimeout,0);
            //NO! votedFor=null;
        }
    }
}

sendMessage(process.argv[4],'Hola');

for(var i=0; i<4; i++){
    if(i!=id) serversIDs[i]=true;
}

var electionTimer=setTimeout(electionTimeout,electionTime);

function electionTimeout(){
    /*Term evolution
    process.stdout.write(state);*/
		currentTerm++;
		state='c';
		votedFor=id;
		grantedVotes=1
		for (var i in serversIDs) {
        (function(serverId){
            var message=JSON.stringify({rpc: 'requestVote', term: currentTerm, candidateId: id});
            sendMessage(serverId,message);
        })(i);
		}
		clearTimeout(electionTimer);
		electionTimer=setTimeout(electionTimeout,electionTime);
}

var heartbeatTimer;

function heartbeatTimeout(){
		for (var i in serversIDs) {
        (function(serverId){
            var message=JSON.stringify({rpc: 'appendEntries', term: currentTerm, leaderId: id, prevLogIndex: null, prevLogTerm: null,entries: null, leaderCommit: null});
            sendMessage(serverId,message);
        })(i);
		}
		clearTimeout(heartbeatTimer);
		heartbeatTimer=setTimeout(heartbeatTimeout,heartbeatTime);
		clearTimeout(electionTimer);
    electionTimer=setTimeout(electionTimeout,electionTime);
}

//Aux functions
function showArguments(a) {
for(var k in a)
console.log('\tPart', k, ':', a[k].toString());
};

function randomInt (low, high) {
    return Math.floor(Math.random() * (high - low) + low);
}
