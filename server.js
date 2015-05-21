var id=process.argv[2];
    currentTerm=0,
    state='f',
    votedFor=null,
    grantedVotes=0,
    electionTime=randomInt(1500, 3000),
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
    if(args[3]=='Hola') showArguments(args);
    else{
    showArguments(args);
    var message=JSON.parse(args[3]);
    if(message.rpc=='requestVote') requestVote(message.term,message.candidateId,message.lastLogIndex,message.lastLogTerm);
    else if(message.rpc=='replyVote') replyVote(message.term,message.voteGranted);
    }
});

//RPCs

function requestVote(term,candidateId,lastLogIndex,lastLogTerm){
    var message;
    if(term<currentTerm){
        message=JSON.stringify({rpc: 'replyVote', term: currentTerm, voteGranted: false});
    }
    else if(votedFor==null || votedFor=candidateId){
        message=JSON.stringify({rpc: 'replyVote', term: currentTerm, voteGranted: true});
    }
    sendMessage(candidateId,message);
}

function replyVote(term,voteGranted){
    if(term>currentTerm) currentTerm=term;
    if(voteGranted) grantedVotes++;
    if(grantedVotes>(Object.keys(serversIDs).length+1)/2) console.log("Election win");
}

sendMessage(process.argv[4],'Hola');

for(var i=0; i<3; i++){
    if(i!=id) serversIDs[i]=true;
}

var electionTimer=setTimeout(electionTimeout,electionTime);

function electionTimeout(){
		currentTerm++;
		state='c';
		votedFor=id;
		grantedVotes=1
		for (var i in serversIDs) {
        (function(serverId){
            var message=JSON.stringify({rpc: 'requestVote', term: currentTerm, candidateId: id})
            sendMessage(serverID,message);
        })(i);
		}
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
