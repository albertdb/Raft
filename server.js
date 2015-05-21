var currentTerm=0,
    state='f',
    votedFor=null
    zmq=require('zmq'),
    socket = zmq.socket('dealer');
    
socket['identity']=process.argv[2];

socket.connect(process.argv[3]);

socket.on('message',function(){
    var args = Array.apply(null, arguments);
    showArguments(args);
});

socket.send(['',process.argv[4],'','Hola']);

//Aux functions
function showArguments(a) {
for(var k in a)
console.log('\tPart', k, ':', a[k].toString());
};