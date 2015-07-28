var debug=process.argv[3]=="true",
    zmq=require('zmq'),
    router=zmq.socket('router');

router.bindSync('tcp://*:'+process.argv[2]);

router.on('message',function(){
    var args = Array.apply(null, arguments);
    if(debug) showArguments(args);
    router.send([args[2],'',args[0],'',args[4]]);
});

//Aux functions
function showArguments(a) {
for(var k in a)
console.log('\tPart', k, ':', a[k].toString());
};