var zmq=require('zmq')
    router=zmq.socket('router');

frontend.bindSync('tcp://*:'+process.argv[2]);

router.on('message',function(){
    var args = Array.apply(null, arguments);
    showArguments(args);
    router.send([args[2],'',args[0],'',args[4]]);
});

//Aux functions
function showArguments(a) {
for(var k in a)
console.log('\tPart', k, ':', a[k].toString());
};