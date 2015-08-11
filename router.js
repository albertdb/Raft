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
var debug=process.argv[3]=="true",
    zmq=require('zmq'),
    router=zmq.socket('router'),
    snappy = require('snappy');

router.bindSync('tcp://*:'+process.argv[2]);

router.on('message',function(){
    var args = Array.apply(null, arguments);
    if(debug){
        var aux=args.slice();
        if(aux[3]=='c') aux[4]=snappy.uncompressSync(aux[4]);
        showArguments(aux);
    }
    router.send([args[2],'',args[0],args[3],args[4]]);
});

//Aux functions
function showArguments(a) {
for(var k in a)
console.log('\tPart', k, ':', a[k].toString());
};