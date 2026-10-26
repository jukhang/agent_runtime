import {test} from 'node:test';
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import vm from 'node:vm';
import {randomUUID} from 'node:crypto';
import {composerKeyboard} from '../src/centn_example/ui/composer.mjs';

test('rejected submission restores Journal status and preserves the original request',async()=>{
  const nodes=new Map();
  function element() {
    return {value:'',textContent:'',dataset:{},style:{},hidden:false,disabled:false,
      addEventListener(){},replaceChildren(){},append(){},showModal(){},
      querySelectorAll(){return [];},scrollHeight:0,scrollTop:0,clientHeight:0};
  }
  const document={
    getElementById(id){if(!nodes.has(id))nodes.set(id,element());return nodes.get(id);},
    querySelectorAll(){return [];},createElement:element,
  };
  const context=vm.createContext({document,composerKeyboard,crypto:{randomUUID},
    fetch:async()=>({ok:false,status:409,json:async()=>({detail:'Submission rejected'})}),
    setTimeout,clearTimeout,URL,console});
  const source=await readFile(new URL('../src/centn_example/ui/app.mjs',import.meta.url),'utf8');
  // Imports are supplied above; execute the real form handler in an isolated DOM harness.
  vm.runInContext(source.replace(/^import .*;\n/gm,''),context);
  vm.runInContext(`state={sessionId:'test',items:new Map(),runs:new Map(),plans:new Map(),
    tools:new Map(),artifacts:new Map(),inputRequests:new Map(),approvals:new Map()};
    config={name:'Agent',agent:{key:{namespace:'test',key:'agent'},revision:'1'}};`,context);
  document.getElementById('input').value='Question';
  await document.getElementById('composer').onsubmit({preventDefault(){}});
  assert.equal(document.getElementById('status').textContent,'就绪');
  assert.equal(document.getElementById('error').textContent,'Submission rejected');
  assert.equal(document.getElementById('input').value,'Question');
  assert.equal(document.getElementById('send').disabled,false);
  const key=vm.runInContext('pendingSubmission.body.idempotencyKey',context);
  await document.getElementById('composer').onsubmit({preventDefault(){}});
  assert.equal(vm.runInContext('pendingSubmission.body.idempotencyKey',context),key);
});
