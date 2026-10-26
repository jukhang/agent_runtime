import {test} from 'node:test';
import assert from 'node:assert/strict';
import {Journal, readSSE} from '../src/centn_example/ui/journal.mjs';

const sessionId = 'session';
function event(kind, seq, payload, itemId='item') {
  return {schemaRevision:1, visibility:'public', sessionId, sessionSeq:String(seq),
    itemId, runId:'run', kind, payload};
}
function page(after, cursor, events) {
  return {sessionId, afterSeq:String(after), cursor:String(cursor), authRevision:'1', events};
}
const start = event('item.started',1,{itemRevision:'1',role:'assistant',itemType:'message',channels:['content','analysis']});
const delta = (channel,index,text,seq) => event(channel === 'content' ? 'assistant.delta' : 'analysis.delta',seq,
  {channel,deltaIndex:String(index),text,itemRevision:'1'});

test('approval decisions replay from Journal and cannot overwrite a terminal decision',()=>{
  const ref = {resourceType:'approval',resourceId:'approval',resourceVersion:'0',scope:{scopeId:'scope'}};
  const required = event('approval.required',1,{approvalRef:ref,summary:'Save report',
    expiresAt:'2027-01-01T00:00:00Z',allowedDecisions:['approved','rejected']});
  const updated = event('approval.updated',2,{approvalRef:{...ref,resourceVersion:'1'},
    revision:'1',status:'approved',expiresAt:'2027-01-01T00:00:00Z',publicReason:'Approved'});
  const state = new Journal(sessionId); state.page(page(0,2,[required,updated]));
  const decision = [...state.approvals.values()][0];
  assert.equal(decision.status,'approved'); assert.deepEqual(decision.allowedDecisions,[]);
  const restored = new Journal(sessionId); restored.snapshot({schemaRevision:1,sessionId,
    snapshotSeq:'2',authRevision:'1',items:[],runs:[],plans:[],approvals:[decision]});
  assert.deepEqual([...restored.approvals.values()],[decision]);
  assert.throws(()=>state.page(page(2,3,[{...updated,sessionSeq:'3',payload:{...updated.payload,
    revision:'2',approvalRef:{...ref,resourceVersion:'2'},status:'rejected'}}])));
  assert.equal(state.cursor,'2');
});

test('content and analysis stay separate; duplicate pages do not replay output',()=>{
  const state = new Journal(sessionId);
  const first=page(0,3,[start,delta('analysis',0,'thinking',2),delta('content',0,'answer',3)]);
  assert.equal(state.page(first),true); assert.equal(state.page(first),false);
  assert.deepEqual(state.items.get('item').channels.map(c=>c.text),['answer','thinking']);
});
test('malformed page is atomic and does not consume its cursor',()=>{
  const state = new Journal(sessionId); state.page(page(0,1,[start]));
  assert.throws(()=>state.page(page(1,3,[delta('content',0,'a',2),delta('content',8,'bad',3)])));
  assert.equal(state.cursor,'1'); assert.equal(state.items.get('item').channels[0].text,'');
});
test('hidden events may advance the cursor; large cursors remain exact',()=>{
  const state = new Journal(sessionId);
  state.snapshot({schemaRevision:1,sessionId,snapshotSeq:'9007199254740993',authRevision:'1',items:[],runs:[],plans:[]});
  state.page(page('9007199254740993','9007199254740995',[]));
  assert.equal(state.cursor,'9007199254740995');
});
test('authorization change, foreign session and replay gaps fail closed',()=>{
  for(const change of [{authRevision:'2'},{sessionId:'other'},{afterSeq:'2'}]){
    const state = new Journal(sessionId); state.page(page(0,1,[start]));
    assert.throws(()=>state.page({...page(1,2,[]),...change}));
    assert.equal(state.cursor,'1');
  }
});
test('snapshot and live replay produce the same visible content',()=>{
  const live = new Journal(sessionId); live.page(page(0,2,[start,delta('content',0,'hello',2)]));
  const history = new Journal(sessionId); history.snapshot({schemaRevision:1, sessionId,
    snapshotSeq:'2',authRevision:'1',items:[...live.items.values()],runs:[],plans:[]});
  const next=page(2,3,[delta('content',1,' world',3)]); live.page(next); history.page(next);
  assert.deepEqual([...history.items], [...live.items]);
});
test('SSE parsing supports UTF8 and CRLF split across arbitrary bytes', async()=>{
  const bytes=new TextEncoder().encode(': ping\r\n\r\ndata: {"text":"你好"}\r\n\r\n');
  const body=new ReadableStream({start(controller){for(const b of bytes)controller.enqueue(Uint8Array.of(b));controller.close();}});
  const values=[]; for await(const item of readSSE(body)) values.push(item);
  assert.deepEqual(values,[{text:'你好'}]);
});

test('unknown event kinds and incomplete completions never advance the cursor',()=>{
  const state = new Journal(sessionId); state.page(page(0,1,[start]));
  assert.throws(()=>state.page(page(1,2,[event('future.unknown',2,{})])));
  assert.throws(()=>state.page(page(1,2,[event('item.completed',2,
    {itemRevision:'1',status:'completed',channelDigests:[],lastDeltaIndexes:[]})])));
  assert.equal(state.cursor,'1'); assert.equal(state.items.get('item').status,'streaming');
});

test('completion digest is verified before visible state or cursor commits',async()=>{
  const state = new Journal(sessionId); state.page(page(0,3,[start,delta('content',0,'answer',2),delta('analysis',0,'why',3)]));
  const digest = async text => Buffer.from(await crypto.subtle.digest('SHA-256',new TextEncoder().encode(text))).toString('hex');
  const payload = {itemRevision:'1',status:'completed',
    channelDigests:[{channel:'content',digest:await digest('answer')},{channel:'analysis',digest:await digest('why')}],
    lastDeltaIndexes:[{channel:'content',lastDeltaIndex:'0'},{channel:'analysis',lastDeltaIndex:'0'}]};
  const invalid=structuredClone(payload); invalid.channelDigests[0].digest='0'.repeat(64);
  await assert.rejects(()=>state.verifiedPage(page(3,4,[event('item.completed',4,invalid)])),/integrity/);
  assert.equal(state.cursor,'3'); assert.equal(state.items.get('item').status,'streaming');
  await state.verifiedPage(page(3,4,[event('item.completed',4,payload)]));
  assert.equal(state.cursor,'4'); assert.equal(state.items.get('item').status,'completed');
  await state.verifiedPage(page(4,5,[event('item.completed',5,payload)]));
  assert.equal(state.cursor,'5');
  await assert.rejects(()=>state.verifiedPage(page(5,6,[event('item.completed',6,{...payload,status:'failed'})])));
  assert.equal(state.cursor,'5');
  await assert.rejects(()=>state.verifiedPage(page(5,6,[event('item.completed',6,invalid)])),/integrity/);
  assert.equal(state.cursor,'5');
});

test('restricted analysis requires an explicit view and never permits internal events',()=>{
  const restricted = {...delta('analysis',0,'private',2), visibility:'restricted'};
  const ordinary = new Journal(sessionId);
  assert.throws(()=>ordinary.page(page(0,2,[start,restricted])));
  assert.equal(ordinary.cursor,'0');
  const authorized = new Journal(sessionId,{includeAnalysis:true});
  authorized.page(page(0,2,[start,restricted]));
  assert.equal(authorized.items.get('item').channels[1].text,'private');
  assert.throws(()=>authorized.page(page(2,3,[{...event('model.prepared',3,{}),visibility:'restricted'}])));
  assert.equal(authorized.cursor,'2');
});

test('tool, artifact, waiting input and failure details survive snapshot restoration',()=>{
  const live = new Journal(sessionId), resourceRef = {resourceType:'artifact',resourceId:'a',resourceVersion:'1',scope:{scopeId:'s'}};
  live.page(page(0,5,[
    event('tool.started',1,{toolCallId:'t',name:'multiply',publicArguments:{a:23,b:17}}),
    event('tool.finished',2,{toolCallId:'t',status:'completed',resultRefs:[],failure:null}),
    event('artifact.created',3,{resourceRef,title:'Report',mime:'text/markdown',previewStatus:'ready'}),
    event('input.required',4,{question:'Which quarter?',requiredFacts:['quarter'],stateVersion:'3'}),
    event('run.status',5,{status:'waiting_user',stateVersion:'3',phase:'waiting'})
  ]));
  const history = new Journal(sessionId);
  history.snapshot({schemaRevision:1,sessionId,snapshotSeq:'5',authRevision:'1',items:[],plans:[],
    runs:[...live.runs.values()],tools:[...live.tools.values()],artifacts:[...live.artifacts.values()],
    inputRequests:[...live.inputRequests.values()],approvals:[]});
  for (const name of ['tools','artifacts','inputRequests']) assert.deepEqual([...history[name]], [...live[name]]);
  const fail=page(5,6,[event('run.failed',6,{stateVersion:'4',failure:{publicMessage:'Provider unavailable'},
    nextSteps:[{action:'retry_same',label:'Retry'}],publicMessage:'Task failed'})]);
  live.page(fail); history.page(fail);
  assert.deepEqual([...history.runs], [...live.runs]);
  assert.equal(history.inputRequests.size,0);
  assert.equal(history.runs.get('run').failure.publicMessage,'Provider unavailable');
});

test('human reply live/history removes only its bound question and remains idempotent',()=>{
  const live = new Journal(sessionId);
  live.page(page(0,2,[
    event('input.required',1,{question:'Which quarter?',requiredFacts:['quarter'],stateVersion:'3'}),
    {...event('input.required',2,{question:'Which year?',requiredFacts:['year'],stateVersion:'4'}),runId:'other'}
  ]));
  const history = new Journal(sessionId);
  history.snapshot({schemaRevision:1,projectionVersion:1,sessionId,snapshotSeq:'2',authRevision:'1',
    items:[],runs:[],plans:[],inputRequests:[...live.inputRequests.values()]});
  const reply = page(2,3,[event('input.accepted',3,{stateVersion:'4',replyToRunId:'run',
    userItem:{id:'reply',role:'user',type:'user_reply',status:'completed',revision:'1',
      channels:[{name:'content',text:'2026 Q3',lastDeltaIndex:null}]}})]);
  live.page(reply); history.page(reply);
  assert.deepEqual([...live.inputRequests.keys()],['other']);
  assert.deepEqual([...live.items], [...history.items]);
  assert.deepEqual([...live.inputRequests], [...history.inputRequests]);
  assert.equal(live.page(reply),false);
  assert.throws(()=>live.page(page(3,4,[event('input.accepted',4,
    {...reply.events[0].payload,replyToRunId:'other'})])));
  assert.equal(live.cursor,'3');
});

test('root and child run identities remain distinct across status and restore',()=>{
  const live = new Journal(sessionId);
  live.page(page(0,2,[
    {...event('run.status',1,{status:'running',stateVersion:'1',phase:'model'}),parentRunId:null},
    {...event('run.status',2,{status:'running',stateVersion:'1',phase:'model'}),runId:'child',parentRunId:'run'}
  ]));
  const history = new Journal(sessionId);
  history.snapshot({schemaRevision:1,projectionVersion:1,sessionId,snapshotSeq:'2',authRevision:'1',
    items:[],plans:[],runs:[...live.runs.values()]});
  assert.equal([...history.runs.values()].filter(run=>!run.parentRunId).at(-1).id,'run');
});
