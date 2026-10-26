// Framework-independent public Journal projection; cursors never pass through Number.
const decimal = value => {
  if (typeof value !== 'string' || !/^(0|[1-9][0-9]*)$/.test(value)) throw Error('Invalid cursor');
  return BigInt(value);
};
const clone = value => structuredClone(value);
const knownKinds = new Set(['turn.accepted','input.accepted','item.started','assistant.delta','analysis.delta',
  'item.completed','run.status','input.required','plan.updated','tool.started','tool.progress',
  'tool.finished','artifact.created','approval.required','approval.updated','command.updated','run.completed',
  'run.failed','run.cancelled','resource.revoked','model.prepared','mailbox.applied',
  'handoff.committed','checkpoint.migrated']);
const collections = ['items','runs','plans','tools','artifacts','inputRequests','approvals'];
const resourceKey = ref => JSON.stringify([ref.resourceType,ref.resourceId,ref.resourceVersion,ref.scope]);

export class Journal {
  constructor(sessionId, {includeAnalysis = false} = {}) {
    this.sessionId = sessionId;
    this.includeAnalysis = includeAnalysis;
    this.cursor = '0';
    this.authRevision = null;
    this.items = new Map();
    this.runs = new Map();
    this.plans = new Map();
    this.tools = new Map();
    this.artifacts = new Map();
    this.inputRequests = new Map();
    this.approvals = new Map();
  }

  snapshot(value) {
    if (value.sessionId !== this.sessionId || value.schemaRevision !== 1
        || (value.projectionVersion !== undefined && value.projectionVersion !== 1)) throw Error('Invalid snapshot');
    decimal(value.snapshotSeq); decimal(value.authRevision);
    const items = new Map();
    for (const item of value.items) {
      if (items.has(item.id)) throw Error('Duplicate item');
      items.set(item.id, clone(item));
    }
    this.items = items;
    this.runs = new Map(value.runs.map(run => [run.id, clone(run)]));
    this.plans = new Map(value.plans.map(plan => [plan.id, clone(plan)]));
    this.tools = new Map((value.tools || []).map(tool => [tool.id,clone(tool)]));
    this.artifacts = new Map((value.artifacts || []).map(artifact => [resourceKey(artifact.resourceRef),clone(artifact)]));
    this.inputRequests = new Map((value.inputRequests || []).map(input => [input.runId,clone(input)]));
    this.approvals = new Map((value.approvals || []).map(approval => [resourceKey(approval.approvalRef),clone(approval)]));
    this.cursor = value.snapshotSeq; this.authRevision = value.authRevision;
  }

  page(page) {
    if (page.kind === 'resync_required') throw Error('Snapshot required');
    if (page.sessionId !== this.sessionId) throw Error('Session mismatch');
    const cursor = decimal(page.cursor), after = decimal(page.afterSeq);
    if (cursor < after) throw Error('Cursor regressed');
    if (this.authRevision !== null && page.authRevision !== this.authRevision) throw Error('Authorization changed');
    if (cursor <= decimal(this.cursor)) return false;
    if (after !== decimal(this.cursor)) throw Error('Journal gap');
    // Applying a bad page must not leave partially visible output or advance its cursor.
    const next = new Journal(this.sessionId, {includeAnalysis:this.includeAnalysis});
    for (const name of collections) next[name] = clone(this[name]);
    let previous = after;
    for (const event of page.events) {
      const seq = decimal(event.sessionSeq);
      const visible = event.visibility === 'public' || (this.includeAnalysis
        && event.visibility === 'restricted' && event.kind === 'analysis.delta');
      if (event.sessionId !== this.sessionId || !visible
          || event.schemaRevision !== 1 || seq <= previous || seq > cursor) throw Error('Invalid envelope');
      previous = seq;
      next.event(event);
    }
    for (const name of collections) this[name] = next[name];
    this.cursor = page.cursor; this.authRevision = page.authRevision;
    return true;
  }

  async verifiedPage(page) {
    const next = new Journal(this.sessionId, {includeAnalysis:this.includeAnalysis});
    for (const name of collections) next[name] = clone(this[name]);
    next.cursor = this.cursor; next.authRevision = this.authRevision;
    if (!next.page(page)) return false;
    for (const event of page.events) {
      if (event.kind !== 'item.completed') continue;
      const item = next.items.get(event.itemId);
      for (const part of event.payload.channelDigests) {
        const text = item.channels.find(channel => channel.name === part.channel)?.text;
        if (text === undefined) throw Error('Unknown completed channel');
        const hash = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(text));
        const digest = [...new Uint8Array(hash)].map(byte => byte.toString(16).padStart(2,'0')).join('');
        if (digest !== part.digest) throw Error('Output integrity mismatch');
      }
    }
    for (const name of collections) this[name] = next[name];
    this.cursor = next.cursor; this.authRevision = next.authRevision;
    return true;
  }

  event(event) {
    if (!knownKinds.has(event.kind)) throw Error('Unsupported event kind');
    const p = event.payload;
    if (event.kind === 'turn.accepted') {
      if (this.items.has(p.userItem.id)) throw Error('Duplicate input item');
      this.items.set(p.userItem.id, clone(p.userItem));
      this.runs.set(event.runId, {id:event.runId,parentRunId:event.parentRunId || null,status:'queued',phase:'queued',stateVersion:'0'});
    } else if (event.kind === 'input.required') {
      this.inputRequests.set(event.runId,{id:event.eventId,runId:event.runId,
        stateVersion:p.stateVersion,question:p.question,requiredFacts:clone(p.requiredFacts)});
    } else if (event.kind === 'input.accepted') {
      if (this.items.has(p.userItem.id) || p.replyToRunId !== event.runId) throw Error('Invalid reply');
      this.items.set(p.userItem.id,clone(p.userItem));
      this.inputRequests.delete(event.runId);
    } else if (event.kind === 'item.started') {
      if (this.items.has(event.itemId) || new Set(p.channels).size !== p.channels.length) throw Error('Duplicate output item or channel');
      this.items.set(event.itemId, {id: event.itemId, role: p.role, type: p.itemType,
        revision: p.itemRevision, status: 'streaming', channels: p.channels.map(name => ({name, text: '', lastDeltaIndex: null}))});
      const run = this.runs.get(event.runId);
      if (run && !['completed','failed','cancelled'].includes(run.status)) {
        this.runs.set(event.runId, {...run,status:'running',phase:'model'});
      }
    } else if (event.kind === 'assistant.delta' || event.kind === 'analysis.delta') {
      const item = this.items.get(event.itemId);
      const channel = item?.channels.find(c => c.name === p.channel);
      if (!channel || item.status !== 'streaming' || item.revision !== p.itemRevision
          || typeof p.text !== 'string') throw Error('Invalid delta');
      const expected = channel.lastDeltaIndex === null ? 0n : decimal(channel.lastDeltaIndex) + 1n;
      if (decimal(p.deltaIndex) !== expected) throw Error('Delta gap');
      channel.text += p.text; channel.lastDeltaIndex = p.deltaIndex;
    } else if (event.kind === 'item.completed') {
      const item = this.items.get(event.itemId);
      if (!item || (item.status !== 'streaming' && item.status !== p.status) || item.revision !== p.itemRevision
          || !['completed','failed','cancelled'].includes(p.status)
          || !Array.isArray(p.channelDigests) || !Array.isArray(p.lastDeltaIndexes)) throw Error('Invalid completion');
      for (const entries of [p.channelDigests,p.lastDeltaIndexes]) {
        if (entries.length !== item.channels.length || new Set(entries.map(part => part.channel)).size !== entries.length
            || entries.some(part => !item.channels.some(channel => channel.name === part.channel))) throw Error('Completion omits channels');
      }
      for (const part of p.lastDeltaIndexes) {
        if (item.channels.find(c => c.name === part.channel)?.lastDeltaIndex !== part.lastDeltaIndex) throw Error('Completion gap');
      }
      item.status = p.status;
    } else if (event.kind === 'plan.updated') {
      this.plans.set(p.planId, {id: p.planId, revision: p.revision, steps: clone(p.steps)});
    } else if (event.kind === 'run.status') {
      this.runs.set(event.runId, {id: event.runId,parentRunId:event.parentRunId || null, ...clone(p)});
      if (p.status !== 'waiting_user') this.inputRequests.delete(event.runId);
    } else if (['run.completed', 'run.failed', 'run.cancelled'].includes(event.kind)) {
      this.runs.set(event.runId, {id: event.runId,parentRunId:event.parentRunId || null, ...clone(p), status: event.kind.slice(4), phase: 'terminal'});
      this.inputRequests.delete(event.runId);
    } else if (event.kind === 'tool.started') {
      if (this.tools.has(p.toolCallId)) throw Error('Duplicate tool start');
      this.tools.set(p.toolCallId,{id:p.toolCallId,runId:event.runId,name:p.name,
        publicArguments:clone(p.publicArguments),status:'running',summary:'',resultRefs:[],failure:null,createdSeq:event.sessionSeq});
    } else if (event.kind === 'tool.progress' || event.kind === 'tool.finished') {
      const tool = this.tools.get(p.toolCallId);
      if (!tool || tool.runId !== event.runId) throw Error('Tool event requires a start');
      if (event.kind === 'tool.progress') tool.summary = p.summary;
      else Object.assign(tool,{status:p.status,resultRefs:clone(p.resultRefs),failure:clone(p.failure)});
    } else if (event.kind === 'artifact.created') {
      this.artifacts.set(resourceKey(p.resourceRef),{runId:event.runId,...clone(p)});
    } else if (event.kind === 'approval.required') {
      if ([...this.approvals.values()].some(item => item.approvalRef.resourceId === p.approvalRef.resourceId)) throw Error('Approval request cannot overwrite an existing decision');
      this.approvals.set(resourceKey(p.approvalRef),{runId:event.runId,status:'pending',
        revision:'0',...clone(p)});
    } else if (event.kind === 'approval.updated') {
      if (p.approvalRef.resourceType !== 'approval' || p.approvalRef.resourceVersion !== p.revision
          || !['approved','rejected','expired','cancelled'].includes(p.status)) throw Error('Invalid approval decision');
      const key = [...this.approvals.keys()].find(key => this.approvals.get(key).approvalRef.resourceId === p.approvalRef.resourceId);
      const old = key && this.approvals.get(key);
      if (old && (old.runId !== event.runId || (old.status || 'pending') !== 'pending'
          || BigInt(p.revision) !== BigInt(old.revision || old.approvalRef.resourceVersion) + 1n)) throw Error('Approval decision conflict');
      if (key) this.approvals.delete(key);
      this.approvals.set(resourceKey(p.approvalRef),{runId:event.runId,...clone(p),
        summary:old?.summary || p.publicReason,allowedDecisions:[]});
    } else if (event.kind === 'resource.revoked') {
      // A host may revoke an individual resource without replacing the session.
      // Rebuild only through the current authorized snapshot, never from stale UI.
      throw Error('Resource authorization changed');
    }
  }
}

export async function* readSSE(body) {
  const reader = body.getReader(), decoder = new TextDecoder();
  let buffer = '';
  try {
    while (true) {
      const {value, done} = await reader.read();
      buffer = (buffer + decoder.decode(value, {stream: !done})).replace(/\r\n/g, '\n');
      if (buffer.length > 8 * 1024 * 1024) throw Error('SSE frame exceeds limit');
      let end;
      while ((end = buffer.indexOf('\n\n')) >= 0) {
        const frame = buffer.slice(0, end); buffer = buffer.slice(end + 2);
        const data = frame.split('\n').filter(line => line.startsWith('data:')).map(line => line.slice(5).trimStart()).join('\n');
        if (data) yield JSON.parse(data);
      }
      if (done) {
        if (buffer.trim()) throw Error('Incomplete SSE frame');
        return;
      }
    }
  } finally { await reader.cancel().catch(() => {}); reader.releaseLock(); }
}
