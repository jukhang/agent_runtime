import {Journal, readSSE} from './journal.mjs';
import {composerKeyboard} from './composer.mjs';
const $ = id => document.getElementById(id);
let token = '', config, state, controller;
let generation = 0;
let pendingSubmission = null, replySelection = null;
let resources = [], selectedResources = new Set();
let refreshTimer, refreshSequence = 0;
let chartPreviewUrl;
let submitting = false, followOutput = true;
const statuses = {idle:'就绪',queued:'等待执行',running:'正在处理',paused:'已暂停',
  waiting_user:'等待你的回复',waiting_tool:'工具执行中',completed:'已完成',
  failed:'未能完成',cancelled:'已取消',cancelling:'正在取消'};
$('timeline').addEventListener('scroll', () => {
  const view = $('timeline');
  followOutput = view.scrollHeight - view.scrollTop - view.clientHeight < 80;
});
function resizeInput() {
  $('input').style.height = 'auto';
  $('input').style.height = `${Math.min($('input').scrollHeight, 220)}px`;
}
const keyboard = composerKeyboard(() => {
  if (!submitting && !$('send').disabled) $('composer').requestSubmit($('send'));
});
for (const type of ['compositionstart','compositionend','keydown','keyup']) {
  $('input').addEventListener(type, event => keyboard[type](event));
}
$('input').addEventListener('input', resizeInput);
function clearChartPreview() {
  if (chartPreviewUrl) URL.revokeObjectURL(chartPreviewUrl);
  chartPreviewUrl = null;
  document.getElementById('resource-chart')?.remove();
}

async function api(path, options = {}) {
  const response = await fetch(path, {...options, headers: {'Content-Type':'application/json',
    Authorization: `Bearer ${token}`, ...options.headers}});
  if (!response.ok) {
    const error = await response.json().catch(() => ({}));
    throw Error(error.message || error.detail || `HTTP ${response.status}`);
  }
  return response;
}

function render() {
  if (!state) return;
  const messages = $('messages');
  $('empty-state').hidden = state.items.size > 0 || state.runs.size > 0;
  const visibleIds = new Set([...state.items.keys()].map(id => `item-${id}`));
  for (const article of messages.querySelectorAll('article')) {
    if (!visibleIds.has(article.id)) article.remove();
  }
  for (const item of state.items.values()) {
    let article = document.getElementById(`item-${item.id}`);
    if (!article) {
      article = document.createElement('article'); article.id = `item-${item.id}`;
      article.dataset.role = item.role;
      const label = document.createElement('strong'); label.textContent = item.role === 'user' ? '你' : config.name;
      const details = document.createElement('details'), summary = document.createElement('summary');
      details.className = 'thinking';
      summary.textContent = '思考过程'; details.append(summary, document.createElement('pre'));
      const content = document.createElement('div'); content.className = 'content';
      article.append(label, details, content); messages.append(article);
    }
    const content = item.channels.find(c => c.name === 'content')?.text || '';
    const analysis = item.channels.find(c => c.name === 'analysis')?.text || '';
    const contentNode = article.querySelector('.content');
    const thinking = article.querySelector('details');
    const analysisNode = article.querySelector('pre');
    // Retain nodes and the user's disclosure/selection state across delta pages.
    if (contentNode.textContent !== content) contentNode.textContent = content;
    if (analysisNode.textContent !== analysis) analysisNode.textContent = analysis;
    const thinkingNow = item.status === 'streaming' && !content;
    article.dataset.streaming = String(item.role !== 'user' && item.status === 'streaming');
    thinking.dataset.active = String(thinkingNow);
    thinking.querySelector('summary').textContent = thinkingNow ? '正在思考' : '思考过程';
    thinking.hidden = !analysis;
  }
  $('plans').replaceChildren();
  for (const plan of state.plans.values()) {
    const list = document.createElement('ol');
    for (const step of plan.steps) {
      const li = document.createElement('li'); li.textContent = `${step.label} · ${step.status}`; list.append(li);
    }
    $('plans').append(list);
  }
  const run = [...state.runs.values()].filter(run => !run.parentRunId).at(-1);
  const streaming = [...state.items.values()].some(item => item.status === 'streaming');
  const status = run?.status || (streaming ? 'running' : 'idle');
  $('status').dataset.state = status;
  $('status').textContent = run?.publicMessage || run?.publicReason || statuses[run?.status]
    || (streaming ? '正在回答' : '就绪');
  for (const button of document.querySelectorAll('[data-control]')) {
    const allowed = run?.allowedControls || (['running','queued'].includes(run?.status) ? ['pause','cancel']
      : run?.status === 'paused' ? ['resume','cancel'] : ['waiting_tool','waiting_user'].includes(run?.status) ? ['cancel'] : []);
    button.disabled = !allowed.includes(button.dataset.control);
  }
  const failure = run?.failure;
  if (failure) $('error').textContent = [failure.publicMessage,
    ...(run?.nextSteps || failure.recoveryOptions || []).map(option => option.label || option.publicMessage || option.action)].filter(Boolean).join('\n');
  $('tools').textContent = [...state.tools.values()].map(tool => `${tool.name} · ${tool.status} ${tool.summary}`).join('\n');
  $('requests').replaceChildren();
  for (const input of state.inputRequests.values()) {
    const question = document.createElement('p'); question.textContent = input.question;
    $('requests').append(question);
  }
  const choices = [...state.inputRequests.values()];
  if (replySelection === null) replySelection = choices.at(-1)?.runId || '';
  if (replySelection && !choices.some(input => input.runId === replySelection)) replySelection = '';
  $('reply-target').replaceChildren();
  const newTask = document.createElement('option'); newTask.value = ''; newTask.textContent = '新任务';
  $('reply-target').append(newTask);
  for (const input of choices) {
    const option = document.createElement('option'); option.value = input.runId;
    option.textContent = `回复：${input.question}`; $('reply-target').append(option);
  }
  $('reply-target').value = replySelection;
  $('reply-label').hidden = !choices.length;
  $('input').placeholder = replySelection ? '回复所选问题，继续该任务' : '输入问题或新任务';
  $('artifacts').replaceChildren();
  for (const artifact of state.artifacts.values()) {
    const p = document.createElement('p'); p.textContent = `${artifact.title} · ${artifact.mime}`; $('artifacts').append(p);
  }
  renderApprovals();
  if (followOutput) $('timeline').scrollTop = $('timeline').scrollHeight;
}

async function list() {
  const sessions = await (await api('/api/sessions')).json();
  $('sessions').replaceChildren();
  for (const session of sessions) {
    const button = document.createElement('button'); button.textContent = session.title || session.id.slice(0,8);
    button.classList.toggle('selected', session.id === state?.sessionId);
    button.onclick = () => select(session.id).catch(showError); $('sessions').append(button);
  }
  return sessions;
}

async function select(id) {
  controller?.abort(); const mine = ++generation;
  clearTimeout(refreshTimer); $('approvals').replaceChildren();
  $('resource-dialog').close(); $('resource-text').textContent = '';
  clearChartPreview();
  pendingSubmission = null; replySelection = null;
  followOutput = true;
  resources = []; selectedResources.clear(); renderResources();
  controller = new AbortController();
  state = new Journal(id, {includeAnalysis:config.thinking}); $('messages').replaceChildren(); $('error').textContent = '';
  await restore(); if (mine !== generation) return; render(); await loadResources(); await list();
  if (mine !== generation) return;
  history.replaceState(null, '', `/?session=${encodeURIComponent(id)}`);
  void subscribe(mine, controller.signal);
  void refreshHostState(mine);
}

async function refreshHostState(mine) {
  const ticket = ++refreshSequence;
  try {
    const target = state;
    if (mine !== generation || ticket !== refreshSequence) return;
    await loadResources();
  } catch (error) { if (mine === generation) showError(error); }
  if (mine === generation && ticket === refreshSequence) refreshTimer = setTimeout(() => void refreshHostState(mine), 2000);
}

function renderApprovals() {
  $('approvals').replaceChildren();
  for (const value of state?.approvals.values() || []) {
    const row = {...value,id:value.approvalRef.resourceId,
      revision:value.revision || value.approvalRef.resourceVersion,status:value.status || 'pending'};
    const section = document.createElement('div'), status = document.createElement('p');
    status.textContent = `${row.summary} · ${row.status}`; section.append(status);
    if (row.status === 'pending' && state.runs.get(row.runId)?.status === 'waiting_user') for (const [label,decision] of [['批准','approved'],['拒绝','rejected']]) {
      const button = document.createElement('button'); button.textContent = label;
      button.onclick = async () => {
        const target = state; button.disabled = true;
        try {
          await api(`/api/sessions/${target.sessionId}/approvals`, {method:'POST',body:JSON.stringify({
            approvalId:row.id,expectedVersion:row.revision,decision,
            idempotencyKey:`approval:${row.id}:${row.revision}:${decision}`})});
        } catch(error) { if (state === target) showError(error); }
        finally { button.disabled = false; }
      };
      section.append(button);
    }
    $('approvals').append(section);
  }
}

async function restore() {
  const target = state;
  const snapshot = await (await api(`/api/runtime/sessions/${target.sessionId}/snapshot`)).json();
  if (state === target) target.snapshot(snapshot);
}

async function subscribe(mine, signal) {
  let delay = 300;
  while (!signal.aborted && mine === generation) {
    try {
      const response = await api(`/api/sessions/${state.sessionId}/events?after=${state.cursor}`, {signal});
      for await (const page of readSSE(response.body)) {
        if (signal.aborted || mine !== generation) return;
        if (page.kind === 'resync_required') {
          if (page.reason === 'authorization_changed') {
            clearTimeout(refreshTimer); resources = []; selectedResources.clear();
            renderResources(); $('approvals').replaceChildren();
            $('resource-dialog').close(); $('resource-text').textContent = '';
            for (const name of ['items','runs','plans','tools','artifacts','inputRequests','approvals']) state[name].clear();
            $('messages').replaceChildren(); render();
          }
          await restore(); if (mine !== generation) return; render(); break;
        }
        const target = state;
        await target.verifiedPage(page);
        if (signal.aborted || mine !== generation) return;
        render(); delay = 300;
      }
    } catch (error) {
      if (signal.aborted || mine !== generation) return;
      showError(error);
      // Never guess past a malformed/gapped page. Rebuild from the authorized snapshot.
      try {
        await restore(); if (mine !== generation) return;
        $('messages').replaceChildren(); render();
      } catch (error) { if (mine === generation) showError(error); }
    }
    await new Promise(resolve => setTimeout(resolve, delay)); delay = Math.min(delay * 2, 5000);
  }
}

function showError(error) { $('error').textContent = String(error.message || error); }

async function loadResources() {
  const target = state;
  const value = await (await api(`/api/sessions/${target.sessionId}/resources`)).json();
  if (state !== target) return;
  const changed = JSON.stringify(resources) !== JSON.stringify(value);
  resources = value;
  if ($('resource-dialog').open && !resources.some(item => item.resourceRef.resourceId === $('resource-dialog').dataset.resourceId)) {
    clearChartPreview(); $('resource-text').textContent = ''; $('resource-dialog').close();
  }
  const ids = new Set(resources.map(item => item.resourceRef.resourceId));
  selectedResources = new Set([...selectedResources].filter(id => ids.has(id)));
  if (changed) renderResources();
}
function renderResources() {
  $('resource-list').replaceChildren();
  for (const item of resources) {
    const row = document.createElement('div'); row.className = 'resource-row';
    const label = document.createElement('label'), input = document.createElement('input');
    input.type = 'checkbox'; input.checked = selectedResources.has(item.resourceRef.resourceId);
    input.onchange = () => { input.checked ? selectedResources.add(item.resourceRef.resourceId)
      : selectedResources.delete(item.resourceRef.resourceId); pendingSubmission = null; };
    label.append(input,document.createTextNode(item.title)); row.append(label);
    for (const [name,route] of [['预览','preview'],['下载','download'],['撤销','revoke']]) {
      const button = document.createElement('button'); button.type = 'button'; button.textContent = name;
      button.onclick = async () => {
        const target = state;
        button.disabled = true;
        try {
          const response = await api(`/api/resources/${route}`,{method:'POST',body:JSON.stringify({resourceRef:item.resourceRef})});
          if (route === 'preview') {
            clearChartPreview();
            if (item.mime === 'image/png') {
              const blob = await response.blob(); if (state !== target) return;
              chartPreviewUrl = URL.createObjectURL(blob);
              const image = document.createElement('img'); image.id = 'resource-chart';
              image.alt = item.title; image.src = chartPreviewUrl;
              image.style.maxWidth = '100%'; image.style.height = 'auto';
              $('resource-text').textContent = ''; $('resource-text').before(image);
            } else {
              const result = await response.json(); if (state !== target) return;
              $('resource-text').textContent = result.text;
            }
            $('resource-title').textContent = item.title;
            $('resource-dialog').dataset.resourceId = item.resourceRef.resourceId;
            $('resource-dialog').showModal();
          } else if (route === 'download') {
            const body = await response.blob(); if (state !== target) return;
            const url = URL.createObjectURL(body), link = document.createElement('a');
            link.href = url; link.download = item.title; document.body.append(link); link.click(); link.remove();
            setTimeout(()=>URL.revokeObjectURL(url),1000);
          } else {
            if (state !== target) return;
            pendingSubmission = null; $('resource-text').textContent = ''; $('resource-dialog').close();
            clearChartPreview();
            await loadResources();
          }
        } catch(error) { showError(error); } finally { button.disabled = false; }
      };
      row.append(button);
    }
    $('resource-list').append(row);
  }
}
$('resource-close').onclick = () => { clearChartPreview(); $('resource-dialog').close(); };
$('resource-dialog').addEventListener('close', clearChartPreview);
$('resource-file').onchange = async () => {
  const file = $('resource-file').files[0]; if (!file || !state) return;
  const target = state;
  try {
    if (file.size > 1048576) throw Error('资料不得超过 1 MiB');
    const text = new TextDecoder('utf-8',{fatal:true}).decode(await file.arrayBuffer());
    const result = await (await api('/api/resources',{method:'POST',body:JSON.stringify({
      sessionId:target.sessionId,idempotencyKey:crypto.randomUUID(),title:file.name,
      text,kind:$('resource-kind').value})})).json();
    if (state !== target) return;
    selectedResources.add(result.resourceRef.resourceId); pendingSubmission = null;
    await loadResources();
  } catch(error) { showError(error); } finally { $('resource-file').value = ''; }
};
$('reply-target').onchange = () => { replySelection = $('reply-target').value; pendingSubmission = null; render(); };
$('new').onclick = async () => {
  try { const session = await (await api('/api/sessions', {method:'POST'})).json(); await select(session.id); }
  catch (error) { showError(error); }
};
$('composer').onsubmit = async event => {
  event.preventDefault(); if (submitting || !state || !$('input').value.trim()) return;
  const target = state;
  const text = $('input').value, button = $('send');
  submitting = true; button.disabled = true; followOutput = true;
  $('error').textContent = ''; $('status').textContent = '正在分析问题';
  try {
    if (!pendingSubmission || pendingSubmission.text !== text || pendingSubmission.sessionId !== state.sessionId) {
      const reply = [...state.inputRequests.values()].find(input => input.runId === replySelection);
      if (reply && selectedResources.size) throw Error('当前澄清回复只支持文字；请取消资料选择或选择新任务。');
      pendingSubmission = {text,sessionId:state.sessionId,path:reply ? '/api/interactions' : '/api/turns',
        body:reply ? {sessionId:state.sessionId,runId:reply.runId,expectedVersion:reply.stateVersion,
          idempotencyKey:crypto.randomUUID(),text} : {sessionId:state.sessionId,
          agent:config.agent,idempotencyKey:crypto.randomUUID(),inputs:[{kind:'text',text},
            ...resources.filter(item=>selectedResources.has(item.resourceRef.resourceId))
              .map(item=>({kind:'resource',resourceRef:item.resourceRef}))]}};
    }
    const submission = pendingSubmission;
    await api(submission.path,{method:'POST',body:JSON.stringify(submission.body)});
    if (pendingSubmission === submission) pendingSubmission = null;
    if (state === target) {
      selectedResources.clear(); renderResources();
      if ($('input').value === text) { $('input').value = ''; resizeInput(); }
    }
  } catch (error) {
    if (state === target) { render(); showError(error); }
  }
  finally { submitting = false; button.disabled = false; }
};
for (const button of document.querySelectorAll('[data-control]')) button.onclick = async () => {
  try {
    const snapshot = await (await api(`/api/runtime/sessions/${state.sessionId}/snapshot`)).json();
    const run = snapshot.runs.filter(run => !run.parentRunId).at(-1); if (!run) return;
    await api(`/api/sessions/${state.sessionId}/controls`, {method:'POST', body:JSON.stringify({
    runId:run.id, kind:button.dataset.control, idempotencyKey:crypto.randomUUID(), expectedVersion:run.stateVersion})}); }
  catch (error) { showError(error); }
};
$('credentials').onsubmit = async event => {
  event.preventDefault(); token = $('token').value;
  try {
    config = await (await api('/api/config')).json(); $('agent').textContent = config.name;
    $('mode-label').textContent = config.thinking ? '深度思考' : 'Agent';
    $('login').close(); $('token').value = '';
    const sessions = await list(), selected = new URL(location.href).searchParams.get('session');
    if (selected && sessions.some(row => row.id === selected)) await select(selected);
    else if (sessions.length) await select(sessions[0].id); else await $('new').onclick();
  } catch (error) { $('login-error').textContent = error.message; }
};
$('login').showModal();
