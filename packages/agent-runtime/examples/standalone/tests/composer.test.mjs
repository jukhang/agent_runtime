import {test} from 'node:test';
import assert from 'node:assert/strict';
import {composerKeyboard} from '../src/centn_example/ui/composer.mjs';

function setup() {
  let sent = 0, prevented = 0;
  const keyboard = composerKeyboard(() => sent++);
  const enter = extra => keyboard.keydown({key:'Enter',preventDefault(){prevented++;},...extra});
  return {keyboard,enter,counts:()=>[sent,prevented]};
}
test('plain Enter sends once and held Enter cannot resubmit',()=>{
  const h=setup(); h.enter(); h.enter({repeat:true});
  assert.deepEqual(h.counts(),[1,2]);
});
test('Shift Enter and modified Enter leave native editing untouched',()=>{
  const h=setup();
  for (const key of ['shiftKey','ctrlKey','altKey','metaKey']) h.enter({[key]:true});
  assert.deepEqual(h.counts(),[0,0]);
});
test('Chinese or English IME confirmation does not send until the next Enter',()=>{
  for (const text of ['中文','English']) {
    const h=setup(); h.keyboard.compositionstart({data:text});
    h.enter({isComposing:true}); h.keyboard.compositionend({data:text});
    h.enter(); assert.deepEqual(h.counts(),[0,0]);
    h.keyboard.keyup({key:'Enter'}); h.enter();
    assert.deepEqual(h.counts(),[1,1]);
  }
});
test('IME compatibility flags block submission even without compositionstart',()=>{
  const h=setup(); h.enter({isComposing:true}); h.enter({keyCode:229});
  assert.deepEqual(h.counts(),[0,0]);
});
test('typing after mouse-confirmed composition releases the confirmation guard',()=>{
  const h=setup(); h.keyboard.compositionend(); h.keyboard.keydown({key:'a'});
  h.enter(); assert.deepEqual(h.counts(),[1,1]);
});
