// The composition-confirming key must be released before Enter can submit.
export function composerKeyboard(submit) {
  let composing = false, confirmed = false;
  return {
    compositionstart() { composing = true; confirmed = false; },
    compositionend() { composing = false; confirmed = true; },
    keyup() { if (!composing) confirmed = false; },
    keydown(event) {
      if (event.key !== 'Enter') { if (!composing) confirmed = false; return; }
      if (composing || confirmed || event.isComposing || event.keyCode === 229) return;
      if (event.shiftKey || event.ctrlKey || event.altKey || event.metaKey) return;
      event.preventDefault();
      if (!event.repeat) submit();
    },
  };
}
