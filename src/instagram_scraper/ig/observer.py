"""In-page response observer: the comment responses Instagram's own UI
receives, read back through documented ``run_js`` instead of HAR dumps.

Without ``since``, ``dump_har_logs`` exports cumulative traffic; getbro now
supports ``since`` and ``resource_types``. The collector filters resource
types but does not advance ``since``: a request started before the cutoff can
finish later, so advancing it could lose the response. The observer keeps
normal comment reads independent of growing HAR exports. It wraps ``fetch`` and
``XMLHttpRequest`` inside the page and passively copies the bodies of comment
read operations into a bounded in-page queue. Nothing about the original
request or response is changed: the page receives its own response object,
the copy is taken from ``Response.clone()`` / ``responseText``.

Queue protocol
--------------
Every captured response gets a sequence number. Reading returns events after
the last acknowledged number; the collector saves the decoded pages to
SQLite, applies them, and only then acknowledges. A read repeated before its
acknowledgement returns the same events again -- safe, because it never
repeats an Instagram request, and duplicates are rejected by the response
fingerprint store on the way in.

The queue is capped at 32 MiB and a single read at 2 MiB. An event that does
not fit the cap is *not* stored and its sequence number is reported in
``droppedSeqs``: a loss is explicit, and never evicts unread pages. An event
larger than one read is returned in parts and reassembled here.

Only comment read operations and account-limit rejections are kept. Cookies,
authorization headers and Relay's runtime form never leave the page: the
request text is the urlencoded GraphQL envelope (``doc_id``, friendly name,
``variables``) or nothing, and it is used exactly as HAR ``postData`` was.
"""
from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone
from typing import Any

from ..bro import commands as cmd

log = logging.getLogger(__name__)

#: bytes of unread events the page keeps before it starts reporting drops
QUEUE_CAP = 32 * 1024 * 1024
#: bytes one read may return; larger events are read in parts
READ_CAP = 2 * 1024 * 1024
#: schema version of the in-page object; a different one is reinstalled
VERSION = 1

_INSTALL_JS = r"""JSON.stringify((function(){
  var V=__VERSION__, CAP=__CAP__;
  var prev=window.__igsObs;
  if(prev && prev.v===V && typeof prev.patched==='function')
    return {ok:true,installed:false,gen:prev.gen,patched:prev.patched()};
  var gen=Math.random().toString(36).slice(2)+Date.now().toString(36);
  var st={v:V,gen:gen,events:[],next:1,acked:0,bytes:0,dropped:0,droppedSeqs:[],errors:0};
  var HOSTS=/^(www\.)?instagram\.com$|^i\.instagram\.com$/;
  function classify(u){
    var x; try{ x=new URL(u,location.href); }catch(e){ return null; }
    if(!HOSTS.test(x.hostname)) return null;
    var p=x.pathname.replace(/\/$/,'');
    if(p==='/graphql/query'||p==='/api/graphql') return 'graphql';
    if(/^\/api\/v1\/media\/\d+\/comments(\/\d+\/child_comments)?$/.test(p)) return 'rest';
    if(p.indexOf('/api/v1/')===0) return 'api';
    return null;
  }
  function bodyText(b){
    try{
      if(b==null) return '';
      if(typeof b==='string') return b;
      if(typeof URLSearchParams!=='undefined' && b instanceof URLSearchParams) return b.toString();
      if(typeof FormData!=='undefined' && b instanceof FormData){
        var parts=[]; b.forEach(function(v,k){ if(typeof v==='string') parts.push(encodeURIComponent(k)+'='+encodeURIComponent(v)); });
        return parts.join('&');
      }
    }catch(e){ st.errors++; }
    return '';
  }
  function push(kind,method,url,reqBody,status,text){
    var cls=classify(url); if(!cls) return;
    text=text||''; reqBody=reqBody||'';
    var head=text.slice(0,4000).toLowerCase();
    var rejection=(status===401||status===403||status===429)||
      /require_login|challenge_required|checkpoint_required|login_required|please wait a few minutes/.test(head);
    var wanted=cls==='rest'||(cls==='graphql'&&/comment/i.test(reqBody.slice(0,20000)));
    if(!wanted && !rejection) return;
    var ev={seq:st.next++,kind:kind,method:method,url:url,status:status,t:Date.now(),rejection:rejection};
    if(wanted){ ev.body=text; ev.request=safeRequest(reqBody); }
    else { ev.body=text.slice(0,2000); ev.request=''; }
    ev.size=new TextEncoder().encode(JSON.stringify(ev)).length;
    if(st.bytes+ev.size>CAP){ st.dropped++; if(st.droppedSeqs.length<64) st.droppedSeqs.push(ev.seq); return; }
    st.events.push(ev); st.bytes+=ev.size;
  }
  function safeRequest(body){
    if(!body) return '';
    try{
      var p=body.trim().charAt(0)==='{'?JSON.parse(body):Object.fromEntries(new URLSearchParams(body));
      var v=typeof p.variables==='string'?JSON.parse(p.variables):p.variables;
      function clean(x){
        if(!x||typeof x!=='object'||Array.isArray(x)) return x;
        var out={}; Object.keys(x).forEach(function(k){
          var allowed=/^(media_id|mediaId|shortcode|comment_id|parent_comment_id|first|after|last|before|sort_order|data|can_support_threading|permalink_enabled|is_chronological)$/.test(k)||/^__relay_internal__pv__[A-Za-z0-9_]+relayprovider$/.test(k);
          out[k]=allowed?clean(x[k]):null;
        }); return out;
      }
      return new URLSearchParams({doc_id:p.doc_id||'',fb_api_req_friendly_name:p.fb_api_req_friendly_name||'',variables:JSON.stringify(clean(v)||{})}).toString();
    }catch(e){return '';}
  }
  var of=window.fetch;
  function wf(input,init){
    var p=of.apply(this,arguments);
    try{
      var url=(typeof input==='string')?input:((input&&input.url)||'');
      var method=String((init&&init.method)||(input&&input.method)||'GET').toUpperCase();
      var req=bodyText(init&&init.body);
      var requestBody=(!req&&input&&typeof input.clone==='function')?input.clone().text():Promise.resolve(req);
      p.then(function(res){
        try{
          var u=res.url||url; if(!classify(u)) return;
          Promise.all([res.clone().text(),requestBody]).then(function(pair){ push('fetch',method,u,pair[1],res.status,pair[0]); },
                                  function(){ st.errors++; });
        }catch(e){ st.errors++; }
      },function(){});
    }catch(e){ st.errors++; }
    return p;
  }
  window.fetch=wf; st.wf=wf;
  var XO=XMLHttpRequest.prototype.open, XS=XMLHttpRequest.prototype.send;
  var xo=function(m,u){ try{ this.__igs={m:String(m||'GET').toUpperCase(),u:String(u)}; }catch(e){} return XO.apply(this,arguments); };
  var xs=function(b){
    var x=this;
    try{
      if(x.__igs && classify(x.__igs.u)){
        x.__igs.b=bodyText(b);
        x.addEventListener('loadend',function(){
          try{
            var t=x.responseType==='json'?JSON.stringify(x.response):((x.responseType===''||x.responseType==='text')?x.responseText:'');
            push('xhr',x.__igs.m,x.responseURL||x.__igs.u,x.__igs.b,x.status,t);
          }catch(e){ st.errors++; }
        });
      }
    }catch(e){ st.errors++; }
    return XS.apply(this,arguments);
  };
  XMLHttpRequest.prototype.open=xo; XMLHttpRequest.prototype.send=xs;
  st.xo=xo; st.xs=xs;
  st.patched=function(){ return window.fetch===wf && XMLHttpRequest.prototype.open===xo && XMLHttpRequest.prototype.send===xs; };
  window.__igsObs=st;
  return {ok:true,installed:true,gen:gen,patched:true};
})())"""

_READ_JS = r"""JSON.stringify((function(after,maxBytes,part){
  var st=window.__igsObs;
  if(!st||typeof st.patched!=='function') return {ok:false,reason:'not_installed'};
  var out=[], bytes=0, evs=st.events;
  for(var i=0;i<evs.length;i++){
    var e=evs[i]; if(e.seq<=after) continue;
    if(e.size>maxBytes){
      if(!out.length){
        // JSON escaping can take six bytes per UTF-16 code unit. Reserve
        // room for the envelope and request even on a Unicode-heavy page.
        var slice=Math.max(1,Math.floor((maxBytes-1024-e.request.length*6)/6)), p=part||0, parts=Math.max(1,Math.ceil(e.body.length/slice));
        out.push({seq:e.seq,kind:e.kind,method:e.method,url:e.url,status:e.status,t:e.t,
                  rejection:!!e.rejection,request:p===0?e.request:'',part:p,parts:parts,
                  body:e.body.slice(p*slice,(p+1)*slice)});
      }
      break;
    }
    if(bytes+e.size>maxBytes) break;
    out.push({seq:e.seq,kind:e.kind,method:e.method,url:e.url,status:e.status,t:e.t,
              rejection:!!e.rejection,request:e.request,body:e.body});
    bytes+=e.size;
  }
  return {ok:true,gen:st.gen,patched:st.patched(),next:st.next,acked:st.acked,pending:evs.length,
          bytes:st.bytes,dropped:st.dropped,droppedSeqs:st.droppedSeqs.slice(),errors:st.errors,events:out};
})(__AFTER__,__MAX__,__PART__))"""

_ACK_JS = r"""JSON.stringify((function(seq){
  var st=window.__igsObs;
  if(!st||typeof st.patched!=='function') return {ok:false,reason:'not_installed'};
  if(__GEN__!==null && st.gen!==__GEN__) return {ok:false,reason:'generation_changed',gen:st.gen};
  var kept=[], bytes=0;
  for(var i=0;i<st.events.length;i++){ var e=st.events[i]; if(e.seq>seq){ kept.push(e); bytes+=e.size; } }
  st.events=kept; st.bytes=bytes; if(seq>st.acked) st.acked=seq;
  return {ok:true,gen:st.gen,acked:st.acked,pending:kept.length,bytes:bytes,dropped:st.dropped,patched:st.patched()};
})(__SEQ__))"""


def install_script() -> str:
    return _INSTALL_JS.replace("__VERSION__", str(VERSION)).replace("__CAP__", str(QUEUE_CAP))


def read_script(after: int, max_bytes: int = READ_CAP, part: int = 0) -> str:
    return (_READ_JS.replace("__AFTER__", str(int(after)))
            .replace("__MAX__", str(int(max_bytes))).replace("__PART__", str(int(part))))


def ack_script(seq: int, gen=None) -> str:
    return _ACK_JS.replace("__SEQ__", str(int(seq))).replace("__GEN__", json.dumps(gen))


def entry_from_event(event: dict[str, Any]) -> dict[str, Any]:
    """Shape one observer event like a HAR entry so the existing parser,
    rejection checks and template observation run unchanged."""
    started = event.get("t")
    try:
        when = datetime.fromtimestamp(float(started) / 1000.0, tz=timezone.utc)
    except (TypeError, ValueError, OverflowError):
        when = datetime.now(tz=timezone.utc)
    request_text = str(event.get("request") or "")
    mime = "application/x-www-form-urlencoded"
    stripped = request_text.lstrip()
    if stripped.startswith("{") or stripped.startswith("["):
        mime = "application/json"
    return {
        "startedDateTime": when.isoformat().replace("+00:00", "Z"),
        "request": {"method": str(event.get("method") or "GET"),
                    "url": str(event.get("url") or ""),
                    "postData": {"mimeType": mime, "text": request_text}},
        "response": {"status": event.get("status"),
                     "content": {"mimeType": "application/json",
                                 "text": str(event.get("body") or "")}},
        "_observer": {"seq": event.get("seq"), "kind": event.get("kind"),
                      "rejection": bool(event.get("rejection"))},
    }


def _decode(value: Any) -> dict[str, Any]:
    raw = (value or {}).get("result") if isinstance(value, dict) else None
    try:
        decoded = json.loads(raw) if isinstance(raw, str) else None
    except ValueError:
        decoded = None
    return decoded if isinstance(decoded, dict) else {"ok": False, "reason": "invalid_result"}


class ResponseObserver:
    """One page's observer: install, read (with acknowledgement), health."""

    def __init__(self, *, read_cap: int = READ_CAP) -> None:
        self.read_cap = int(read_cap)
        self.gen: str | None = None
        self.available = False
        self.last_read_seq = 0
        self.acked_seq = 0
        self.pending_ack: int | None = None
        self.installs = 0
        self.document_changes = 0
        self.dropped = 0
        self.errors = 0
        self.reads = 0
        self.events_read = 0
        self.bytes_read = 0
        self.read_seconds = 0.0
        self.parts_read = 0
        self.unavailable_reason: str | None = None
        self.delivered_ids: set[str] = set()

    # ------------------------------------------------------------- install --

    def install(self, session, *, on_event=None) -> dict[str, Any]:
        """Install (or confirm) the in-page observer; a new document gets a new
        generation token, and events before that point are not observed."""
        if not hasattr(session, "run"):
            self.available = False
            self.unavailable_reason = "unsupported_session"
            return {"ok": False, "reason": self.unavailable_reason}
        values = session.run([cmd.run_js(install_script(), out_type="str")],
                             retries=0, on_event=on_event)
        result = _decode(values[0] if values else None)
        if not result.get("ok") or not result.get("gen"):
            self.available = False
            self.unavailable_reason = str(result.get("reason") or "install_failed")
            return result
        if self.gen is not None and result["gen"] != self.gen:
            self.document_changes += 1
        if result.get("installed"):
            self.installs += 1
            # A fresh document starts its own numbering.
            self.last_read_seq = 0
            self.acked_seq = 0
            self.pending_ack = None
        self.gen = result["gen"]
        self.available = bool(result.get("patched", True))
        self.unavailable_reason = None if self.available else "unpatched"
        return result

    # ---------------------------------------------------------------- read --

    def read(self, session, *, on_event=None, event_factory=None) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        """Return (HAR-shaped entries, status). Entries are not acknowledged
        until :meth:`ack` -- call it after they are durably applied."""
        if not hasattr(session, "run"):
            return [], {"ok": False, "reason": "unsupported_session"}
        started = time.monotonic()
        entries: list[dict[str, Any]] = []
        after = self.acked_seq
        status: dict[str, Any] = {}
        chunks: dict[int, dict[str, Any]] = {}
        part = 0
        for _ in range(256):  # enough for the 32 MiB queue at the default read cap
            values = session.run([cmd.run_js(read_script(after, self.read_cap, part), out_type="str")],
                                 retries=0, on_event=event_factory() if event_factory else on_event)
            self.reads += 1
            status = _decode(values[0] if values else None)
            if not status.get("ok"):
                self.available = False
                self.unavailable_reason = str(status.get("reason") or "read_failed")
                break
            if self.gen is not None and status.get("gen") != self.gen:
                # The document changed underneath: whatever the old document
                # queued is gone with it. Report and let the caller reinstall.
                self.document_changes += 1
                self.gen = status.get("gen")
                self.last_read_seq = 0
                self.acked_seq = 0
                after = 0
                status["documentChanged"] = True
                break
            if not status.get("patched", True):
                self.available = False
                self.unavailable_reason = "unpatched"
            self.dropped = int(status.get("dropped") or 0)
            self.errors = int(status.get("errors") or 0)
            events = status.get("events") or []
            if not events:
                break
            whole = [e for e in events if "parts" not in e]
            for event in whole:
                entries.append(entry_from_event(event))
                self.events_read += 1
                self.bytes_read += len(str(event.get("body") or "")) + len(str(event.get("request") or ""))
                self.last_read_seq = max(self.last_read_seq, int(event.get("seq") or 0))
            partial = next((e for e in events if "parts" in e), None)
            if partial is None:
                after = self.last_read_seq
                if after >= int(status.get("next") or after + 1) - 1:
                    break
                continue
            # One event larger than a read: collect its parts in order.
            self.parts_read += 1
            seq = int(partial.get("seq") or 0)
            slot = chunks.setdefault(seq, {**{k: v for k, v in partial.items() if k not in ("body", "part", "parts")}, "body": ""})
            slot["body"] += str(partial.get("body") or "")
            if partial.get("request"):
                slot["request"] = partial["request"]
            if int(partial.get("part") or 0) + 1 < int(partial.get("parts") or 1):
                part = int(partial.get("part") or 0) + 1
                continue
            entries.append(entry_from_event(slot))
            self.events_read += 1
            self.bytes_read += len(slot["body"]) + len(str(slot.get("request") or ""))
            self.last_read_seq = max(self.last_read_seq, seq)
            chunks.pop(seq, None)
            after = seq
            part = 0
            # keep reading: there may be more events after the large one
        self.read_seconds += time.monotonic() - started
        if chunks:
            self.available = False
            self.unavailable_reason = "incomplete_response_parts"
        if entries:
            self.pending_ack = self.last_read_seq
        status = {k: v for k, v in status.items() if k != "events"}
        status["entries"] = len(entries)
        return entries, status

    def ack(self, session, *, on_event=None) -> dict[str, Any] | None:
        """Drop everything up to the last read event -- only after it is saved."""
        if self.pending_ack is None or not hasattr(session, "run"):
            return None
        seq = self.pending_ack
        values = session.run([cmd.run_js(ack_script(seq, self.gen), out_type="str")],
                             retries=0, on_event=on_event)
        result = _decode(values[0] if values else None)
        if result.get("ok"):
            self.acked_seq = max(self.acked_seq, int(result.get("acked") or seq))
            self.pending_ack = None
        else:
            self.available = False
            self.unavailable_reason = str(result.get("reason") or "ack_failed")
        return result

    def summary(self) -> dict[str, Any]:
        return {"available": self.available, "reason": self.unavailable_reason,
                "installs": self.installs, "documentChanges": self.document_changes,
                "reads": self.reads, "eventsRead": self.events_read,
                "bytesRead": self.bytes_read, "readSeconds": round(self.read_seconds, 3),
                "partsRead": self.parts_read, "dropped": self.dropped,
                "pageErrors": self.errors, "ackedSeq": self.acked_seq,
                "deliveredComments": len(self.delivered_ids)}
