"""Adapters for comment read queries observed in this browser, never guessed IDs."""
from __future__ import annotations
import copy
import json
import re
from urllib.parse import parse_qs, urlsplit
from ..bro import commands as cmd
from .network import _variables, _scope_matches
from ..errors import InstagramError

_ALLOWED = {"media_id", "mediaId", "shortcode", "comment_id", "parent_comment_id",
            "first", "after", "last", "before", "sort_order", "data",
            "can_support_threading", "permalink_enabled"}
_RELAY_PROVIDER = re.compile(r"__relay_internal__pv__[A-Za-z0-9_]+relayprovider\Z")
UI_REPLY_QUERIES = {"PolarisPostChildCommentsQuery", "PolarisPostCommentsChildrenPaginationtQuery"}
_ENDPOINTS = {"/graphql/query", "/api/graphql"}
LOCATOR_VERSION = 2

_ADVANCE_JS = r"""JSON.stringify((function(){
  function id(a){var m=(a.getAttribute('href')||'').match(/\/c\/(\d+)(?:\/|$)/);return m&&m[1];}
  function visibleIds(region){
    var rr=typeof region.getBoundingClientRect==='function'?region.getBoundingClientRect():null;
    return [].slice.call(region.querySelectorAll('a[href*="/c/"]')).filter(function(a){
      if(!id(a)||!a.getClientRects().length) return false;
      if(!rr||typeof a.getBoundingClientRect!=='function') return true;
      var ar=a.getBoundingClientRect(); return ar.bottom>rr.top&&ar.top<rr.bottom;
    }).map(id);
  }
  var links=[].slice.call(document.querySelectorAll('a[href*="/c/"]'))
    .filter(function(a){return /\/c\/\d+\/?/.test(a.getAttribute('href')||'');});
  if(!links.length) return {action:'no_comment_links',path:location.pathname};
  var candidates=[];
  links.forEach(function(a){
    var n=a.parentElement;
    while(n && n!==document.body){
      var s=getComputedStyle(n), overflow=n.scrollHeight-n.clientHeight;
      if(overflow>100 && n.clientHeight>120 && /(auto|scroll)/.test(s.overflowY)){
        candidates.push(n); break;
      }
      n=n.parentElement;
    }
  });
  if(!candidates.length) return {action:'no_verified_scroller',links:links.length,path:location.pathname};
  var best=candidates.sort(function(a,b){
    return b.querySelectorAll('a[href*="/c/"]').length-a.querySelectorAll('a[href*="/c/"]').length;
  })[0];
  var before=best.scrollTop, maximum=Math.max(0,best.scrollHeight-best.clientHeight);
  var beforeIds=visibleIds(best);
  // Instagram serves the next comment page on two triggers only: the
  // "Load more comments" control when it renders one, and the list being
  // scrolled to its very bottom. The ceiling probe reached 1106 comments
  // doing exactly this each round; a 70% viewport step reached neither and
  // stalled at 15-38 parents over four live sessions.
  var loadMore='absent';
  var controls=[].slice.call(best.querySelectorAll('button,[role="button"],svg[aria-label]'));
  for(var i=0;i<controls.length;i++){
    var c=controls[i];
    var own=(c.getAttribute('aria-label')||'').toLowerCase();
    var inner=(typeof c.querySelector==='function')?c.querySelector('svg[aria-label]'):null;
    var label=own||(inner?(inner.getAttribute('aria-label')||'').toLowerCase():'');
    if(!(label.indexOf('load more comment')>=0||label.indexOf('more comments')>=0)) continue;
    if(!c.getClientRects().length) continue;
    var target=(c.tagName.toLowerCase()==='svg')?(c.closest('button')||c.parentElement||c):c;
    if(target.disabled||target.getAttribute('aria-disabled')==='true') continue;
    target.click(); loadMore='clicked'; break;
  }
  best.scrollTop=maximum;
  best.dispatchEvent(new Event('scroll',{bubbles:true}));
  var afterIds=visibleIds(best);
  return {action:best.scrollTop>before?'scrolled_comment_region':'comment_region_end',
          path:location.pathname,links:links.length,before:before,immediate:best.scrollTop,after:best.scrollTop,
          step:best.scrollTop-before,loadMore:loadMore,atEnd:best.scrollTop>=maximum-2,
          scrollHeight:best.scrollHeight,clientHeight:best.clientHeight,
          beforeFirstVisibleId:beforeIds[0]||null,beforeLastVisibleId:beforeIds[beforeIds.length-1]||null,
          firstVisibleId:afterIds[0]||null,lastVisibleId:afterIds[afterIds.length-1]||null};
})())"""

_SCROLL_STATE_JS = r"""JSON.stringify((function(){
  function id(a){var m=(a.getAttribute('href')||'').match(/\/c\/(\d+)(?:\/|$)/);return m&&m[1];}
  var links=[].slice.call(document.querySelectorAll('a[href*="/c/"]')).filter(function(a){return id(a);});
  if(!links.length) return {action:'no_comment_links',path:location.pathname};
  var candidates=[];
  links.forEach(function(a){var n=a.parentElement;while(n&&n!==document.body){
    var s=getComputedStyle(n), overflow=n.scrollHeight-n.clientHeight;
    if(overflow>100&&n.clientHeight>120&&/(auto|scroll)/.test(s.overflowY)){candidates.push(n);break;} n=n.parentElement;
  }});
  if(!candidates.length) return {action:'no_verified_scroller',links:links.length,path:location.pathname};
  var best=candidates.sort(function(a,b){return b.querySelectorAll('a[href*="/c/"]').length-a.querySelectorAll('a[href*="/c/"]').length;})[0];
  var rr=typeof best.getBoundingClientRect==='function'?best.getBoundingClientRect():null;
  var ids=[].slice.call(best.querySelectorAll('a[href*="/c/"]')).filter(function(a){
    if(!id(a)||!a.getClientRects().length) return false;
    if(!rr||typeof a.getBoundingClientRect!=='function') return true;
    var ar=a.getBoundingClientRect(); return ar.bottom>rr.top&&ar.top<rr.bottom;
  }).map(id);
  var maximum=Math.max(0,best.scrollHeight-best.clientHeight);
  return {action:'observed_comment_region',path:location.pathname,links:links.length,
          settledTop:best.scrollTop,settledScrollHeight:best.scrollHeight,
          settledClientHeight:best.clientHeight,settledAtEnd:best.scrollTop>=maximum-2,
          settledFirstVisibleId:ids[0]||null,settledLastVisibleId:ids[ids.length-1]||null};
})())"""


def _safe_variables(value):
    if not isinstance(value, dict) or any(
            k not in _ALLOWED | {"is_chronological"} and not _RELAY_PROVIDER.fullmatch(k)
            for k in value):
        return False
    # Relay adds feature-provider variables without changing the query. Accept
    # only its namespaced provider shape and only booleans; arbitrary variables
    # and provider values remain ineligible for replay.
    if any(_RELAY_PROVIDER.fullmatch(k) and type(v) is not bool
           for k, v in value.items()):
        return False
    # The UI's first child read (PolarisPostChildCommentsQuery, "View all N
    # replies") sends is_chronological: null; a live pilot on 14 September
    # lost every thread's first response to this check. Null is what the
    # page itself sent and replays unchanged; anything else stays rejected.
    if "is_chronological" in value and value["is_chronological"] is not None and (
            type(value["is_chronological"]) is not bool):
        return False
    return all(_safe_variables(v) if isinstance(v, dict) else
               isinstance(v, (str, int, float, bool, type(None))) for v in value.values())


def _diagnostic(diagnostics, reason, *, endpoint, name="", variables=None, doc=""):
    """Record bounded schema diagnostics without request values or identifiers."""
    if diagnostics is None:
        return
    diagnostics["decodedBodies"] = diagnostics.get("decodedBodies", 0) + 1
    if reason == "accepted":
        diagnostics["acceptedQueries"] = diagnostics.get("acceptedQueries", 0) + 1
        return
    reasons = diagnostics.setdefault("rejectionReasons", {})
    reasons[reason] = reasons.get(reason, 0) + 1
    samples = diagnostics.setdefault("rejectionSamples", [])
    variables = variables if isinstance(variables, dict) else {}
    unsafe = sorted(str(k) for k, v in variables.items()
                    if k not in _ALLOWED | {"is_chronological"} and
                    not _RELAY_PROVIDER.fullmatch(str(k)))
    sample = {
        "reason": reason,
        "endpoint": endpoint,
        "friendlyName": name[:120],
        "hasNumericDocId": bool(doc and str(doc).isdigit()),
        "variableKeys": sorted(str(k) for k in variables)[:40],
        "unsafeVariableKeys": unsafe[:20],
    }
    if len(samples) < 8:
        samples.append(sample)
    elif not any(row.get("reason") == reason for row in samples):
        # Preserve at least one example for every reason. A busy Instagram tab
        # emits many unrelated queries before the useful comment operation;
        # a simple first-eight cap otherwise hides the schema drift we need.
        for index in range(len(samples) - 1, -1, -1):
            existing = samples[index].get("reason")
            if sum(row.get("reason") == existing for row in samples) > 1:
                samples[index] = sample
                break


def _connections(value, path=()):
    if not isinstance(value, dict) or len(path) > 6:
        return
    for key, child in value.items():
        if not isinstance(child, dict):
            continue
        current = (*path, key)
        if "comment" in key.lower() and isinstance(child.get("edges"), list) and isinstance(child.get("page_info"), dict):
            yield current, child
        elif key != "edges":
            yield from _connections(child, current)


def observe(request, body, media_id, diagnostics=None):
    """Return only proven comment Query templates with an observed cursor slot."""
    url = urlsplit(request.get("url") or "")
    if url.hostname not in ("www.instagram.com", "instagram.com", "i.instagram.com") or request.get("method") != "POST" or url.path.rstrip("/") not in _ENDPOINTS:
        return []
    params = parse_qs((request.get("postData") or {}).get("text") or "")
    name = (params.get("fb_api_req_friendly_name") or [""])[0]
    doc = (params.get("doc_id") or [""])[0]
    if "mutation" in name.lower() or not re.fullmatch(r"[A-Za-z0-9_]*Comments?[A-Za-z0-9_]*Query", name, re.I) or not doc.isdigit():
        _diagnostic(diagnostics, "operation_identity", endpoint=url.path.rstrip("/"),
                    name=name, doc=doc)
        return []
    variables = _variables(request)
    if not _scope_matches(variables, str(media_id)):
        _diagnostic(diagnostics, "scope_mismatch", endpoint=url.path.rstrip("/"),
                    name=name, variables=variables, doc=doc)
        return []
    if not _safe_variables(variables):
        _diagnostic(diagnostics, "unsafe_variables", endpoint=url.path.rstrip("/"),
                    name=name, variables=variables, doc=doc)
        return []
    container = (variables if ("after" in variables or "before" in variables)
                 else variables.get("data", {}))
    if not isinstance(container, dict):
        container = {}
    forward = "after" in container and "first" in container
    # Child reads observed from the UI may paginate backwards (last/before);
    # they are as real as forward ones and must reach the direction branch
    # below instead of being rejected here for lacking after/first.
    backward = ("before" in container and "last" in container and
                not container.get("first") and container.get("last"))
    if not forward and not backward:
        _diagnostic(diagnostics, "cursor_shape", endpoint=url.path.rstrip("/"),
                    name=name, variables=variables, doc=doc)
        return []
    parent = (variables.get("comment_id") or variables.get("parent_comment_id")
              or container.get("comment_id") or container.get("parent_comment_id"))
    result = []
    for path, connection in _connections(body):
        is_child = any("threaded" in p or "child" in p for p in path)
        if bool(parent) != is_child:
            continue
        template = {"docId": doc, "friendlyName": name, "variables": copy.deepcopy(variables),
                    "endpoint": url.path.rstrip("/"),
                    "replayEndpoint": url.path.rstrip("/"),
                    # Used only until persistable() registers it in the live
                    # context. It must never enter SQLite or an output file.
                    "_runtimeForm": {key: values[-1] for key, values in params.items()},
                    "cursorInData": container is not variables, "path": list(path),
                    "observedCursor": container.get("after"),
                    "observedEndCursor": (connection.get("page_info") or {}).get("end_cursor"),
                    "observedHasNext": (connection.get("page_info") or {}).get("has_next_page"),
                    "mediaId": str(media_id), "parentId": str(parent) if parent else None,
                    "sort": container.get("sort_order") or variables.get("sort_order"),
                    "validation": "observed_first_page"}
        if backward:
            # A backward read: its cursor slot is ``before`` and its
            # continuation is described by has_previous_page/start_cursor.
            info = connection.get("page_info") or {}
            template.update(observedCursor=container.get("before"),
                            observedEndCursor=info.get("start_cursor"),
                            observedHasNext=info.get("has_previous_page"),
                            direction="backward")
        else:
            template["direction"] = "forward"
        result.append((template, connection))
    _diagnostic(diagnostics, "accepted" if result else "connection_shape",
                endpoint=url.path.rstrip("/"), name=name, variables=variables, doc=doc)
    return result


def fetch(ctx, template, cursor):
    variables = copy.deepcopy(template["variables"])
    container = variables["data"] if template["cursorInData"] else variables
    container["before" if template.get("direction") == "backward" else "after"] = cursor
    replies = bool(template.get("parentId"))
    runtime_form = getattr(ctx, "native_forms", {}).get(runtime_key(template))
    endpoint = template.get("replayEndpoint") or template.get("endpoint") or "/graphql/query"
    if endpoint == "/api/graphql" and not runtime_form:
        raise InstagramError("Native GraphQL runtime envelope is unavailable; re-observe the UI query")
    body = ctx.graphql(
        template["docId"], variables, friendly_name=template["friendlyName"],
        endpoint=endpoint, observed_form=runtime_form,
        pending_source="native",
        pending_transform=lambda decoded: normalize(decoded, template, replies=replies),
        what="observed comment connection")
    return connection(body, template)


def connection(body, template):
    if not isinstance(body, dict) or body.get("errors"):
        raise InstagramError("Native comment query changed or rejected its cursor")
    value = body
    for key in template["path"]:
        value = value.get(key) if isinstance(value, dict) else None
    if not isinstance(value, dict) or not isinstance(value.get("edges"), list):
        raise InstagramError("Observed native comment connection is absent")
    return value


def normalize(body, template, *, replies=False):
    """Turn a completed Relay response into the durable page contract."""
    return page(connection(body, template), replies=replies,
                direction=template.get("direction") or "forward")


def runtime_key(template):
    return "|".join(str(template.get(key) or "") for key in (
        "endpoint", "docId", "friendlyName"))


def persistable(ctx, template):
    """Register the current VM's form envelope and return a secret-free copy."""
    clean = copy.deepcopy(template)
    runtime_form = clean.pop("_runtimeForm", None)
    if runtime_form:
        forms = getattr(ctx, "native_forms", None)
        if forms is None:
            forms = {}
            setattr(ctx, "native_forms", forms)
        forms[runtime_key(clean)] = runtime_form
    return clean


def runtime_available(ctx, template):
    return runtime_key(template) in getattr(ctx, "native_forms", {})


# ---------------------------------------------------------------------------
# One locator for every UI script: the same region, the same notion of a
# parent row and the same ownership rule for reply controls. Listing threads
# and pressing one control no longer disagree about who owns a button.
#
# Ownership is structural and ordered, not container-based: walking the
# verified comment region in document order, a reply control belongs to the
# last *confirmed* parent row before it -- a parent is confirmed by the
# network (its id is in the caller's list), a permalink the caller does not
# know is a reply until proven otherwise and never makes a parent. A shared
# ancestor holding a neighbouring parent is therefore not ambiguity; the only
# genuine ambiguity is a control that precedes every confirmed parent row.
_UI_LIB_JS = r"""
  function id(a){var m=(a.getAttribute('href')||'').match(/\/c\/(\d+)(?:\/|$)/);return m&&m[1];}
  var CONTROL=/^(?:View all \d+ repl(?:y|ies)|View replies \(\d+\)|(?:Show|View|Load) more replies)$/i;
  function allLinks(){
    return [].slice.call(document.querySelectorAll('a[href*="/c/"]')).filter(function(a){return !!id(a);});
  }
  function findRegion(links){
    var candidates=[];
    links.forEach(function(a){
      var n=a.parentElement;
      while(n && n!==document.body){
        var st=getComputedStyle(n), overflow=n.scrollHeight-n.clientHeight;
        if(overflow>100 && n.clientHeight>120 && /(auto|scroll)/.test(st.overflowY)){candidates.push(n);break;}
        n=n.parentElement;
      }
    });
    if(!candidates.length) return null;
    return candidates.sort(function(a,b){
      return b.querySelectorAll('a[href*="/c/"]').length-a.querySelectorAll('a[href*="/c/"]').length;
    })[0];
  }
  function rect(el){ return (el && typeof el.getBoundingClientRect==='function')?el.getBoundingClientRect():null; }
  function inView(el,region){
    if(!el.getClientRects().length) return false;
    var r=rect(region), x=rect(el);
    if(!r||!x) return true;
    return x.bottom>r.top && x.top<r.bottom && x.right>r.left && x.left<r.right;
  }
  function usable(b){ return !!b.getClientRects().length && b.getAttribute('aria-disabled')!=='true' && !b.disabled; }
  function depthOf(el,region){ var d=0, n=el.parentElement; while(n && n!==region){ d++; n=n.parentElement; } return d; }
  function walk(region,known){
    var order=[].slice.call(region.querySelectorAll('*')), items=[], seen=[];
    // A parent row is confirmed by the network (its id is known) or by the
    // list's own structure: the comment list renders parents at one depth
    // and every reply deeper, inside its parent's row. The first screens of
    // a post come from the page's embedded data, which no network response
    // carries, so structure is what confirms them.
    var minDepth=Infinity, depths={};
    order.forEach(function(el,index){
      if(String(el.tagName||'').toUpperCase()!=='A' || !id(el)) return;
      var d=depthOf(el,region); depths[id(el)]=Math.min(d, depths[id(el)]===undefined?d:depths[id(el)]);
      if(d<minDepth) minDepth=d;
    });
    order.forEach(function(el,index){
      var tag=String(el.tagName||'').toUpperCase();
      if(tag==='A'){
        var v=id(el); if(!v) return;
        var structural=depths[v]===minDepth;
        items.push({type:(known[v]||structural)?'parent':'link',id:v,el:el,
                    structural:structural&&!known[v],position:index});
        return;
      }
      if(tag==='BUTTON'||tag==='SPAN'||el.getAttribute('role')==='button'){
        var t=(el.innerText||'').trim(); if(!CONTROL.test(t)) return;
        var target=(tag==='SPAN')?(el.closest('button,[role="button"]')||el):el;
        if(seen.indexOf(target)>=0) return; seen.push(target);
        var m=t.match(/(?:View all\s+(\d+)\s+repl(?:y|ies)|View replies\s*\((\d+)\))/i);
        items.push({type:'control',el:target,text:t,
                    advertisedCount:m?parseInt(m[1]||m[2],10):null,position:index});
      }
    });
    return items;
  }
  function controls(region,known){
    var items=walk(region,known), parents=items.filter(function(it){return it.type==='parent';});
    var before=0, out=[];
    function contains(ancestor,child){
      var n=child; while(n){if(n===ancestor)return true;if(n===region)return false;n=n.parentElement;}
      return false;
    }
    items.forEach(function(it){
      if(it.type!=='control') return;
      var owner=null, ambiguous=false, container=null, n=it.el.parentElement;
      // A valid control and its parent must share a local row container which
      // contains exactly one confirmed parent before the control.  The old
      // global "last parent before button" rule misattributed about a third
      // of live clicks when virtualized rows shared wrappers.
      while(n && n!==region){
        var local=parents.filter(function(p){return p.position<it.position&&contains(n,p.el);});
        if(local.length===1){owner=local[0].id;container=n;break;}
        if(local.length>1){ambiguous=true;break;}
        n=n.parentElement;
      }
      if(owner===null) before++;
      out.push({el:it.el,text:it.text,advertisedCount:it.advertisedCount,
                owner:owner,container:container,ambiguous:ambiguous});
    });
    return {controls:out,unowned:before};
  }
  function knownSet(list){ var k={}; (list||[]).forEach(function(v){k[String(v)]=true;}); return k; }
"""

_SCREEN_JS = r"""JSON.stringify((function(knownList){
""" + _UI_LIB_JS + r"""
  var known=knownSet(knownList);
  var links=allLinks();
  if(!links.length) return {action:'no_comment_links',parents:[],threads:[],path:location.pathname};
  var region=findRegion(links);
  if(!region) return {action:'no_verified_scroller',parents:[],threads:[],links:links.length,path:location.pathname};
  var items=walk(region,known);
  var present=[], visible=[], structural=[];
  items.forEach(function(it){
    if(it.type!=='parent') return;
    if(present.indexOf(it.id)<0) present.push(it.id);
    if(it.structural && structural.indexOf(it.id)<0) structural.push(it.id);
    if(inView(it.el,region) && visible.indexOf(it.id)<0) visible.push(it.id);
  });
  var found=controls(region,known), threads=[], seen=[], ambiguous=0;
  found.controls.forEach(function(c){
    if(!usable(c.el) || !inView(c.el,region)) return;
    if(c.owner===null){ ambiguous++; return; }
    if(seen.indexOf(c.owner)<0){ seen.push(c.owner); threads.push({parentId:c.owner,
      text:c.text,advertisedCount:c.advertisedCount,ownership:'local_unique',
      renderedLinks:c.container?c.container.querySelectorAll('a[href*="/c/"]').length:null}); }
  });
  var maximum=Math.max(0,region.scrollHeight-region.clientHeight);
  return {action:threads.length?'threads_visible':'no_thread_controls',
          parents:visible, presentParents:present, structuralParents:structural, threads:threads,
          ambiguousControls:ambiguous, unownedControls:found.unowned,
          links:links.length, path:location.pathname,
          firstVisibleId:visible[0]||null, lastVisibleId:visible[visible.length-1]||null,
          scrollTop:region.scrollTop,scrollHeight:region.scrollHeight,clientHeight:region.clientHeight,
          atEnd:region.scrollTop>=maximum-2,locatorVersion:2};
})(__KNOWN__))"""

_STEP_JS = r"""JSON.stringify((function(){
""" + _UI_LIB_JS + r"""
  var links=allLinks();
  if(!links.length) return {action:'no_comment_links',path:location.pathname};
  var region=findRegion(links);
  if(!region) return {action:'no_verified_scroller',links:links.length,path:location.pathname};
  var before=region.scrollTop, maximum=Math.max(0,region.scrollHeight-region.clientHeight);
  var step=Math.max(1,Math.floor(region.clientHeight*0.70));
  region.scrollTop=Math.min(maximum,before+step);
  region.dispatchEvent(new Event('scroll',{bubbles:true}));
  return {action:region.scrollTop>before?'scanned_forward':'scan_region_end',
          path:location.pathname,links:links.length,before:before,immediate:region.scrollTop,after:region.scrollTop,
          step:region.scrollTop-before,atEnd:region.scrollTop>=maximum-2,
          scrollHeight:region.scrollHeight,clientHeight:region.clientHeight};
})())"""

_NEXT_PARENT_JS = r"""JSON.stringify((function(parent){
""" + _UI_LIB_JS + r"""
  var links=allLinks(), region=findRegion(links);
  if(!region) return {action:links.length?'no_verified_scroller':'no_comment_links',path:location.pathname};
  var el=links.find(function(a){return id(a)===parent;});
  if(!el) return {action:'next_parent_missing',parentId:parent,path:location.pathname};
  var r=rect(region), x=rect(el), before=region.scrollTop;
  var absolute=(r&&x)?region.scrollTop+(x.top-r.top):(typeof el.offsetTop==='number'?el.offsetTop:before);
  var maximum=Math.max(0,region.scrollHeight-region.clientHeight);
  var target=Math.max(0,Math.min(maximum,Math.floor(absolute-region.clientHeight*0.15)));
  if(target<=before+2) return {action:'next_parent_not_forward',parentId:parent,
    path:location.pathname,before:before,after:before,scrollHeight:region.scrollHeight,
    clientHeight:region.clientHeight,atEnd:before>=maximum-2};
  region.scrollTop=target;
  region.dispatchEvent(new Event('scroll',{bubbles:true}));
  return {action:region.scrollTop!==before?'advanced_to_parent':'parent_already_visible',
          parentId:parent,path:location.pathname,before:before,immediate:region.scrollTop,
          after:region.scrollTop,step:region.scrollTop-before,atEnd:region.scrollTop>=maximum-2,
          scrollHeight:region.scrollHeight,clientHeight:region.clientHeight};
})(__PARENT__))"""

_ANCHOR_JS = r"""JSON.stringify((function(anchor,fallbacks){
""" + _UI_LIB_JS + r"""
  var links=allLinks();
  if(!links.length) return {action:'no_comment_links',path:location.pathname};
  var region=findRegion(links);
  if(!region) return {action:'no_verified_scroller',links:links.length,path:location.pathname};
  var byId={};
  [].slice.call(region.querySelectorAll('a[href*="/c/"]')).forEach(function(a){var v=id(a); if(v && !byId[v]) byId[v]=a;});
  var used=null, el=null;
  if(byId[anchor]){ used=anchor; el=byId[anchor]; }
  else { for(var i=0;i<fallbacks.length;i++){ if(byId[fallbacks[i]]){ used=fallbacks[i]; el=byId[fallbacks[i]]; break; } } }
  var before=region.scrollTop, maximum=Math.max(0,region.scrollHeight-region.clientHeight);
  if(!el) return {action:'anchor_missing',anchorId:anchor,before:before,after:before,
                  scrollHeight:region.scrollHeight,clientHeight:region.clientHeight,present:Object.keys(byId).length};
  var r=rect(region), x=rect(el), top;
  if(r && x) top=region.scrollTop+(x.top-r.top)+(x.height||0);
  else top=(typeof el.offsetTop==='number')?el.offsetTop:0;
  // One screen of overlap: the anchor row settles at the bottom edge, so the
  // next forward step reveals only rows not yet examined.
  var target=Math.max(0,Math.min(maximum,Math.floor(top-region.clientHeight)));
  region.scrollTop=target;
  region.dispatchEvent(new Event('scroll',{bubbles:true}));
  return {action:'anchored',anchorId:anchor,usedId:used,before:before,after:region.scrollTop,
          scrollHeight:region.scrollHeight,clientHeight:region.clientHeight,present:Object.keys(byId).length};
})(__ANCHOR__,__FALLBACKS__))"""

_INSPECT_PARENT_IDS_JS = r"""JSON.stringify((function(targets,knownList){
""" + _UI_LIB_JS + r"""
  var links=allLinks(), region=findRegion(links);
  if(!region) return {action:links.length?'no_verified_scroller':'no_comment_links',parents:[],path:location.pathname};
  var known=knownSet(knownList), items=walk(region,known), found=controls(region,known);
  var parentItems={}; items.forEach(function(it){if(it.type==='parent'&&!parentItems[it.id])parentItems[it.id]=it;});
  var controlsByParent={}; found.controls.forEach(function(c){
    if(c.owner&&!controlsByParent[c.owner]&&usable(c.el)) controlsByParent[c.owner]=c;
  });
  var result=targets.map(function(parent){
    parent=String(parent); var it=parentItems[parent], c=controlsByParent[parent], x=it?rect(it.el):null;
    return {parentId:parent,present:!!it,rendered:!!(it&&it.el.getClientRects().length),
      visible:!!(it&&inView(it.el,region)),domIndex:it?it.position:null,
      top:x?x.top:null,bottom:x?x.bottom:null,control:!!c,
      controlText:c?c.text:null,advertisedCount:c?c.advertisedCount:null,
      renderedLinks:c&&c.container?c.container.querySelectorAll('a[href*="/c/"]').length:null};
  });
  return {action:'parents_inspected',parents:result,path:location.pathname,
    scrollTop:region.scrollTop,scrollHeight:region.scrollHeight,clientHeight:region.clientHeight,
    atEnd:region.scrollTop>=Math.max(0,region.scrollHeight-region.clientHeight)-2};
})(__TARGETS__,__KNOWN__))"""

_FOCUS_PARENT_JS = r"""JSON.stringify((function(parent){
""" + _UI_LIB_JS + r"""
  var links=allLinks(), region=findRegion(links);
  if(!region) return {action:links.length?'no_verified_scroller':'no_comment_links',parentId:parent,path:location.pathname};
  var el=links.find(function(a){
    if(id(a)!==parent)return false; var n=a; while(n&&n!==region)n=n.parentElement; return n===region;
  });
  if(!el) return {action:'audit_parent_missing',parentId:parent,path:location.pathname};
  var before=region.scrollTop, maximum=Math.max(0,region.scrollHeight-region.clientHeight);
  var r=rect(region),x=rect(el),absolute=(r&&x)?before+(x.top-r.top):(typeof el.offsetTop==='number'?el.offsetTop:before);
  region.scrollTop=Math.max(0,Math.min(maximum,Math.floor(absolute-region.clientHeight*0.25)));
  region.dispatchEvent(new Event('scroll',{bubbles:true}));
  return {action:'audit_parent_focused',parentId:parent,path:location.pathname,
    before:before,immediate:region.scrollTop,after:region.scrollTop,
    scrollHeight:region.scrollHeight,clientHeight:region.clientHeight,
    direction:region.scrollTop>before?'forward':region.scrollTop<before?'backward':'same'};
})(__PARENT__))"""


def screen(session, known_parents=(), *, wait=0.0, on_event=None):
    """Enumerate the screen: parent rows in view, present parent rows in
    document order, and the reply controls in view with their owners."""
    script = _SCREEN_JS.replace("__KNOWN__", json.dumps([str(v) for v in known_parents]))
    return _run_ui(session, script, wait=wait, on_event=on_event)


def visible_thread_parents(session, known_parents=(), *, wait=0.0, on_event=None):
    """Parent IDs whose reply control is on screen right now, in list order."""
    result = screen(session, known_parents, wait=wait, on_event=on_event)
    if isinstance(result, dict) and "threads" in result:
        result = {**result, "parents": result["threads"], "visibleParents": result.get("parents")}
    return result


def scan_step(session, *, wait=3.0, on_event=None):
    """Move the verified comment region forward by 70% of its height and,
    after ``wait``, report the settled geometry."""
    if not hasattr(session, "run"):
        return {"action": "unsupported_session"}
    batch = [cmd.run_js(_STEP_JS, out_type="str")]
    if wait and wait > 0:
        batch.extend((cmd.sleep(wait), cmd.run_js(_SCROLL_STATE_JS, out_type="str")))
    values = session.run(batch, retries=0, on_event=on_event)
    result = _decode_ui(values[0] if values else None)
    if wait and wait > 0 and len(values) > 2:
        settled = _decode_ui(values[2])
        if settled.get("action") == "observed_comment_region":
            result.update({k: v for k, v in settled.items() if k != "action"})
    return result


def scan_to_parent(session, parent_id, *, wait=2.0, on_event=None):
    """Move directly to the next confirmed unscanned parent in the DOM."""
    script = _NEXT_PARENT_JS.replace("__PARENT__", json.dumps(str(parent_id)))
    if not hasattr(session, "run"):
        return {"action": "unsupported_session"}
    batch = [cmd.run_js(script, out_type="str")]
    if wait and wait > 0:
        batch.extend((cmd.sleep(wait), cmd.run_js(_SCROLL_STATE_JS, out_type="str")))
    values = session.run(batch, retries=0, on_event=on_event)
    result = _decode_ui(values[0] if values else None)
    if wait and wait > 0 and len(values) > 2:
        settled = _decode_ui(values[2])
        if settled.get("action") == "observed_comment_region":
            result.update({k: v for k, v in settled.items() if k != "action"})
    return result


def restore_anchor(session, anchor_id, fallback_ids=(), *, wait=2.0, on_event=None):
    """Scroll back so the last examined row sits at the bottom of the screen."""
    script = (_ANCHOR_JS.replace("__ANCHOR__", json.dumps(str(anchor_id)))
              .replace("__FALLBACKS__", json.dumps([str(v) for v in fallback_ids])))
    return _run_ui(session, script, wait=wait, on_event=on_event)


def inspect_parent_ids(session, parent_ids, known_parents=(), *, on_event=None):
    script = (_INSPECT_PARENT_IDS_JS
              .replace("__TARGETS__", json.dumps([str(v) for v in parent_ids]))
              .replace("__KNOWN__", json.dumps([str(v) for v in known_parents])))
    return _run_ui(session, script, wait=0.0, on_event=on_event)


def focus_parent(session, parent_id, *, wait=2.0, on_event=None):
    script = _FOCUS_PARENT_JS.replace("__PARENT__", json.dumps(str(parent_id)))
    if not hasattr(session, "run"):
        return {"action": "unsupported_session"}
    batch = [cmd.run_js(script, out_type="str")]
    if wait and wait > 0:
        batch.extend((cmd.sleep(wait), cmd.run_js(_SCROLL_STATE_JS, out_type="str")))
    values = session.run(batch, retries=0, on_event=on_event)
    result = _decode_ui(values[0] if values else None)
    if wait and wait > 0 and len(values) > 2:
        settled = _decode_ui(values[2])
        if settled.get("action") == "observed_comment_region":
            result.update({k: v for k, v in settled.items() if k != "action"})
    return result


_DRAIN_JS = r"""JSON.stringify((function(parent,knownList){
""" + _UI_LIB_JS + r"""
  var known=knownSet(knownList); known[parent]=true;
  var links=allLinks();
  if(!links.some(function(a){return id(a)===parent;})) return {action:'parent_not_visible',parentId:parent};
  var region=findRegion(links)||document.body;
  var found=controls(region,known), own=null, foreign=0;
  found.controls.forEach(function(c){
    if(!usable(c.el)) return;
    if(c.owner===parent){ if(!own) own=c; } else foreign++;
  });
  if(own){
    own.el.click();
    return {action:'clicked_replies',parentId:parent,text:own.text,
            renderedLinks:own.container?own.container.querySelectorAll('a[href*="/c/"]').length:null,
            foreignControls:foreign};
  }
  if(found.unowned){
    // A usable control precedes every confirmed parent row: nothing proves
    // whose thread it continues. Record the shape, press nothing.
    var order=[]; walk(region,known).forEach(function(it){ order.push(it.type==='parent'?'P':it.type==='link'?'l':'c'); });
    return {action:'ambiguous_thread_boundary',parentId:parent,unownedControls:found.unowned,
            structure:order.join('').slice(0,120)};
  }
  return {action:'reply_control_absent',parentId:parent,foreignControls:foreign};
})(__PARENT__,__OTHERS__))"""


def drain_reply_thread(session, parent_id, other_parent_ids=(), *, wait=0.5, on_event=None):
    """Click one reply control inside exactly one thread.

    ``other_parent_ids`` are the confirmed parents; a control between this
    parent's row and the next confirmed one is this thread's. Missing
    controls and genuinely ambiguous boundaries remain incomplete: only a
    fresh terminal network response can prove that a thread is exhausted.
    """
    if not re.fullmatch(r"\d+", str(parent_id)):
        raise ValueError("Reply UI requires a numeric parent ID")
    script = _DRAIN_JS.replace("__PARENT__", json.dumps(str(parent_id))).replace(
        "__OTHERS__", json.dumps([str(v) for v in other_parent_ids]))
    return _run_ui(session, script, wait=wait, on_event=on_event)


def advance_ui(session, *, wait=3.0, on_event=None):
    """Trigger the next comment page: press "Load more comments" when it is
    rendered, then take the verified comment region to its bottom."""
    if not hasattr(session, "run"):
        return {"action": "unsupported_session"}
    batch = [cmd.run_js(_ADVANCE_JS, out_type="str")]
    if wait and wait > 0:
        batch.extend((cmd.sleep(wait), cmd.run_js(_SCROLL_STATE_JS, out_type="str")))
    values = session.run(batch, retries=0, on_event=on_event)
    result = _decode_ui(values[0] if values else None)
    if wait and wait > 0 and len(values) > 2:
        settled = _decode_ui(values[2])
        if settled.get("action") == "observed_comment_region":
            result.update({k: v for k, v in settled.items() if k != "action"})
        elif result.get("action") not in ("unsupported_session", "invalid_result"):
            result["settledAction"] = settled.get("action")
    return result


def observe_ui(session, *, wait=5.0, on_event=None):
    """Wait without another scroll, then read the settled comment geometry."""
    if not hasattr(session, "run"):
        return {"action": "unsupported_session"}
    batch = []
    if wait and wait > 0:
        batch.append(cmd.sleep(wait))
    batch.append(cmd.run_js(_SCROLL_STATE_JS, out_type="str"))
    values = session.run(batch, retries=0, on_event=on_event)
    return _decode_ui(values[-1] if values else None)


def _decode_ui(value):
    raw = (value or {}).get("result") if isinstance(value, dict) else None
    try:
        return json.loads(raw) if isinstance(raw, str) else {"action": "invalid_result"}
    except ValueError:
        return {"action": "invalid_result"}


def _run_ui(session, script, *, wait, on_event):
    if not hasattr(session, "run"):
        return {"action": "unsupported_session"}
    # A zero-length sleep is a no-op to us but a parameter getbro rejects: one
    # measured run lost its whole parent step to `sleep(wait_time=0.0)` in the
    # read-only thread listing. Send the pause only when there is one to make.
    batch = [cmd.run_js(script, out_type="str")]
    if wait and wait > 0:
        batch.append(cmd.sleep(wait))
    values = session.run(batch, retries=0, on_event=on_event)
    return _decode_ui(values[0] if values else None)


def page(connection, *, replies=False, direction="forward"):
    """Normalize a Relay connection to the same page contract as REST.

    A backward connection continues through ``has_previous_page`` and
    ``start_cursor``; REST spells that head direction ``min_id``.
    """
    nodes = [e["node"] for e in connection.get("edges", []) if isinstance(e, dict) and isinstance(e.get("node"), dict)]
    info = connection.get("page_info") or {}
    if direction == "backward":
        return {"child_comments" if replies else "comments": nodes,
                "has_more_head_child_comments" if replies else "has_more_headload_comments": info.get("has_previous_page"),
                "next_min_child_cursor" if replies else "next_min_id": info.get("start_cursor")}
    return {"child_comments" if replies else "comments": nodes,
            "has_more_tail_child_comments" if replies else "has_more_comments": info.get("has_next_page"),
            "next_max_child_cursor" if replies else "next_max_id": info.get("end_cursor")}
