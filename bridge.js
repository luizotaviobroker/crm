// Ponte do CRM com o Firebase: login com Google + banco Firestore.
// Imita a interface de banco que o CRM já usava (collection/doc/get/set/delete/onSnapshot),
// mas sincroniza só o que mudou desde a última vez (economiza leituras do plano grátis).
import { initializeApp } from 'https://www.gstatic.com/firebasejs/10.14.1/firebase-app.js';
import { getAuth, GoogleAuthProvider, signInWithPopup, signInWithRedirect, getRedirectResult, onAuthStateChanged, signOut } from 'https://www.gstatic.com/firebasejs/10.14.1/firebase-auth.js';
import { initializeFirestore, memoryLocalCache, collection, doc, getDoc, setDoc, onSnapshot, query, where, serverTimestamp, Timestamp, writeBatch } from 'https://www.gstatic.com/firebasejs/10.14.1/firebase-firestore.js';
import { firebaseConfig } from './firebase-config.js';

const app = initializeApp(firebaseConfig);
const auth = getAuth(app);
const fs = initializeFirestore(app, { localCache: memoryLocalCache() });

// cópia dos dados guardada no próprio aparelho (IndexedDB), para abrir rápido e ler do servidor só o que mudou
const IDB = new Promise(res => { try { const r = indexedDB.open('crm-cache', 1); r.onupgradeneeded = () => r.result.createObjectStore('c'); r.onsuccess = () => res(r.result); r.onerror = () => res(null); } catch (x) { res(null); } });
const idbGet = async k => { const d = await IDB; if (!d) return null; return new Promise(res => { try { const q = d.transaction('c').objectStore('c').get(k); q.onsuccess = () => res(q.result || null); q.onerror = () => res(null); } catch (x) { res(null); } }); };
const idbPut = async (k, v) => { const d = await IDB; if (!d) return; try { d.transaction('c', 'readwrite').objectStore('c').put(v, k); } catch (x) { console.error(x); } };
const idbClear = async () => { const d = await IDB; if (!d) return; await new Promise(res => { try { const t = d.transaction('c', 'readwrite'); t.objectStore('c').clear(); t.oncomplete = res; t.onerror = res; } catch (x) { res(); } }); };

const $g = id => document.getElementById(id);
const gate = $g('authGate'), msg = $g('agMsg'), btn = $g('agIn');
const say = t => { if (msg) msg.textContent = t; };

// ---------- dados ----------
// o Firestore não aceita lista dentro de lista: esses documentos vão guardados como texto (_j)
const hasNested = (v, inArr) => Array.isArray(v) ? (inArr || v.some(x => hasNested(x, true))) : (v && typeof v === 'object') ? Object.values(v).some(x => hasNested(x, false)) : false;
const enc = v => hasNested(v, false) ? { _j: JSON.stringify(v) } : v;
const clean = v => { if (!v) return v; if (typeof v._j === 'string') { try { return JSON.parse(v._j); } catch (x) { return null; } } const o = Object.assign({}, v); delete o._u; delete o._del; delete o._pend; return o; };
const ms = t => typeof t === 'number' ? t : (t && typeof t.toMillis === 'function') ? t.toMillis() : 0;
const plain = v => { const o = Object.assign({}, v); o._u = ms(v._u); return o; };

// um "motor" por coleção: começa pela cópia do aparelho e escuta do servidor só o que mudou
const ENG = {};
function engine(name) {
  if (ENG[name]) return ENG[name];
  const e = ENG[name] = { map: new Map(), subs: new Set(), errs: new Set(), t: null, st: null };
  const emit = () => { clearTimeout(e.t); e.t = setTimeout(() => { const s = snap(e); e.subs.forEach(f => { try { f(s); } catch (x) { console.error(x); } }); }, 0); };
  const store = () => { clearTimeout(e.st); e.st = setTimeout(() => { const o = {}; let mx = 0; for (const [id, v] of e.map) { o[id] = v; if (!v._pend) mx = Math.max(mx, v._u || 0); } idbPut(name, { docs: o, maxU: mx }); }, 800); };
  e.emit = () => { emit(); store(); };
  e.ready = (async () => {
    const c = collection(fs, name);
    const cached = await idbGet(name);
    let maxU = 0;
    if (cached && cached.docs) { for (const [id, v] of Object.entries(cached.docs)) e.map.set(id, v); maxU = cached.maxU || 0; }
    await new Promise(res => {
      let first = true; const done = () => { if (first) { first = false; res(); } };
      // sem internet ou servidor lento: abre com a cópia do aparelho e continua tentando
      if (e.map.size) setTimeout(done, 4000);
      onSnapshot(query(c, where('_u', '>', Timestamp.fromMillis(maxU))), s => {
        let ch = 0;
        s.docChanges().forEach(x => { if (x.type === 'removed') return; const v = x.doc.data(); if (!v._u) return; e.map.set(x.doc.id, plain(v)); ch++; });
        done(); if (ch) e.emit(); else emit();
      }, err => { console.error(name, err); e.err = err; e.errs.forEach(f => f(err)); done(); });
    });
  })();
  return e;
}
function snap(e) {
  const docs = [];
  for (const [id, v] of e.map) if (v && !v._del) docs.push({ id, exists: true, data: () => clean(v) });
  return { docs, size: docs.length, empty: !docs.length, forEach: f => docs.forEach(f) };
}
// gravação otimista: a tela já recebe a versão nova, sem esperar o servidor confirmar
function local(path, v) { const parts = path.split('/'); if (parts.length !== 2) return; const e = ENG[parts[0]]; if (!e) return; e.map.set(parts[1], Object.assign({}, v, { _u: Date.now(), _pend: 1 })); e.emit(); }
function ref(path) {
  const r = doc(fs, path);
  return {
    id: r.id,
    async get() { let s; try { s = await getDoc(r); } catch (x) { s = null; } const v = s && s.exists() ? s.data() : null; return { id: r.id, exists: !!(v && !v._del), data: () => clean(v) }; },
    set: v => { gcHook(path, v); local(path, enc(v)); return setDoc(r, Object.assign({}, enc(v), { _u: serverTimestamp() })); },
    update: v => setDoc(r, Object.assign({}, v, { _u: serverTimestamp() }), { merge: true }),
    delete: () => { gcHook(path, null); local(path, { _del: true }); return setDoc(r, { _del: true, _u: serverTimestamp() }); },
    onSnapshot: (cb, err) => onSnapshot(r, s => { const v = s.exists() ? s.data() : null; cb({ id: r.id, exists: !!(v && !v._del), data: () => clean(v) }); }, err)
  };
}
const DB = {
  doc: ref,
  collection: name => ({
    doc: id => ref(name + '/' + (id || Math.random().toString(36).slice(2) + Date.now().toString(36))),
    async get() { const e = engine(name); await e.ready; return snap(e); },
    onSnapshot(cb, err) { const e = engine(name); e.subs.add(cb); if (err) e.errs.add(err); e.ready.then(() => cb(snap(e))); return () => { e.subs.delete(cb); e.errs.delete(err); }; }
  })
};


// ---------- Google Agenda: visitas vão para a agenda "CRM Ginco" (via Apps Script do próprio Luiz) ----------
let GC = null, gcLoaded = null;
const gcLoad = () => gcLoaded || (gcLoaded = getDoc(doc(fs, 'config/gcal')).then(s => { const v = s.exists() ? s.data() : null; GC = v && v.url && v.key && !v.off ? v : null; document.body.classList.toggle('gcon', !!GC); return GC; }).catch(() => null));
const gcTel = t => !t ? '' : t.length === 11 ? `0${t.slice(0, 2)} ${t.slice(2, 7)}-${t.slice(7)}` : t.length === 10 ? `0${t.slice(0, 2)} ${t.slice(2, 6)}-${t.slice(6)}` : t;
const isVisit = c => !!(c && !c._del && c.proximo && (c.proxTipo || (c.etapa === 'Agendou' ? 'visita' : 'fup')) === 'visita');
function gcItem(id, c) {
  if (!isVisit(c)) return { id, del: true };
  const emp = Array.isArray(c.emp) ? c.emp.filter(Boolean).join(', ') : (c.emp || '');
  const cel = String(c.cel || '').replace(/\D/g, '');
  const desc = [
    'Cliente: ' + (c.nome || ''),
    cel ? 'Telefone: ' + gcTel(cel) : '',
    cel ? 'WhatsApp: https://wa.me/55' + cel : '',
    'Empreendimento: ' + (emp || '—'),
    'CIC: ' + (c.cic || '—'),
    c.obs ? '\nResumo: ' + c.obs : '',
    '\n(Criado pelo CRM Ginco — mude a visita pelo CRM, não por aqui)'
  ].filter(Boolean).join('\n');
  const it = { id, titulo: 'Visita: ' + (c.nome || 'cliente') + (emp ? ' · ' + emp : ''), desc: desc.slice(0, 7000) };
  if (c.proxH) it.ini = `${c.proximo}T${c.proxH}:00-04:00`; else it.dia = c.proximo;
  return it;
}
const gcQ = () => { try { return JSON.parse(localStorage.getItem('gcq') || '{}'); } catch (x) { return {}; } };
const gcSaveQ = q => { try { localStorage.setItem('gcq', JSON.stringify(q)); } catch (x) { } };
const tst = t => { try { window.toast && window.toast(t); } catch (x) { } };
let gcBusy = false, gcAgain = false;
async function gcFlush(silent) {
  if (gcBusy) { gcAgain = true; return; }
  const cfg = await gcLoad(); if (!cfg) return;
  const q = gcQ(), items = Object.values(q); if (!items.length) return;
  gcBusy = true;
  try {
    const r = await fetch(cfg.url, { method: 'POST', body: JSON.stringify({ key: cfg.key, items }) });
    const j = await r.json();
    if (!j.ok) throw new Error(j.erro || 'erro');
    const now = gcQ(); items.forEach(it => { if (JSON.stringify(now[it.id]) === JSON.stringify(it)) delete now[it.id]; }); gcSaveQ(now);
    const n = items.filter(i => !i.del).length;
    if (!silent) tst(n ? (n === 1 ? '📅 Visita enviada ao Google Agenda' : `📅 ${n} visitas enviadas ao Google Agenda`) : '📅 Google Agenda atualizado');
  } catch (x) { console.error('gcal', x); if (!silent) tst('📅 Não consegui falar com o Google Agenda — tento de novo depois'); }
  gcBusy = false; if (gcAgain) { gcAgain = false; gcFlush(silent); }
}
function gcPut(items) { const q = gcQ(); items.forEach(it => q[it.id] = it); gcSaveQ(q); }
function gcHook(path, v) {
  try {
    const [c, id] = path.split('/'); if (c !== 'clientes' || !id) return;
    const e = ENG.clientes; const prev = e && e.map.get(id) ? clean(e.map.get(id)) : null;
    const a = gcItem(id, prev), b = gcItem(id, v);
    if (JSON.stringify(a) === JSON.stringify(b)) return;
    gcPut([b]); setTimeout(() => gcFlush(), 300);
  } catch (x) { console.error(x); }
}
window.CRM_gcal = async function () {
  await gcLoad();
  const s = await getDoc(doc(fs, 'config/gcal')).catch(() => null); const cur = s && s.exists() ? s.data() : {};
  const m = document.createElement('div'); m.className = 'authgate'; m.style.zIndex = 300;
  m.innerHTML = `<div class="ag" style="text-align:left"><b>Google Agenda</b>
<p>Toda visita marcada no CRM vai para a agenda <b>CRM Ginco</b> no seu Google Agenda, com nome, telefone, empreendimento, CIC e resumo. Se mudar a data, o horário ou o tipo, o evento muda junto.</p>
<label style="display:grid;gap:4px;font-size:14px">Endereço do app (termina em /exec)<input id="gcU" style="font:inherit;padding:10px;border:1px solid #cfd8e0;border-radius:8px" value="${(cur.url || '').replace(/"/g, '')}"></label>
<label style="display:grid;gap:4px;font-size:14px">Chave<input id="gcK" style="font:inherit;padding:10px;border:1px solid #cfd8e0;border-radius:8px" value="${(cur.key || '').replace(/"/g, '')}"></label>
<p id="gcS" style="min-height:1.2em"></p>
<button type="button" id="gcSave">Salvar e testar</button>
<button type="button" id="gcAll" class="ghost" style="font-size:15px">Enviar todas as visitas já marcadas (de hoje em diante)</button>
${GC ? '<button type="button" id="gcOff" class="ghost">Desligar a sincronização</button>' : ''}
<button type="button" id="gcX" class="ghost">Fechar</button></div>`;
  document.body.appendChild(m);
  const st = t => m.querySelector('#gcS').textContent = t;
  m.querySelector('#gcX').onclick = () => m.remove();
  m.querySelector('#gcSave').onclick = async () => {
    const url = m.querySelector('#gcU').value.trim(), key = m.querySelector('#gcK').value.trim();
    if (!/^https:\/\/script\.google\.com\/.+\/exec$/.test(url)) { st('O endereço precisa começar com https://script.google.com/ e terminar em /exec.'); return; }
    if (!key) { st('Falta a chave.'); return; }
    st('Testando…');
    try { const r = await fetch(url, { method: 'POST', body: JSON.stringify({ key, items: [] }) }); const j = await r.json(); if (!j.ok) { st(j.erro === 'chave' ? 'A chave não confere com a do script.' : 'O script respondeu com erro: ' + j.erro); return; } }
    catch (x) { st('Não consegui falar com o script. Confira se implantou como App da Web com acesso "Qualquer pessoa".'); return; }
    await setDoc(doc(fs, 'config/gcal'), { url, key, off: false, _u: serverTimestamp() });
    gcLoaded = null; await gcLoad(); st('Funcionando. A agenda "CRM Ginco" já existe no seu Google Agenda.');
  };
  m.querySelector('#gcAll').onclick = async () => {
    if (!(await gcLoad())) { st('Salve e teste primeiro.'); return; }
    const e = engine('clientes'); await e.ready; const hoje = new Date().toLocaleDateString('sv');
    const its = []; for (const [id, v] of e.map) { const c = clean(v); if (isVisit(c) && c.proximo >= hoje) its.push(gcItem(id, c)); }
    if (!its.length) { st('Nenhuma visita marcada de hoje em diante.'); return; }
    st(`Enviando ${its.length} visitas…`); gcPut(its); await gcFlush(); st(Object.keys(gcQ()).length ? 'Parte não foi. Tente de novo em instantes.' : `Pronto: ${its.length} visitas no Google Agenda.`);
  };
  const off = m.querySelector('#gcOff'); if (off) off.onclick = async () => { await setDoc(doc(fs, 'config/gcal'), Object.assign({}, cur, { off: true, _u: serverTimestamp() })); gcLoaded = null; await gcLoad(); st('Desligado. Os eventos que já estão lá continuam.'); };
};

// ---------- importar o backup do CRM antigo ----------
async function importBackup(file) {
  say('Lendo o arquivo…');
  const data = JSON.parse(await file.text());
  const items = [];
  for (const [colName, docs] of Object.entries(data)) for (const [id, v] of Object.entries(docs || {})) if (v && typeof v === 'object') items.push([colName, id, v]);
  let n = 0;
  for (let i = 0; i < items.length; i += 400) {
    const b = writeBatch(fs);
    for (const [c, id, v] of items.slice(i, i + 400)) b.set(doc(fs, c, id), Object.assign({}, enc(v), { _u: serverTimestamp() }));
    await b.commit(); n += Math.min(400, items.length - i); say(`Importando… ${n} de ${items.length}`);
  }
  await idbClear();
  say(`Pronto: ${items.length} registros importados. Abrindo o CRM…`);
  setTimeout(() => { location.href = location.pathname; }, 900);
}

// ---------- quando não carrega ----------
function problem(err) {
  const c = err && err.code;
  say(c === 'failed-precondition' ? 'O CRM já está aberto em outra aba ou janela. Feche as outras e toque em Tentar de novo.'
    : c === 'permission-denied' ? 'Sem permissão no banco: confira se entrou com o e-mail liberado nas regras do Firestore.'
    : 'Não consegui carregar seus dados' + (c ? ' (' + c + ': ' + (err.message || '') + ')' : ' (o servidor não respondeu — confira a internet)') + '. Toque em Tentar de novo.');
  btn.hidden = false; btn.textContent = 'Tentar de novo';
  btn.onclick = async () => { say('Limpando a cópia do aparelho…'); await idbClear(); location.href = location.pathname; };
}
// ---------- login ----------
let started = false;
async function start(user) {
  if (started) return; started = true;
  say('Carregando seus dados…'); btn.hidden = true;
  const cli = engine('clientes');
  const ok = await Promise.race([cli.ready.then(() => true), new Promise(r => setTimeout(() => r(false), 25000))]);
  if (!ok || cli.err) { problem(cli.err); return; }
  if (!cli.map.size || /[?&]importar/.test(location.search)) {
    // banco vazio: oferece importar o backup
    say(cli.map.size ? `Importar backup: os registros do arquivo substituem os que têm o mesmo código. Escolha o crm-backup.json.` : `Olá, ${user.displayName || user.email}. Seu banco ainda está vazio. Escolha o arquivo de backup (crm-backup.json) para trazer seus clientes e leads.`);
    const inp = $g('agFile'); $g('agImp').hidden = false;
    inp.onchange = () => { const f = inp.files && inp.files[0]; if (f) importBackup(f).catch(x => { console.error(x); say('Não consegui importar: ' + (x.message || x)); }); };
    $g('agSkip').onclick = () => { gate.hidden = true; window.__setDb(DB); };
    if (cli.map.size) $g('agSkip').textContent = 'Voltar ao CRM sem importar';
    return;
  }
  gate.hidden = true;
  window.__setDb(DB);
  gcLoad().then(() => gcFlush(true));
}
btn.onclick = async () => {
  say('Abrindo o login do Google…');
  const p = new GoogleAuthProvider(); p.setCustomParameters({ prompt: 'select_account' });
  try { await signInWithPopup(auth, p); }
  catch (x) {
    if (x && (x.code === 'auth/popup-blocked' || x.code === 'auth/operation-not-supported-in-this-environment')) { await signInWithRedirect(auth, p); return; }
    if (x && x.code === 'auth/unauthorized-domain') { say('Este endereço ainda não foi autorizado no Firebase (Authentication → Configurações → Domínios autorizados).'); return; }
    if (x && x.code === 'auth/popup-closed-by-user') { say('Login cancelado. Toque em Entrar com Google de novo.'); return; }
    say('Não consegui entrar: ' + (x && (x.code || x.message)));
  }
};
getRedirectResult(auth).catch(x => say('Não consegui entrar: ' + (x && (x.code || x.message))));
onAuthStateChanged(auth, u => { if (u) start(u).catch(x => { console.error(x); say('Erro ao abrir: ' + (x.code || x.message) + (x.code === 'permission-denied' ? ' — confira se entrou com o e-mail liberado nas regras.' : '')); }); else { gate.hidden = false; btn.hidden = false; say('Entre com a sua conta Google para abrir o CRM.'); } });
window.CRM_signOut = () => signOut(auth).then(() => location.reload());
window.CRM_user = () => auth.currentUser && (auth.currentUser.email || '');
