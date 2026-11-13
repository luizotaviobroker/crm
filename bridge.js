// Ponte do CRM com o Firebase: login com Google + banco Firestore.
// Imita a interface de banco que o CRM já usava (collection/doc/get/set/delete/onSnapshot),
// mas sincroniza só o que mudou desde a última vez (economiza leituras do plano grátis).
import { initializeApp } from 'https://www.gstatic.com/firebasejs/10.14.1/firebase-app.js';
import { getAuth, GoogleAuthProvider, signInWithPopup, signInWithRedirect, getRedirectResult, onAuthStateChanged, signOut } from 'https://www.gstatic.com/firebasejs/10.14.1/firebase-auth.js';
import { initializeFirestore, persistentLocalCache, persistentSingleTabManager, terminate, clearIndexedDbPersistence, collection, doc, getDoc, getDocsFromCache, setDoc, onSnapshot, query, where, serverTimestamp, Timestamp, writeBatch } from 'https://www.gstatic.com/firebasejs/10.14.1/firebase-firestore.js';
import { firebaseConfig } from './firebase-config.js';

const app = initializeApp(firebaseConfig);
const auth = getAuth(app);
const fs = initializeFirestore(app, { localCache: persistentLocalCache({ tabManager: persistentSingleTabManager(undefined) }) });

const $g = id => document.getElementById(id);
const gate = $g('authGate'), msg = $g('agMsg'), btn = $g('agIn');
const say = t => { if (msg) msg.textContent = t; };

// ---------- dados ----------
// o Firestore não aceita lista dentro de lista: esses documentos vão guardados como texto (_j)
const hasNested = (v, inArr) => Array.isArray(v) ? (inArr || v.some(x => hasNested(x, true))) : (v && typeof v === 'object') ? Object.values(v).some(x => hasNested(x, false)) : false;
const enc = v => hasNested(v, false) ? { _j: JSON.stringify(v) } : v;
const clean = v => { if (!v) return v; if (typeof v._j === 'string') { try { return JSON.parse(v._j); } catch (x) { return null; } } const o = Object.assign({}, v); delete o._u; delete o._del; return o; };
const ms = t => (t && typeof t.toMillis === 'function') ? t.toMillis() : 0;

// um "motor" por coleção: começa pelo que já está guardado no aparelho e escuta só as mudanças
const ENG = {};
function engine(name) {
  if (ENG[name]) return ENG[name];
  const e = { map: new Map(), subs: new Set(), errs: new Set(), t: null };
  const emit = () => { clearTimeout(e.t); e.t = setTimeout(() => { const s = snap(e); e.subs.forEach(f => { try { f(s); } catch (x) { console.error(x); } }); }, 0); };
  e.emit = emit;
  e.ready = (async () => {
    const c = collection(fs, name);
    let maxU = 0;
    try { const cs = await getDocsFromCache(c); cs.forEach(d => { const v = d.data(); e.map.set(d.id, v); maxU = Math.max(maxU, ms(v._u)); }); } catch (x) { /* sem cache ainda */ }
    await new Promise(res => {
      let first = true;
      onSnapshot(query(c, where('_u', '>', Timestamp.fromMillis(maxU))), s => {
        s.docChanges().forEach(ch => { if (ch.type !== 'removed') e.map.set(ch.doc.id, ch.doc.data()); });
        if (first) { first = false; res(); } emit();
      }, err => { console.error(name, err); e.err = err; e.errs.forEach(f => f(err)); if (first) { first = false; res(); } });
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
function local(path, v) { const parts = path.split('/'); if (parts.length !== 2) return; const e = ENG[parts[0]]; if (!e) return; e.map.set(parts[1], Object.assign({}, v, { _u: Timestamp.now() })); e.emit(); }
function ref(path) {
  const r = doc(fs, path);
  return {
    id: r.id,
    async get() { let s; try { s = await getDoc(r); } catch (x) { s = null; } const v = s && s.exists() ? s.data() : null; return { id: r.id, exists: !!(v && !v._del), data: () => clean(v) }; },
    set: v => { local(path, enc(v)); return setDoc(r, Object.assign({}, enc(v), { _u: serverTimestamp() })); },
    update: v => setDoc(r, Object.assign({}, v, { _u: serverTimestamp() }), { merge: true }),
    delete: () => { local(path, { _del: true }); return setDoc(r, { _del: true, _u: serverTimestamp() }); },
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
  say(`Pronto: ${items.length} registros importados. Abrindo o CRM…`);
  setTimeout(() => { location.href = location.pathname; }, 900);
}

// ---------- quando não carrega ----------
function problem(err) {
  const c = err && err.code;
  say(c === 'failed-precondition' ? 'O CRM já está aberto em outra aba ou janela. Feche as outras e toque em Tentar de novo.'
    : c === 'permission-denied' ? 'Sem permissão no banco: confira se entrou com o e-mail liberado nas regras do Firestore.'
    : 'Não consegui carregar seus dados' + (c ? ' (' + c + ')' : ' (demorou demais)') + '. Toque em Tentar de novo.');
  btn.hidden = false; btn.textContent = 'Tentar de novo';
  btn.onclick = async () => { say('Limpando o cache do aparelho…'); try { await terminate(fs); await clearIndexedDbPersistence(fs); } catch (x) { console.error(x); } location.href = location.pathname; };
}
// ---------- login ----------
let started = false;
async function start(user) {
  if (started) return; started = true;
  say('Carregando seus dados…'); btn.hidden = true;
  const cli = engine('clientes');
  const ok = await Promise.race([cli.ready.then(() => true), new Promise(r => setTimeout(() => r(false), 15000))]);
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
