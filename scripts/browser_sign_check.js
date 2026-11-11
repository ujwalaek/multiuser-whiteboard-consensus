// Checks the browser client's request signing against the replicas: the same WebCrypto
// calls as web/index.html (Ed25519 key, sign the JSON, append "sig"), sent to a running
// PBFT cluster. Expect: an unsigned stroke is refused, the signed one commits.
// usage: node scripts/browser_sign_check.js   (cluster from scripts/run_cluster.sh pbft 4)
const { subtle } = globalThis.crypto ?? require('crypto').webcrypto; // Node 18 keeps it in crypto
const hex = buf => [...new Uint8Array(buf)].map(b => b.toString(16).padStart(2, '0')).join('');
(async () => {
  const k = await subtle.generateKey({ name: 'Ed25519' }, false, ['sign', 'verify']);
  const id = hex(await subtle.exportKey('raw', k.publicKey));
  const op = { kind: 'draw', id: 'browser-check-' + Date.now(), client: id, lamport: 1,
    shape: { type: 'polyline', points: [[5, 5], [60, 40]], color: '#00aa00' } };
  const signed = JSON.stringify(op);
  const sig = hex(await subtle.sign('Ed25519', k.privateKey, new TextEncoder().encode(signed)));
  const body = signed.slice(0, -1) + `,"sig":"${sig}"}`;
  const post = b => fetch('http://localhost:8000/submit', { method: 'POST', body: b }).then(r => r.json());
  console.log('unsigned stroke ->', JSON.stringify(await post(signed)));
  console.log('signed stroke   ->', JSON.stringify(await post(body)));
  await new Promise(r => setTimeout(r, 500));
  const log = await (await fetch('http://localhost:8001/admin/log')).json();
  const found = log.some(b => b.ops.some(o => o.id === op.id));
  console.log(found ? 'COMMITTED: replica r1 holds the browser-signed stroke' : 'NOT COMMITTED');
  process.exit(found ? 0 : 1);
})();
