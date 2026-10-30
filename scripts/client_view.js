// Shows what a whiteboard client would draw, using the same web/board.js as the page.
// usage: node scripts/client_view.js <n> <need> [replica ...]
//   need = 1 and one replica: a client that trusts that replica (the Raft client)
//   need = f+1 and all replicas: the PBFT client
const { Quorum, Board } = require('../web/board.js');
const [n, need, ...only] = process.argv.slice(2).map(Number);
const from = only.length ? only : [...Array(n).keys()];

(async () => {
  const q = new Quorum(need), board = new Board();
  const logs = await Promise.all(from.map(i =>
    fetch(`http://localhost:${8000 + i}/admin/log`).then(r => r.json()).catch(() => [])));
  const longest = Math.max(0, ...logs.map(l => l.length));
  let drawn = 0, tampered = 0;
  for (let k = 0; k < longest; k++) {               // deliver round-robin, as sockets interleave
    from.forEach((i, j) => {
      if (k >= logs[j].length) return;
      for (const b of q.receive(i, logs[j][k])) b.ops.forEach((op, x) => {
        if (b.dup && b.dup[x]) return;
        if (board.apply(op).type === 'add') { drawn++; if (op.tampered) tampered++; }
      });
    });
  }
  const sus = [...q.suspects].map(i => 'r' + i).join(',') || 'none';
  console.log(`view from [${from.map(i => 'r' + i)}] need=${need}: ${drawn} shapes drawn, ${tampered} tampered, ignored: ${sus}`);
})();
