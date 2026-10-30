// Client-side logic, kept free of the DOM so Node can test it (web/board.test.js).
// Loaded by index.html as a plain script (globals) and by Node through module.exports.

// Quorum releases committed blocks in index order once `need` different replicas
// have sent the same block hash for that index. A hash covers the whole chain
// before it, so f+1 matching hashes mean at least one honest replica vouches for
// the entire history, and a lone Byzantine replica cannot change what is drawn.
class Quorum {
  constructor(need) {
    this.need = need;
    this.next = 1;               // next index to release
    this.votes = new Map();      // index -> Map(hash -> {block, from: Set})
    this.accepted = new Map();   // index -> released hash, to spot replicas that disagree
    this.suspects = new Set();   // replicas that sent a block contradicting a released one
  }

  // receive records `block` from replica `from` and returns the blocks now released.
  receive(from, block) {
    const ok = this.accepted.get(block.index);
    if (ok !== undefined) {
      if (ok !== block.hash) this.suspects.add(from);
      return [];
    }
    if (!this.votes.has(block.index)) this.votes.set(block.index, new Map());
    const byHash = this.votes.get(block.index);
    if (!byHash.has(block.hash)) byHash.set(block.hash, { block, from: new Set() });
    byHash.get(block.hash).from.add(from);

    const out = [];
    for (;;) {
      const cands = this.votes.get(this.next);
      const win = cands && [...cands.values()].find(c => c.from.size >= this.need);
      if (!win) return out;
      for (const [h, c] of cands) if (h !== win.block.hash) c.from.forEach(r => this.suspects.add(r));
      this.accepted.set(this.next, win.block.hash);
      this.votes.delete(this.next);
      out.push(win.block);
      this.next++;
    }
  }
}

// stamp orders concurrent edits: Lamport time first, client id as the tie-break.
const later = (a, b) => a[0] !== b[0] ? a[0] > b[0] : a[1] > b[1];

// Board is the whiteboard state, driven only by committed ops in commit order.
// Moves carry the absolute position the user dragged to; when two users move the
// same shape concurrently, the move with the later (lamport, client) stamp wins
// whatever order consensus committed them in, so every replica's clients agree.
// Erase is final: a later move of an erased shape is ignored.
class Board {
  constructor() { this.shapes = new Map(); } // id -> {op, x, y, stamp, erased}

  // apply returns what changed: {type: 'add'|'remove'|'move'|'none', id}
  apply(op) {
    const s = this.shapes.get(op.target || op.id);
    switch (op.kind) {
      case 'draw':
        if (s) return { type: 'none', id: op.id };
        this.shapes.set(op.id, { op, x: 0, y: 0, stamp: [-1, ''], erased: false });
        return { type: 'add', id: op.id };
      case 'erase':
        if (!s || s.erased) return { type: 'none', id: op.target };
        s.erased = true;
        return { type: 'remove', id: op.target };
      case 'move': {
        const st = [op.lamport || 0, op.client || ''];
        if (!s || s.erased || !later(st, s.stamp)) return { type: 'none', id: op.target };
        Object.assign(s, { x: op.x, y: op.y, stamp: st });
        return { type: 'move', id: op.target };
      }
    }
    return { type: 'none' };
  }

  live() { return [...this.shapes].filter(([, s]) => !s.erased).map(([id, s]) => ({ id, x: s.x, y: s.y })); }
}

if (typeof module !== 'undefined') module.exports = { Quorum, Board };
