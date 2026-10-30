// Run with: node --test web/
const test = require('node:test');
const assert = require('node:assert');
const { Quorum, Board } = require('./board.js');

const blk = (index, hash) => ({ index, hash, ops: [] });

test('f+1 matching replies release a block; one liar cannot', () => {
  const q = new Quorum(2); // N = 4, f = 1
  assert.deepStrictEqual(q.receive(1, blk(1, 'evil')), []);
  assert.deepStrictEqual(q.receive(0, blk(1, 'good')), []);
  const out = q.receive(2, blk(1, 'good'));
  assert.strictEqual(out.length, 1);
  assert.strictEqual(out[0].hash, 'good');
  assert.ok(q.suspects.has(1), 'the replica that sent a different block is flagged');
});

test('blocks are released in index order', () => {
  const q = new Quorum(2);
  q.receive(0, blk(2, 'b'));
  q.receive(1, blk(2, 'b'));
  assert.deepStrictEqual(q.receive(0, blk(1, 'a')), []);
  const out = q.receive(1, blk(1, 'a'));
  assert.deepStrictEqual(out.map(b => b.index), [1, 2]);
});

test('a late contradicting block marks its sender', () => {
  const q = new Quorum(2);
  q.receive(0, blk(1, 'a'));
  q.receive(2, blk(1, 'a'));
  q.receive(3, blk(1, 'x'));
  assert.ok(q.suspects.has(3));
  assert.ok(!q.suspects.has(0));
});

test('need = 1 (Raft) trusts the single connected replica', () => {
  const q = new Quorum(1);
  assert.strictEqual(q.receive(0, blk(1, 'a')).length, 1);
});

const draw = id => ({ kind: 'draw', id, shape: { points: [[0, 0], [1, 1]] } });
const move = (target, x, lamport, client) => ({ kind: 'move', target, x, y: 0, lamport, client });

test('concurrent moves: the later Lamport stamp wins in either commit order', () => {
  const a = move('s', 10, 5, 'alice'), b = move('s', 20, 7, 'bob');
  for (const order of [[a, b], [b, a]]) {
    const bd = new Board();
    bd.apply(draw('s'));
    order.forEach(op => bd.apply(op));
    assert.strictEqual(bd.shapes.get('s').x, 20);
  }
});

test('equal Lamport time is broken by client id, the same way everywhere', () => {
  const a = move('s', 10, 5, 'alice'), b = move('s', 20, 5, 'bob');
  for (const order of [[a, b], [b, a]]) {
    const bd = new Board();
    bd.apply(draw('s'));
    order.forEach(op => bd.apply(op));
    assert.strictEqual(bd.shapes.get('s').x, 20);
  }
});

test('erase is final', () => {
  const bd = new Board();
  bd.apply(draw('s'));
  assert.strictEqual(bd.apply({ kind: 'erase', target: 's' }).type, 'remove');
  assert.strictEqual(bd.apply(move('s', 5, 99, 'z')).type, 'none');
  assert.deepStrictEqual(bd.live(), []);
});
