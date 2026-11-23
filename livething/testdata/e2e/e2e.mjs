// The stock @livething/client against testdata/chat.dx (test.sh starts it on
// $PORT). Each check prints "ok <name>" or fails the run.
import { channels, client, clientRequests, connect, keepalive } from '@livething/client';

const url = `ws://127.0.0.1:${process.env.PORT}/chat?client=e2e`;

function fail(message) {
  console.error(`FAIL ${message}`);
  process.exit(1);
}

function ok(name) {
  console.log(`ok ${name}`);
}

function equal(actual, expected, name) {
  if (JSON.stringify(actual) !== JSON.stringify(expected)) {
    fail(`${name}: got ${JSON.stringify(actual)}, want ${JSON.stringify(expected)}`);
  }
  ok(name);
}

function within(ms, promise, name) {
  return Promise.race([
    promise,
    new Promise((_, reject) => setTimeout(() => reject(new Error(`${name}: timed out`)), ms)),
  ]);
}

function socketFor(token) {
  const s = client()(
    connect(url, { token: token ? () => token : undefined }),
    channels(),
    clientRequests({ timeout: 3000 }),
    keepalive(),
  );
  s.onError((err) => {
    if (!s.quietErrors) console.error('onError', err);
  });
  return s;
}

function opened(s) {
  return new Promise((resolve, reject) => {
    s.onOpen(() => resolve());
    s.onClose((e) => reject(new Error(`closed ${e?.code ?? ''}`)));
    s.connect();
  });
}

async function main() {
  // Signed in with the token subprotocol.
  const maya = socketFor('good');
  await within(3000, opened(maya), 'open');
  ok('connected');

  const profile = await maya.request('get-profile');
  equal(profile.user, 'maya', 'token reaches authenticate');
  equal(profile.path, '/chat?client=e2e', 'path and query');
  if (!/^c_[0-9a-f]{16}$/.test(profile.conn)) fail(`connection id ${profile.conn}`);
  ok('connection id');

  equal(await maya.request('add', { a: 2, b: 40 }), { sum: 42 }, 'request reply');

  try {
    await maya.request('nope', {});
    fail('unknown request resolved');
  } catch (err) {
    equal([err.code, err.detail], ['not-found', { name: 'nope' }], 'request error');
  }

  // Connection-scope event answered with an emit.
  const poked = new Promise((resolve) => maya.on('poked', resolve));
  maya.send('poke', null);
  equal(await within(2000, poked, 'poke'), { by: profile.conn }, 'event and emit');

  // A guest joins the same room; a broadcast reaches both.
  const guest = socketFor(null);
  await within(3000, opened(guest), 'guest open');
  equal((await guest.request('get-profile')).user, '', 'guest has no user');

  const mayaRoom = maya.channel('rooms', 'room:general');
  const guestRoom = guest.channel('rooms', 'room:general');
  await within(2000, mayaRoom.subscribe(), 'subscribe');
  await within(2000, guestRoom.subscribe(), 'guest subscribe');
  ok('subscribe');

  const members = await mayaRoom.request('who');
  equal(members.members.length, 2, 'channel request sees both members');

  const toGuest = new Promise((resolve) => guestRoom.on('message', resolve));
  const toMaya = new Promise((resolve) => mayaRoom.on('message', resolve));
  mayaRoom.send('send-message', { text: 'hello' });
  const [g, m] = await within(2000, Promise.all([toGuest, toMaya]), 'broadcast');
  equal(g.text, 'hello', 'broadcast reaches the other member');
  equal(m.from, 'maya', 'broadcast reaches the sender');

  // Refused joins.
  try {
    await within(2000, maya.channel('rooms', 'room:secret').subscribe(), 'secret');
    fail('room:secret let maya in');
  } catch (err) {
    equal(err.code, 'not-a-member', 'subscribe refused');
  }

  // Leaving: the guest stops hearing the room.
  await within(2000, guestRoom.unsubscribe(), 'unsubscribe');
  ok('unsubscribe');
  equal((await mayaRoom.request('who')).members.length, 1, 'left the members');

  // A refused token never opens.
  const bad = socketFor('bad');
  bad.quietErrors = true;
  const refused = await new Promise((resolve) => {
    bad.onOpen(() => resolve('opened'));
    bad.onClose(() => resolve('closed'));
    bad.connect();
    setTimeout(() => resolve('timeout'), 3000);
  });
  equal(refused, 'closed', 'refused token closes without opening');
  bad.disconnect();

  // Keepalive: the server pings every second and drops after three; the
  // client answers, so maya is still connected after five.
  await new Promise((resolve) => setTimeout(resolve, 5000));
  equal(maya.state.get(), 'open', 'keepalive keeps the connection');
  equal((await maya.request('add', { a: 1, b: 1 })).sum, 2, 'still answers after keepalive');

  // A raw socket that never answers is pinged, then dropped as dead.
  const silent = await new Promise((resolve) => {
    const w = new WebSocket(url);
    const seen = [];
    w.onmessage = (e) => seen.push(e.data.startsWith('[') ? JSON.parse(e.data)[0] : e.data);
    w.onclose = () => resolve(seen);
    setTimeout(() => resolve(['still open']), 6000);
  });
  equal([silent[0], silent.includes('ping'), silent.includes('still open')], ['__connected__', true, false],
    'silent connection pinged then dropped');

  maya.disconnect();
  guest.disconnect();
  console.log('all passed');
  process.exit(0);
}

main().catch((err) => fail(err.stack ?? String(err)));
