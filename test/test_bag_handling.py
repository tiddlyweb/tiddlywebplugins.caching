"""
A bug was discovered wherein revisions were
not being deleted from the cache properly.
This test confirms it is fixed. In the process
it should confirm generally workingness.
"""

from tiddlyweb.config import config
from tiddlyweb.store import Store, NoTiddlerError

from tiddlyweb.model.tiddler import Tiddler
from tiddlyweb.model.bag import Bag

import py.test


def setup_module(module):
    module.store = Store(config['server_store'][0], config['server_store'][1],
            environ={'tiddlyweb.config': config})
    try:
        bag = Bag('holder')
        store.delete(bag)
    except:
        pass
    bag = Bag('holder')
    module.store.put(bag)


def test_memcache_up():
    store.storage._mc.set('keyone', 'valueone')
    assert store.storage._mc.get('keyone') == 'valueone'
    store.storage._mc.delete('keyone')


def test_put_tiddlers_delete_bag():
    tiddler = Tiddler('tiddler1', 'holder')
    tiddler.text = 'one'
    store.put(tiddler)
    tiddler = Tiddler('tiddler2', 'holder')
    tiddler.text = 'two'
    store.put(tiddler)

    retrieved = Tiddler('tiddler1', 'holder')
    retrieved = store.get(retrieved)
    assert retrieved.text == 'one'
    assert retrieved.bag == 'holder'

    retrieved = Tiddler('tiddler2', 'holder')
    retrieved = store.get(retrieved)
    assert retrieved.text == 'two'
    assert retrieved.bag == 'holder'

    bag = Bag('holder')
    store.delete(bag)

    py.test.raises(NoTiddlerError, 'store.get(retrieved)')


def test_get_bag():
    bag = Bag('thing')
    bag.desc = 'stuff'
    store.put(bag)

    if hasattr(bag, 'list_tiddlers'):
        retrieved = Bag('thing')
        retrieved.skinny = True
        retrieved = store.get(retrieved)
        assert retrieved.desc == 'stuff'
        retrieved = Bag('thing')
        retrieved = store.get(retrieved)
        assert retrieved.desc == 'stuff'
    else:
        retrieved = Bag('thing')
        retrieved = store.get(retrieved)
        assert retrieved.desc == 'stuff'
