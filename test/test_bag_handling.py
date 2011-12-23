"""
A bug was discovered wherein revisions were
not being deleted from the cache properly.
This test confirms it is fixed. In the process
it should confirm generally workingness.
"""
import os, shutil

from tiddlyweb.config import config
from tiddlyweb.store import Store, NoTiddlerError

from tiddlyweb.model.tiddler import Tiddler
from tiddlyweb.model.bag import Bag

import py.test


def setup_module(module):
    if os.path.exists('store'):
        shutil.rmtree('store')
    module.store = Store(config['server_store'][0], config['server_store'][1],
            environ={'tiddlyweb.config': config})
    try:
        bag = Bag('holder')
        module.store.delete(bag)
    except:
        pass
    bag = Bag('holder')
    module.store.put(bag)


def test_memcache_up():
    store.storage.mc.set('keyone', 'valueone')
    assert store.storage.mc.get('keyone') == 'valueone'
    store.storage.mc.delete('keyone')


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

def test_get_bags():
    bags = store.list_bags()
    bags = store.list_bags()
    for name in ['alpha', 'beta', 'gamma']:
        store.put(Bag(name))
    bags = store.list_bags()
    assert 'alpha' in [bag.name for bag in bags]

def test_listing_tiddlers():
    for title in ['hi', 'bye', 'greetings', 'salutations']:
        tiddler = Tiddler(title, 'thing')
        tiddler.text = title
        store.put(tiddler)

    tiddlers1 = list(store.list_bag_tiddlers(Bag('thing')))
    tiddlers2 = list(store.list_bag_tiddlers(Bag('thing')))

    assert len(tiddlers1) == len(tiddlers2)

    tiddler = Tiddler('adios', 'thing')
    tiddler.text = 'adios'
    store.put(tiddler)

    tiddlers3 = list(store.list_bag_tiddlers(Bag('thing')))
    tiddlers4 = list(store.list_bag_tiddlers(Bag('thing')))

    assert len(tiddlers1) != len(tiddlers3)
    assert len(tiddlers3) == len(tiddlers4)
