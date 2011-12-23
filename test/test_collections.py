"""
Collection (list_*) code was added without tests and since then some
bugs have been found. This tries to at least get some coverage of
the code. Not to check full correctness, just to exercise.
"""

import os, shutil

from tiddlyweb.config import config
from tiddlyweb.store import Store, StoreError

from tiddlyweb.model.tiddler import Tiddler
from tiddlyweb.model.bag import Bag
from tiddlyweb.model.recipe import Recipe
from tiddlyweb.model.user import User

import py.test


def setup_module(module):
    if os.path.exists('store'):
        shutil.rmtree('store')

    module.store = Store(config['server_store'][0], config['server_store'][1],
            environ={'tiddlyweb.config': config})
    module.store.storage.mc.flush_all()


def test_memcache_up():
    store.storage.mc.set('keyone', 'valueone')
    assert store.storage.mc.get('keyone') == 'valueone'
    store.storage.mc.delete('keyone')


def test_list_users():
    user = User('monkey')
    store.put(user)

    users = list(store.list_users())
    assert len(users) == 1

    store.get(users[0])
    store.delete(user)

    users = list(store.list_users())
    assert len(users) == 0

def test_list_recipes():
    recipe = Recipe('monkey')
    store.put(recipe)

    recipes = list(store.list_recipes())
    assert len(recipes) == 1

    store.get(recipes[0])
    store.delete(recipe)

    recipes = list(store.list_recipes())
    assert len(recipes) == 0

