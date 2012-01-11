
import logging
import uuid

from tiddlyweb.store import (Store as StoreBoss, HOOKS,
        StoreError, NoTiddlerError)
from tiddlyweb.stores import StorageInterface
from tiddlyweb.manage import make_command
from tiddlyweb.model.tiddler import Tiddler
from tiddlyweb.util import sha

from tiddlywebplugins.utils import get_store


__version__ = '0.9.14'


ANY_NAMESPACE = 'any'
BAGS_NAMESPACE = 'bags'
RECIPES_NAMESPACE = 'recipes'
USERS_NAMESPACE = 'users'


def container_namespace_key(container, container_name=''):
    if not container_name:
        key = '%s_namespace' % container
    else:
        key = '%s:%s_namespace' % (container, container_name)
    return sha(key.encode('UTF-8', 'replace')).hexdigest()


def tiddler_change_hook(store, tiddler):
    bag_name = tiddler.bag
    any_key = container_namespace_key(ANY_NAMESPACE)
    bag_key = container_namespace_key('bags', bag_name)
    logging.debug('%s tiddler change resetting namespace keys, %s, %s',
            __name__, any_key, bag_key)
    # This get_store is required to work around confusion with what
    # store is current.
    top_store = get_store(store.environ['tiddlyweb.config'])
    top_store.storage.mc.set(any_key.encode('utf8'), '%s' % uuid.uuid4())
    top_store.storage.mc.set(bag_key.encode('utf8'), '%s' % uuid.uuid4())


def bag_change_hook(store, bag):
    bag_name = bag.name
    any_key = container_namespace_key(ANY_NAMESPACE)
    bags_key = container_namespace_key(BAGS_NAMESPACE)
    bag_key = container_namespace_key('bags', bag_name)
    logging.debug('%s bag change resetting namespace keys, %s, %s, %s',
            __name__, any_key, bags_key, bag_key)
    top_store = get_store(store.environ['tiddlyweb.config'])
    top_store.storage.mc.set(any_key.encode('utf8'), '%s' % uuid.uuid4())
    top_store.storage.mc.set(bag_key.encode('utf8'), '%s' % uuid.uuid4())
    top_store.storage.mc.set(bags_key.encode('utf8'), '%s' % uuid.uuid4())


def recipe_change_hook(store, recipe):
    recipe_name = recipe.name
    any_key = container_namespace_key(ANY_NAMESPACE)
    recipes_key = container_namespace_key(RECIPES_NAMESPACE)
    recipe_key = container_namespace_key('recipes', recipe_name)
    logging.debug('%s: %s recipe change resetting namespace keys, %s, %s, %s',
            store.storage, __name__, any_key, recipes_key, recipe_key)
    top_store = get_store(store.environ['tiddlyweb.config'])
    top_store.storage.mc.set(any_key.encode('utf8'), '%s' % uuid.uuid4())
    top_store.storage.mc.set(recipe_key.encode('utf8'), '%s' % uuid.uuid4())
    top_store.storage.mc.set(recipes_key.encode('utf8'), '%s' % uuid.uuid4())


def user_change_hook(store, user):
    user_name = user.usersign
    any_key = container_namespace_key(ANY_NAMESPACE)
    users_key = container_namespace_key(USERS_NAMESPACE)
    user_key = container_namespace_key('users', user_name)
    logging.debug('%s: %s user change resetting namespace keys, %s, %s, %s',
            store.storage, __name__, any_key, users_key, user_key)
    top_store = get_store(store.environ['tiddlyweb.config'])
    top_store.storage.mc.set(any_key.encode('utf8'), '%s' % uuid.uuid4())
    top_store.storage.mc.set(user_key.encode('utf8'), '%s' % uuid.uuid4())
    top_store.storage.mc.set(users_key.encode('utf8'), '%s' % uuid.uuid4())


# Establish the hooks that will reset namespaces
# These must be first on the list or weird things can happen.
HOOKS['tiddler']['put'].insert(0, tiddler_change_hook)
HOOKS['tiddler']['delete'].insert(0, tiddler_change_hook)
HOOKS['bag']['put'].insert(0, bag_change_hook)
HOOKS['bag']['delete'].insert(0, bag_change_hook)
HOOKS['recipe']['put'].insert(0, recipe_change_hook)
HOOKS['recipe']['delete'].insert(0, recipe_change_hook)
HOOKS['user']['put'].insert(0, user_change_hook)
HOOKS['user']['delete'].insert(0, user_change_hook)


class Store(StorageInterface):

    _MC = None

    def __init__(self, store_config=None, environ=None):
        if store_config is None:
            store_config = {}
        if environ is None:
            environ = {}
        self.environ = environ
        self.config = environ.get('tiddlyweb.config')

        self.mc = self._MC

        if self.mc == None:
            try:
                from google.appengine.api import memcache
                self._MC = memcache
            except ImportError:
                kwargs = {}
                try:
                    import pylibmc as memcache
                    kwargs = {'binary': True}
                except ImportError:
                    import memcache
                try:
                    self._MC = memcache.Client(self.config.get(
                        'memcache_hosts', ['127.0.0.1:11211']),
                        **kwargs)
                except KeyError:
                    from tiddlyweb.config import config
                    self.config = config
                    self._MC = memcache.Client(self.config.get(
                        'memcache_hosts', ['127.0.0.1:11211']),
                        **kwargs)
            self.mc = self._MC
            self._dne_text = sha(self.config.get('secret',
                'abc123')).hexdigest()

        cached_store = StoreBoss(self.config['cached_store'][0],
                self.config['cached_store'][1], environ=environ)
        self.cached_storage = cached_store.storage
        self.prefix = self.config['server_prefix']
        self.host = self.config['server_host']['host']

    def recipe_delete(self, recipe):
        key = self._recipe_key(recipe)
        self.mc.delete(key)
        self.cached_storage.recipe_delete(recipe)

    def recipe_get(self, recipe):
        key = self._recipe_key(recipe)
        cached_recipe = self._get(key)
        if cached_recipe:
            recipe = cached_recipe
        else:
            recipe = self.cached_storage.recipe_get(recipe)
            try:
                del recipe.store
            except AttributeError:
                pass
            self.mc.set(key, recipe)
        return recipe

    def recipe_put(self, recipe):
        key = self._recipe_key(recipe)
        self.cached_storage.recipe_put(recipe)
        self.mc.delete(key)

    def bag_delete(self, bag):
        # we don't need to delete tiddler from the cache, name spacing
        # will do that
        self.cached_storage.bag_delete(bag)

    def bag_get(self, bag):
        key = self._bag_key(bag)
        cached_bag = self._get(key)
        if cached_bag:
            bag = cached_bag
        else:
            bag = self.cached_storage.bag_get(bag)
            try:
                del bag.store
            except AttributeError:
                pass
            self.mc.set(key, bag)
        return bag

    def bag_put(self, bag):
        key = self._bag_key(bag)
        self.cached_storage.bag_put(bag)
        # we don't need to delete from the cache, namespace will
        # handle that

    def tiddler_delete(self, tiddler):
        # XXX what about revisions?
        self.cached_storage.tiddler_delete(tiddler)

    def tiddler_get(self, tiddler):
        if not tiddler.revision or tiddler.revision == 0:
            key = self._tiddler_key(tiddler)
        else:
            key = self._tiddler_revision_key(tiddler)
        cached_tiddler = self._get(key)
        if cached_tiddler:
            if cached_tiddler.text == self._dne_text:
                raise NoTiddlerError('Tiddler %s:%s:%s not found' %
                        (cached_tiddler.bag,
                           cached_tiddler.title,
                           cached_tiddler.revision))
            logging.debug('satisfying tiddler_get with cache %s:%s',
                    tiddler.bag, tiddler.title)
            cached_tiddler.recipe = tiddler.recipe
            tiddler = cached_tiddler
        else:
            try:
                logging.debug('satisfying tiddler_get with data %s:%s',
                        tiddler.bag, tiddler.title)
                tiddler = self.cached_storage.tiddler_get(tiddler)
                try:
                    del tiddler.store
                except AttributeError:
                    pass
                self.mc.set(key, tiddler)
            except StoreError, exc:
                dne_tiddler = Tiddler(tiddler.title, tiddler.bag)
                dne_tiddler.text = self._dne_text
                self.mc.set(key, dne_tiddler)
                raise
        return tiddler

    def tiddler_put(self, tiddler):
        self.cached_storage.tiddler_put(tiddler)
        # let hooks take care of cleaning cache

    def user_delete(self, user):
        self.cached_storage.user_delete(user)
        # let hooks take care of cleaning cache

    def user_get(self, user):
        key = self._user_key(user)
        cached_user = self._get(key)
        if cached_user:
            user = cached_user
        else:
            user = self.cached_storage.user_get(user)
            try:
                del user.store
            except AttributeError:
                pass
            self.mc.set(key, user)
        return user

    def user_put(self, user):
        self.cached_storage.user_put(user)

    def list_recipes(self):
        if self.config.get('memcache.cache_lists', False):
            key = self._recipes_key()
            cached_recipes = self._get(key)
            if cached_recipes:
                recipes = cached_recipes
            else:
                recipes = list(self.cached_storage.list_recipes())
                self.mc.set(key, recipes)
            return iter(recipes)
        else:
            return self.cached_storage.list_recipes()

    def list_bags(self):
        if self.config.get('memcache.cache_lists', False):
            key = self._bags_key()
            cached_bags = self._get(key)
            if cached_bags:
                bags = cached_bags
            else:
                bags = list(self.cached_storage.list_bags())
                self.mc.set(key, bags)
            return iter(bags)
        else:
            return self.cached_storage.list_bags()

    def list_users(self):
        if self.config.get('memcache.cache_lists', False):
            key = self._users_key()
            cached_users = self._get(key)
            if cached_users:
                users = cached_users
            else:
                users = list(self.cached_storage.list_users())
                self.mc.set(key, users)
            return iter(users)
        else:
            return self.cached_storage.list_users()

    def list_bag_tiddlers(self, bag):
        if self.config.get('memcache.cache_lists', False):
            key = self._bag_tiddlers_key(bag.name)
            cached_tiddlers = self._get(key)
            if cached_tiddlers:
                tiddlers = cached_tiddlers
            else:
                tiddlers = list(self.cached_storage.list_bag_tiddlers(bag))
                self.mc.set(key, tiddlers)
            return iter(tiddlers)
        return self.cached_storage.list_bag_tiddlers(bag)

    def list_tiddler_revisions(self, tiddler):
        return self.cached_storage.list_tiddler_revisions(tiddler)

    def search(self, search_query):
        return self.cached_storage.search(search_query)

    def _tiddler_key(self, tiddler):
        return self._mangle('bags', tiddler.bag, tiddler.title)

    def _bag_tiddlers_key(self, bag_name):
        return self._mangle('bags', bag_name, 'bags/tiddlers')

    def _tiddler_revision_key(self, tiddler):
        key = '%s/%s' % (tiddler.title, tiddler.revision)
        return self._mangle('bags', tiddler.bag, key)

    def _user_key(self, user):
        return self._mangle('users', user.usersign)

    def _bag_key(self, bag):
        return self._mangle('bags', bag.name)

    def _recipe_key(self, recipe):
        return self._mangle('recipes', recipe.name)

    def _bags_key(self):
        return self._mangle('bags')

    def _recipes_key(self):
        return self._mangle('recipes')

    def _users_key(self):
        return self._mangle('users')

    def _mangle(self, container, container_name='', descendant=None):
        namespace_key = container_namespace_key(container, container_name)
        namespace = self.mc.get(namespace_key)
        if not namespace:
            namespace = '%s' % uuid.uuid4()
            logging.debug('%s no namespace for %s, setting to %s', __name__,
                    namespace_key, namespace)
            self.mc.set(namespace_key.encode('utf8'), namespace)
        key = '/'.join([container, container_name])
        if descendant is not None:
            key = key + '/%s' % descendant
        fullkey = '%s:%s:%s:%s' % (namespace, self.host, self.prefix, key)
        return sha(fullkey.encode('UTF-8')).hexdigest()

    def _get(self, key):
        return self.mc.get(key)


def init(config):

    @make_command()
    def memcachestats(args):
        """dump the memcachestats"""
        from pprint import pprint
        store = get_store(config)
        pprint(store.storage.mc.get_stats())
