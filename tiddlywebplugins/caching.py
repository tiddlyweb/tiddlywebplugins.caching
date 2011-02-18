
import logging
import uuid

from tiddlyweb.store import Store as StoreBoss, HOOKS
from tiddlyweb.stores import StorageInterface
from tiddlyweb.manage import make_command
from tiddlyweb.model.bag import Bag
from tiddlyweb.model.tiddler import Tiddler
from tiddlyweb.util import sha

from tiddlywebplugins.utils import get_store


__version__ = '0.9.4'


ANY_NAMESPACE = 'any_namespace'
BAGS_NAMESPACE = 'bags_namespace'
RECIPES_NAMESPACE = 'recipes_namespace'
USERS_NAMESPACE = 'users_namespace'


def container_namespace_key(container, container_name=''):
    if not container_name:
        key = '%s_namespace' % container
    else:
        key = '%s:%s_namespace' % (container, container_name)
    return key.encode('UTF-8', 'replace')


def tiddler_change_hook(store, tiddler):
    bag_name = tiddler.bag
    title = tiddler.title
    any_key = ANY_NAMESPACE
    bag_key = container_namespace_key('bags', bag_name)
    logging.debug('%s tiddler change resetting namespace keys, %s, %s',
            __name__, any_key, bag_key)
    # This get_store is required to work around confusion with what
    # store is current.
    top_store = get_store(store.environ['tiddlyweb.config'])
    top_store.storage._mc.set(any_key.encode('utf8'), '%s' % uuid.uuid4())
    top_store.storage._mc.set(bag_key.encode('utf8'), '%s' % uuid.uuid4())


def bag_change_hook(store, bag):
    bag_name = bag.name
    any_key = ANY_NAMESPACE
    bags_key = BAGS_NAMESPACE
    bag_key = container_namespace_key('bags', bag_name)
    logging.debug('%s bag change resetting namespace keys, %s, %s, %s',
            __name__, any_key, bags_key, bag_key)
    top_store = get_store(store.environ['tiddlyweb.config'])
    top_store.storage._mc.set(any_key.encode('utf8'), '%s' % uuid.uuid4())
    top_store.storage._mc.set(bag_key.encode('utf8'), '%s' % uuid.uuid4())
    top_store.storage._mc.set(bags_key.encode('utf8'), '%s' % uuid.uuid4())


def recipe_change_hook(store, recipe):
    recipe_name = recipe.name
    any_key = ANY_NAMESPACE
    recipes_key = RECIPES_NAMESPACE
    recipe_key = container_namespace_key('recipes', recipe_name)
    logging.debug('%s: %s recipe change resetting namespace keys, %s, %s, %s',
            store.storage, __name__, any_key, recipes_key, recipe_key)
    top_store = get_store(store.environ['tiddlyweb.config'])
    top_store.storage._mc.set(any_key.encode('utf8'), '%s' % uuid.uuid4())
    top_store.storage._mc.set(recipe_key.encode('utf8'), '%s' % uuid.uuid4())
    top_store.storage._mc.set(recipes_key.encode('utf8'), '%s' % uuid.uuid4())


def user_change_hook(store, user):
    user_name = user.usersign
    any_key = ANY_NAMESPACE
    users_key = USERS_NAMESPACE
    user_key = container_namespace_key('users', user_name)
    logging.debug('%s: %s user change resetting namespace keys, %s, %s, %s',
            store.storage, __name__, any_key, users_key, user_key)
    top_store = get_store(store.environ['tiddlyweb.config'])
    top_store.storage._mc.set(any_key.encode('utf8'), '%s' % uuid.uuid4())
    top_store.storage._mc.set(user_key.encode('utf8'), '%s' % uuid.uuid4())
    top_store.storage._mc.set(users_key.encode('utf8'), '%s' % uuid.uuid4())


# Establish the hooks that will reset namespaces
HOOKS['tiddler']['put'].append(tiddler_change_hook)
HOOKS['tiddler']['delete'].append(tiddler_change_hook)
HOOKS['bag']['put'].append(bag_change_hook)
HOOKS['bag']['delete'].append(bag_change_hook)
HOOKS['recipe']['put'].append(recipe_change_hook)
HOOKS['recipe']['delete'].append(recipe_change_hook)
HOOKS['user']['put'].append(user_change_hook)
HOOKS['user']['delete'].append(user_change_hook)


class Store(StorageInterface):

    _MC = None

    def __init__(self, store_config=None, environ=None):
        if store_config is None:
            store_config = {}
        if environ is None:
            environ = {}
        self.environ = environ
        self.config = environ.get('tiddlyweb.config')

        self._mc = self._MC

        if self._mc == None:
            try:
                from google.appengine.api import memcache
                self._MC = memcache
            except ImportError:
                import memcache
                try:
                    self._MC = memcache.Client(self.config.get(
                        'memcache_hosts', ['127.0.0.1:11211']))
                except KeyError:
                    from tiddlyweb.config import config
                    self.config = config
                    self._MC = memcache.Client(self.config.get(
                        'memcache_hosts', ['127.0.0.1:11211']))
            self._mc = self._MC

        cached_store = StoreBoss(self.config['cached_store'][0],
                self.config['cached_store'][1], environ=environ)
        self.cached_storage = cached_store.storage
        self.prefix = self.config['server_prefix']
        self.host = self.config['server_host']['host']

    def recipe_delete(self, recipe):
        key = self._recipe_key(recipe)
        self._mc.delete(key)
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
            self._mc.set(key, recipe)
        return recipe

    def recipe_put(self, recipe):
        key = self._recipe_key(recipe)
        self.cached_storage.recipe_put(recipe)
        self._mc.delete(key)

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
            self._mc.set(key, bag)
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
            logging.debug('satisfying tiddler_get with cache %s:%s',
                    tiddler.bag, tiddler.title)
            cached_tiddler.recipe = tiddler.recipe
            tiddler = cached_tiddler
        else:
            logging.debug('satisfying tiddler_get with data %s:%s',
                    tiddler.bag, tiddler.title)
            tiddler = self.cached_storage.tiddler_get(tiddler)
            try:
                del tiddler.store
            except AttributeError:
                pass
            self._mc.set(key, tiddler)
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
            self._mc.set(key, user)
        return user

    def user_put(self, user):
        self.cached_storage.user_put(user)

    def list_recipes(self):
        return self.cached_storage.list_recipes()

    def list_bags(self):
        return self.cached_storage.list_bags()

    def list_users(self):
        return self.cached_storage.list_users()

    def list_bag_tiddlers(self, bag):
        return self.cached_storage.list_bag_tiddlers(bag)

    def list_tiddler_revisions(self, tiddler):
        return self.cached_storage.list_tiddler_revisions(tiddler)

    def search(self, search_query):
        return self.cached_storage.search(search_query)

    def _tiddler_key(self, tiddler):
        return self._mangle('bags', tiddler.bag, tiddler.title)

    def _tiddler_revision_key(self, tiddler):
        key = '%s/%s' % (tiddler.title, tiddler.revision)
        return self._mangle('bags', tiddler.bag, key)

    def _user_key(self, user):
        return self._mangle('users', user.usersign)

    def _bag_key(self, bag):
        return self._mangle('bags', bag.name)

    def _recipe_key(self, recipe):
        return self._mangle('recipes', recipe.name)

    def _mangle(self, container, container_name='', descendant=''):
        namespace_key = container_namespace_key(container, container_name)
        namespace = self._mc.get(namespace_key)
        if not namespace:
            namespace = '%s' % uuid.uuid4()
            logging.debug('%s no namespace for %s, setting to %s', __name__,
                    namespace_key, namespace)
            self._mc.set(namespace_key.encode('utf8'), namespace)
        key = '%s/%s/%s' % (container, container_name, descendant)
        fullkey = '%s:%s:%s:%s' % (namespace, self.host, self.prefix, key)
        return sha(fullkey.encode('UTF-8')).hexdigest()

    def _get(self, key):
        logging.warn('trying cache key %s', key)
        return self._mc.get(key)


def init(config):

    @make_command()
    def memcachestats(args):
        """dump the memcachestats"""
        from pprint import pprint
        store = get_store(config)
        pprint(store.storage._mc.get_stats())
