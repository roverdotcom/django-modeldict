import random
import sys
import time

from django.core.cache import cache
import six

NoValue = object()

DefaultRemoteTimeout = object()


class CachedDict(object):
    def __init__(self,
            cache=cache,
            timeout=30,
            remote_timeout=DefaultRemoteTimeout,
            max_local_timeout_jitter=None):
        cls_name = type(self).__name__

        self._local_cache = {}
        self._local_last_updated = None

        self._last_checked_for_remote_changes = 0.0
        self.timeout = timeout
        self.max_local_timeout_jitter = max_local_timeout_jitter

        self.remote_cache = cache
        self.remote_cache_key = cls_name
        self.remote_cache_last_updated_key = '%s.last_updated' % (cls_name,)

        if not remote_timeout is DefaultRemoteTimeout:
            self.remote_timeout = remote_timeout

    def __getitem__(self, key):
        self._populate()

        try:
            return self._local_cache[key]
        except KeyError:
            if self._local_last_updated is None:
                # Another thread reset the cache
                return self[key]
            value = self.get_default(key)

            if value is NoValue:
                raise

            return value

    def __setitem__(self, key, value):
        raise NotImplementedError

    def __delitem__(self, key):
        raise NotImplementedError

    def __len__(self):
        self._populate()
        return len(self._local_cache)

    def __contains__(self, key):
        self._populate()
        return key in self._local_cache

    def __iter__(self):
        self._populate()
        return iter(self._local_cache)

    def __repr__(self):
        return "<%s>" % (self.__class__.__name__,)

    def iteritems(self):
        self._populate()
        return six.iteritems(self._local_cache)

    def itervalues(self):
        self._populate()
        return six.itervalues(self._local_cache)

    def iterkeys(self):
        self._populate()
        return six.iterkeys(self._local_cache)

    def keys(self):
        return list(self.iterkeys())

    def values(self):
        return list(self.itervalues())

    def items(self):
        self._populate()
        return self._local_cache.items()

    def get(self, key, default=None):
        self._populate()
        return self._local_cache.get(key, default)

    def pop(self, key, default=NoValue):
        value = self.get(key, default)

        try:
            del self[key]
        except KeyError:  # pragma: no cover
            # Concurrent edit
            pass  # pragma: no cover

        return value

    def setdefault(self, key, value):
        if key not in self:
            self[key] = value
        return self[key]

    def get_default(self, key):
        return NoValue

    def local_cache_has_expired(self):
        """
        Returns ``True`` if the in-memory cache has expired.
        """
        recheck_at = (
            self._last_checked_for_remote_changes + self._local_timeout
        )
        return time.time() > recheck_at

    def local_cache_is_invalid(self):
        """
        Returns ``True`` if the local cache is invalid and needs to be
        refreshed with data from the remote cache.

        A return value of ``None`` signifies that no data was available.
        """
        # If the local_cache hasn't been set up, avoid hitting memcache entirely
        if self._local_last_updated is None:
            return True

        remote_last_updated = self.remote_cache.get(
            self.remote_cache_last_updated_key
        )

        if not remote_last_updated:
            # TODO: I don't like how we're overloading the return value here for
            # this method.  It would be better to have a separate method or
            # @property that is the remote last_updated value.
            return None  # Never been updated

        return (
            self._local_last_updated is None or
            remote_last_updated > self._local_last_updated
        )

    def get_cache_data(self):
        """
        Pulls data from the cache backend.
        """
        return self._get_cache_data()

    def clear_cache(self):
        """
        Clears the in-process cache.
        """
        self._local_last_updated = None
        self._last_checked_for_remote_changes = 0.0
        self._local_cache.clear()

    def _populate(self, reset=False):
        """
        Ensures the cache is populated and still valid.

        The cache is checked when:

        - The local timeout has been reached
        - The local cache is not set

        The cache is invalid when:

        - The global cache has expired (via remote_cache_last_updated_key)
        """
        now = time.time()

        if reset:
            self.clear_cache()
        # Otherwise, if the local cache has expired, we need to go check with
        # our remote last_updated value to see if the dict values have changed.
        elif self.local_cache_has_expired():

            local_cache_is_invalid = self.local_cache_is_invalid()

            # If local_cache_is_invalid  is None, that means that there was no
            # data present, so we assume we need to add the key to cache.
            if local_cache_is_invalid is None:
                self.remote_cache.add(
                    self.remote_cache_last_updated_key,
                    now,
                    **self._cache_set_kwargs
                )

            # Now, if the remote has changed OR it was None in the first place,
            # pull in the values from the remote cache and set it to the
            # local_cache
            if local_cache_is_invalid or local_cache_is_invalid is None:
                remote_value = self.remote_cache.get(self.remote_cache_key)
                if remote_value is not None:
                    self._local_cache = remote_value
                    # We've updated from remote, so mark ourselves as
                    # such so that we won't expire until the next timeout
                else:
                    # Version-suffixed cache was invalidated; refresh it from
                    # the DB.
                    self._refresh_versioned_cache()
                self._local_last_updated = now

            # We last checked for remote changes just now
            self._last_checked_for_remote_changes = now

        # Update from cache if local_cache is still empty
        if self._local_last_updated is None:
            self._update_cache_data()

        return self._local_cache

    def _refresh_versioned_cache(self):
        # Another process from the other Python version invalidated this
        # version's cache. Write it from the DB, but don't invalidate the other
        # version's cache or change remote_cache_last_updated
        self._local_cache = self.get_cache_data()
        self.remote_cache.set(self.remote_cache_key, self._local_cache)

    def _update_cache_data(self):
        self._local_cache = self.get_cache_data()

        now = time.time()
        self._local_last_updated = now
        self._last_checked_for_remote_changes = now

        # We only set remote_cache_last_updated_key when we know the cache is
        # current because setting this will force all clients to invalidate
        # their cached data if it's newer
        self.remote_cache.set_many({
            self.remote_cache_key: self._local_cache,
            self.remote_cache_last_updated_key: self._last_checked_for_remote_changes,
        }, **self._cache_set_kwargs)

        # Need to invalidate the cache key for the other Python version, so that
        # it will get rewritten by a process running that Python version.
        other_version_no = '2' if sys.version_info[0] == 3 else '3'
        other_version_remote_cache_key = (
            self.remote_cache_key[:-1]
            + other_version_no
        )

        self.remote_cache.delete(other_version_remote_cache_key)

    def _get_cache_data(self):
        raise NotImplementedError

    def _cleanup(self, *args, **kwargs):
        # We set _last_updated to a false value to ensure we hit the
        # last_updated cache on the next request
        self._last_checked_for_remote_changes = 0.0

    @property
    def _local_timeout(self):
        if self.max_local_timeout_jitter:
            return (
                self.timeout + self.max_local_timeout_jitter * random.random()
            )
        return self.timeout

    @property
    def _cache_set_kwargs(self):
        if hasattr(self, 'remote_timeout'):
            return {'timeout': self.remote_timeout}
        return {}
