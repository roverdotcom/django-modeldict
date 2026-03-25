import time
from unittest import mock

import pytest
from django.core.cache import cache
from django.core.signals import request_finished
from django.test import TestCase, TransactionTestCase
from modeldict import ModelDict
from modeldict.base import CachedDict

from testapp.models import ModelDictModel


class ModelDictTest(TransactionTestCase):
    # XXX: uses transaction test due to request_finished signal causing a rollback

    def setUp(self):
        cache.clear()
        self.now = time.time()

    def assertHasReceiver(self, signal, function):
        for receiver_entry in signal.receivers:
            receiver_ref = receiver_entry[1]
            if receiver_ref() == function:
                return True
        self.fail("Signal {} has no receiver {}".format(signal, function))

    def test_remote_cache_key_suffixed_by_py_version(self):
        mydict = ModelDict(ModelDictModel, key='key', value='value', instances=True)
        assert mydict.remote_cache_key.endswith('py3')

    def test_api(self):
        base_count = ModelDictModel.objects.count()

        mydict = ModelDict(ModelDictModel, key='key', value='value')
        mydict['foo'] = 'bar'
        assert mydict['foo'] == 'bar'
        assert ModelDictModel.objects.values_list('value', flat=True).get(key='foo') == 'bar'
        assert ModelDictModel.objects.count() == base_count + 1
        mydict['foo'] = 'bar2'
        assert mydict['foo'] == 'bar2'
        assert ModelDictModel.objects.values_list('value', flat=True).get(key='foo') == 'bar2'
        assert ModelDictModel.objects.count() == base_count + 1
        mydict['foo2'] = 'bar'
        assert mydict['foo2'] == 'bar'
        assert ModelDictModel.objects.values_list('value', flat=True).get(key='foo2') == 'bar'
        assert ModelDictModel.objects.count() == base_count + 2
        del mydict['foo2']
        with pytest.raises(KeyError):
            mydict.__getitem__('foo2')
        assert not ModelDictModel.objects.filter(key='foo2').exists()
        assert ModelDictModel.objects.count() == base_count + 1

        ModelDictModel.objects.create(key='foo3', value='hello')

        assert mydict['foo3'] == 'hello'
        assert ModelDictModel.objects.filter(key='foo3').exists(), True
        assert ModelDictModel.objects.count() == base_count + 2

        request_finished.send(sender=self)

        assert mydict._last_checked_for_remote_changes == 0.0

        # These should still error because even though the cache repopulates (local cache)
        # the remote cache pool does not
        # self.assertRaises(KeyError, mydict.__getitem__, 'foo3')
        # self.assertTrue(ModelDictModel.objects.filter(key='foo3').exists())
        # self.assertEquals(ModelDictModel.objects.count(), base_count + 2)

        assert mydict['foo'] == 'bar2'
        assert ModelDictModel.objects.values_list('value', flat=True).get(key='foo') == 'bar2'
        assert ModelDictModel.objects.count() == base_count + 2

        assert mydict.pop('foo') == 'bar2'
        assert mydict.pop('foo', None) is None
        assert not ModelDictModel.objects.filter(key='foo').exists()
        assert ModelDictModel.objects.count() == base_count + 1

    def test_modeldict_instances(self):
        base_count = ModelDictModel.objects.count()

        mydict = ModelDict(ModelDictModel, key='key', value='value', instances=True)
        mydict['foo'] = ModelDictModel(key='foo', value='bar')
        assert isinstance(mydict['foo'], ModelDictModel)
        assert mydict['foo'].pk
        assert mydict['foo'].value == 'bar'
        assert ModelDictModel.objects.values_list('value', flat=True).get(key='foo') == 'bar'
        assert ModelDictModel.objects.count() == base_count + 1
        old_pk = mydict['foo'].pk
        mydict['foo'] = ModelDictModel(key='foo', value='bar2')
        assert isinstance(mydict['foo'], ModelDictModel)
        assert mydict['foo'].pk == old_pk
        assert mydict['foo'].value == 'bar2'
        assert ModelDictModel.objects.values_list('value', flat=True).get(key='foo') == 'bar2'
        assert ModelDictModel.objects.count() == base_count + 1

        # test deletion
        mydict['foo'].delete()
        assert 'foo' not in mydict

    def test_modeldict_instances_auto_create(self):
        mydict = ModelDict(ModelDictModel, key='key', value='value', instances=True, auto_create=True)

        obj = mydict['foo']
        assert isinstance(obj, ModelDictModel)
        assert obj.value == ''

    def test_modeldict_len_empty(self):
        mydict = ModelDict(ModelDictModel, key='key', value='value')
        assert len(mydict) == 0

    def test_modeldict_len_one(self):
        mydict = ModelDict(ModelDictModel, key='key', value='value')
        mydict['hello'] = 'world'
        assert len(mydict) == 1

    def test_modeldict_len_two(self):
        mydict = ModelDict(ModelDictModel, key='key', value='value')
        mydict['hello'] = 'world'
        mydict['hi'] = 'world'
        assert len(mydict) == 2

    def test_modeldict_iter(self):
        mydict = ModelDict(ModelDictModel, key='key', value='value')
        mydict['hello'] = 'world'
        assert next(iter(mydict)) == 'hello'

    def test_modeldict_iteritems(self):
        mydict = ModelDict(ModelDictModel, key='key', value='value')
        mydict['hello'] = 'world'
        assert list(mydict.iteritems()) == [('hello', 'world')]

    def test_modeldict_itervalues(self):
        mydict = ModelDict(ModelDictModel, key='key', value='value')
        mydict['hello'] = 'world'
        assert list(mydict.itervalues()) == ['world']

    def test_modeldict_iterkeys(self):
        mydict = ModelDict(ModelDictModel, key='key', value='value')
        mydict['hello'] = 'world'
        assert list(mydict.iterkeys()) == ['hello']

    def test_modeldict_keys(self):
        mydict = ModelDict(ModelDictModel, key='key', value='value')
        mydict['hello'] = 'world'
        assert list(mydict.keys()) == ['hello']

    def test_modeldict_values(self):
        mydict = ModelDict(ModelDictModel, key='key', value='value')
        mydict['hello'] = 'world'
        assert list(mydict.values()) == ['world']

    def test_modeldict_items(self):
        mydict = ModelDict(ModelDictModel, key='key', value='value')
        mydict['hello'] = 'world'
        assert list(mydict.items()) == [('hello', 'world')]

    @mock.patch('modeldict.base.time')
    def test_modeldict_localcache_has_expired_false_with_no_jitter(self, mock_time):
        timeout = 30
        mock_time.time.return_value = self.now + timeout
        mydict = ModelDict(ModelDictModel, key='key', value='value', timeout=timeout)
        mydict._last_checked_for_remote_changes = self.now
        assert mydict.local_cache_has_expired() == False

    @mock.patch('modeldict.base.time')
    def test_modeldict_localcache_has_expired_true_with_no_jitter(self, mock_time):
        timeout = 30
        mock_time.time.return_value = self.now + timeout + 1
        mydict = ModelDict(ModelDictModel, key='key', value='value', timeout=timeout)
        mydict._last_checked_for_remote_changes = self.now
        assert mydict.local_cache_has_expired() == True

    @mock.patch('modeldict.base.time')
    @mock.patch('modeldict.base.random')
    def test_modeldict_localcache_has_expired_false_with_jitter(self, mock_random, mock_time):
        timeout = 30
        max_local_timeout_jitter = 10
        mock_time.time.return_value = self.now + timeout + max_local_timeout_jitter
        mock_random.random.return_value = 1.0
        mydict = ModelDict(
            ModelDictModel,
            key='key',
            value='value',
            timeout=timeout,
            max_local_timeout_jitter=max_local_timeout_jitter)
        mydict._last_checked_for_remote_changes = self.now
        assert mydict.local_cache_has_expired() == False

    @mock.patch('modeldict.base.time')
    @mock.patch('modeldict.base.random')
    def test_modeldict_localcache_has_expired_true_with_jitter(self, mock_random, mock_time):
        timeout = 30
        max_local_timeout_jitter = 10
        mock_time.time.return_value = self.now + timeout + max_local_timeout_jitter + 1
        mock_random.random.return_value = 1.0
        mydict = ModelDict(
            ModelDictModel,
            key='key',
            value='value',
            timeout=timeout,
            max_local_timeout_jitter=max_local_timeout_jitter)
        mydict._last_checked_for_remote_changes = self.now
        assert mydict.local_cache_has_expired() == True

    def test_modeldict_expirey(self):
        base_count = ModelDictModel.objects.count()

        mydict = ModelDict(ModelDictModel, key='key', value='value')

        assert mydict._local_cache == {}

        mydict['test_modeldict_expirey'] = 'hello'

        assert len(mydict._local_cache) == base_count + 1
        assert mydict['test_modeldict_expirey'] == 'hello'

        self.client.get('/')

        assert mydict._last_checked_for_remote_changes == 0.0
        assert mydict['test_modeldict_expirey'] == 'hello'
        assert len(mydict._local_cache) == base_count + 1

        request_finished.send(sender=self)

        assert mydict._last_checked_for_remote_changes == 0.0
        assert mydict['test_modeldict_expirey'] == 'hello'
        assert len(mydict._local_cache) == base_count + 1

    def test_modeldict_no_auto_create(self):
        # without auto_create
        mydict = ModelDict(ModelDictModel, key='key', value='value')
        with pytest.raises(KeyError):
            mydict['hello']
        assert ModelDictModel.objects.count() == 0

    def test_modeldict_auto_create_no_value(self):
        # with auto_create and no value
        mydict = ModelDict(ModelDictModel, key='key', value='value', auto_create=True)
        repr(mydict['hello'])
        assert ModelDictModel.objects.count() == 1
        assert ModelDictModel.objects.get(key='hello').value == ''

    def test_modeldict_auto_create(self):
        # with auto_create and value
        mydict = ModelDict(ModelDictModel, key='key', value='value', auto_create=True)
        mydict['hello'] = 'foo'
        assert ModelDictModel.objects.count() == 1
        assert ModelDictModel.objects.get(key='hello').value == 'foo'

    def test_save_behavior(self):
        mydict = ModelDict(ModelDictModel, key='key', value='value', auto_create=True)
        mydict['hello'] = 'foo'
        for n in range(10):
            mydict[str(n)] = 'foo'
        assert len(mydict) == 11
        assert ModelDictModel.objects.count() == 11

        mydict = ModelDict(ModelDictModel, key='key', value='value', auto_create=True)
        m = ModelDictModel.objects.get(key='hello')
        m.value = 'bar'
        m.save()

        assert ModelDictModel.objects.count() == 11
        assert len(mydict) == 11
        assert mydict['hello'] == 'bar'

        mydict = ModelDict(ModelDictModel, key='key', value='value', auto_create=True)
        m = ModelDictModel.objects.get(key='hello')
        m.value = 'bar2'
        m.save()

        assert ModelDictModel.objects.count() == 11
        assert len(mydict) == 11
        assert mydict['hello'] == 'bar2'

    def test_setdefault(self):
        mydict = ModelDict(ModelDictModel, key='key', value='value')

        with pytest.raises(KeyError):
            mydict['hello']

        ret = mydict.setdefault('hello', 'world')
        assert ret == 'world'
        assert mydict['hello'] == 'world'

        ret = mydict.setdefault('hello', 'world2')
        assert ret == 'world'
        assert mydict['hello'] == 'world'

    def test_setdefault_instances(self):
        mydict = ModelDict(ModelDictModel, key='key', value='value')

        with pytest.raises(KeyError):
            mydict['hello']

        instance = ModelDictModel(key='hello', value='world')
        ret = mydict.setdefault('hello', instance)
        assert ret == 'world'
        assert mydict['hello'] == 'world'

        instance2 = ModelDictModel(key='hello', value='world2')
        ret = mydict.setdefault('hello', instance2)
        assert ret == 'world'
        assert mydict['hello'] == 'world'

    def test_django_signals_are_connected(self):
        from django.db.models.signals import post_save, post_delete
        from django.core.signals import request_finished

        mydict = ModelDict(ModelDictModel, key='key', value='value', auto_create=True)
        self.assertHasReceiver(post_save, mydict._post_save)
        self.assertHasReceiver(post_delete, mydict._post_delete)
        self.assertHasReceiver(request_finished, mydict._cleanup)

    def test_celery_signals_are_connected(self):
        from celery.signals import task_postrun

        mydict = ModelDict(ModelDictModel, key='key', value='value', auto_create=True)
        self.assertHasReceiver(task_postrun, mydict._cleanup)


class CacheIntegrationTest(TestCase):
    def setUp(self):
        self.cache = mock.Mock()
        self.cache.get.return_value = {}
        self.mydict = ModelDict(ModelDictModel, key='key', value='value', auto_create=True, cache=self.cache)

    def test_switch_creation(self):
        self.mydict['hello'] = 'foo'
        assert self.cache.get.call_count == 0
        assert self.cache.set_many.call_count == 1
        self.cache.set_many.assert_any_call({
            self.mydict.remote_cache_key: {u'hello': u'foo'},
            self.mydict.remote_cache_last_updated_key: self.mydict._last_checked_for_remote_changes,
        })

    @mock.patch('modeldict.base.CachedDict.get_cache_data')
    def test_cache_is_refreshed_if_key_is_missing(self, mock_get_cache_data):
        self.mydict['hello'] = 'foo'
        self.cache.reset_mock()

        self.cache.get.return_value = None
        self.mydict._last_checked_for_remote_changes = 0.0
        self.mydict['hello']

        self.cache.set.assert_called_once_with(
            self.mydict.remote_cache_key,
            mock_get_cache_data.return_value
        )

    def test_switch_creation_with_custom_remote_timeout(self):
        cache = mock.Mock()
        mydict = ModelDict(ModelDictModel,
                key='key',
                value='value',
                auto_create=True,
                cache=cache,
                remote_timeout=None)
        mydict['hello'] = 'foo'
        assert cache.get.call_count == 0
        assert cache.set_many.call_count == 1
        cache.set_many.assert_any_call({
            mydict.remote_cache_key: {u'hello': u'foo'},
            mydict.remote_cache_last_updated_key: mydict._last_checked_for_remote_changes,
        }, timeout=None)

    def test_switch_change(self):
        self.mydict['hello'] = 'foo'
        self.cache.reset_mock()
        self.mydict['hello'] = 'bar'
        assert self.cache.get.call_count == 0
        assert self.cache.set_many.call_count == 1
        self.cache.set_many.assert_any_call({
            self.mydict.remote_cache_key: {u'hello': u'bar'},
            self.mydict.remote_cache_last_updated_key: self.mydict._last_checked_for_remote_changes
        })

    def test_switch_delete(self):
        self.mydict['hello'] = 'foo'
        self.cache.reset_mock()
        del self.mydict['hello']
        assert self.cache.get.call_count == 0
        assert self.cache.set_many.call_count == 1
        self.cache.set_many.assert_any_call({
            self.mydict.remote_cache_key: {},
            self.mydict.remote_cache_last_updated_key: self.mydict._last_checked_for_remote_changes
        })

    def test_switch_access(self):
        self.mydict['hello'] = 'foo'
        self.cache.reset_mock()
        foo = self.mydict['hello']
        foo = self.mydict['hello']
        foo = self.mydict['hello']
        foo = self.mydict['hello']
        assert foo == 'foo'
        assert self.cache.get.call_count == 0
        assert self.cache.set_many.call_count == 0

    def test_switch_access_without_local_cache(self):
        self.mydict['hello'] = 'foo'
        self.mydict._local_cache = {}
        self.mydict._local_last_updated = None
        self.mydict._last_checked_for_remote_changes = 0.0
        self.cache.reset_mock()
        foo = self.mydict['hello']
        assert foo == 'foo'
        # "1" here signifies that we didn't ask the remote cache for its last
        # updated value
        assert self.cache.get.call_count == 1
        assert self.cache.set_many.call_count == 0
        self.cache.get.assert_any_call(self.mydict.remote_cache_key)
        self.cache.reset_mock()
        foo = self.mydict['hello']
        foo = self.mydict['hello']
        foo = self.mydict['hello']
        assert self.cache.get.call_count == 0
        assert self.cache.set_many.call_count == 0

    def test_switch_access_with_expired_local_cache(self):
        self.mydict['hello'] = 'foo'
        self.mydict._last_checked_for_remote_changes = 0.0
        self.cache.reset_mock()
        foo = self.mydict['hello']
        assert foo == 'foo'
        assert self.cache.get.call_count == 2
        assert self.cache.set_many.call_count == 0
        self.cache.get.assert_any_call(self.mydict.remote_cache_last_updated_key)
        self.cache.reset_mock()
        foo = self.mydict['hello']
        foo = self.mydict['hello']
        assert self.cache.get.call_count == 0
        assert self.cache.set_many.call_count == 0

    def test_does_not_pull_down_all_data(self):
        self.mydict['hello'] = 'foo'
        self.cache.get.return_value = self.mydict._local_last_updated - 100
        self.cache.reset_mock()

        self.mydict._cleanup()

        assert self.mydict['hello'] == 'foo'
        self.cache.get.assert_called_once_with(
            self.mydict.remote_cache_last_updated_key
        )


class CachedDictTest(TestCase):
    def setUp(self):
        self.cache = mock.Mock()
        self.mydict = CachedDict(timeout=100, cache=self.cache)

    @mock.patch('modeldict.base.CachedDict._update_cache_data')
    @mock.patch('modeldict.base.CachedDict.local_cache_has_expired', mock.Mock(return_value=True))
    @mock.patch('modeldict.base.CachedDict.local_cache_is_invalid', mock.Mock(return_value=False))
    def test_expired_does_update_data(self, _update_cache_data):
        self.mydict._local_cache = {}
        self.mydict._local_last_updated = time.time()
        self.mydict._last_checked_for_remote_changes = time.time()
        self.mydict._populate()

        assert not _update_cache_data.called

    @mock.patch('modeldict.base.CachedDict._update_cache_data')
    @mock.patch('modeldict.base.CachedDict.local_cache_has_expired', mock.Mock(return_value=False))
    @mock.patch('modeldict.base.CachedDict.local_cache_is_invalid', mock.Mock(return_value=True))
    def test_reset_does_expire(self, _update_cache_data):
        self.mydict._local_cache = {}
        self.mydict._local_last_updated = time.time()
        self.mydict._last_checked_for_remote_changes = time.time()
        self.mydict._populate(reset=True)

        _update_cache_data.assert_called_once_with()

    @mock.patch('modeldict.base.CachedDict._update_cache_data')
    @mock.patch('modeldict.base.CachedDict.local_cache_has_expired', mock.Mock(return_value=False))
    @mock.patch('modeldict.base.CachedDict.local_cache_is_invalid', mock.Mock(return_value=True))
    def test_does_not_expire_by_default(self, _update_cache_data):
        self.mydict._local_cache = {}
        self.mydict._local_last_updated = time.time()
        self.mydict._last_checked_for_remote_changes = time.time()
        self.mydict._populate()

        assert not _update_cache_data.called

    def test_is_expired_missing_last_checked_for_remote_changes(self):
        self.mydict._last_checked_for_remote_changes = 0.0
        assert self.mydict.local_cache_has_expired()
        assert not self.cache.get.called

    def test_is_expired_last_updated_beyond_timeout(self):
        self.mydict._local_last_updated = time.time() - 101
        assert self.mydict.local_cache_has_expired()

    def test_is_expired_within_bounds(self):
        self.mydict._last_checked_for_remote_changes = time.time()

    def test_is_not_expired_if_remote_cache_is_old(self):
        # set it to an expired time
        self.mydict._local_cache = {'a': 1}
        self.mydict._local_last_updated = time.time() - 100
        self.cache.get.return_value = self.mydict._local_last_updated - 1

        result = self.mydict.local_cache_is_invalid()

        self.cache.get.assert_called_once_with(self.mydict.remote_cache_last_updated_key)
        assert not result

    def test_is_expired_if_remote_cache_is_new(self):
        # set it to an expired time, but with a local cache
        self.mydict._local_cache = dict(a=1)
        last_update = time.time() - 101
        self.mydict._local_last_updated = last_update
        self.mydict._last_checked_for_remote_changes = last_update
        self.cache.get.return_value = time.time()

        result = self.mydict.local_cache_is_invalid()

        assert result
        self.cache.get.assert_called_once_with(
            self.mydict.remote_cache_last_updated_key
        )

    def test_is_invalid_if_local_cache_is_none(self):
        self.mydict._local_cache = None
        assert self.mydict.local_cache_is_invalid()

    def test_is_invalid_if_remote_cache_updated_right_after_local_last_updated(self):
        cache.clear()
        mydict = CachedDict(timeout=100)

        mydict.remote_cache.set_many({
            mydict.remote_cache_key: {'MYFLAG': 'value1'},
            mydict.remote_cache_last_updated_key: 12345
        })

        # load the local cache from remote cache
        # this sets: mydict._local_last_updated = time.time()
        mydict._populate()

        # simulate remote cache updated by external process
        # remote_cache[remote_cache_last_updated_key] = time.time()
        mydict.remote_cache.set_many({
            mydict.remote_cache_key: {'MYFLAG': 'value2'},
            mydict.remote_cache_last_updated_key: time.time()
        })

        assert mydict.local_cache_is_invalid()

    def test_populate_timeout(self):
        cache.clear()
        mydict = CachedDict(timeout=100)

        now = time.time()
        mydict.remote_cache.set_many({
            mydict.remote_cache_key: {'MYFLAG': 'value1'},
            mydict.remote_cache_last_updated_key: now
        })

        # load the local cache from remote cache
        mydict._populate()

        mydict.remote_cache.set_many({
            mydict.remote_cache_key: {'MYFLAG': 'value2'},
            mydict.remote_cache_last_updated_key: now + 1
        })

        # before timeout: local cache should not be updated
        with mock.patch('time.time', mock.Mock(return_value=now + mydict.timeout - 1)):
            mydict._populate()
            mydict._populate()
            mydict._populate()
        assert mydict._local_cache == {'MYFLAG': 'value1'}

        # after timeout: local cache should be updated
        with mock.patch('time.time', mock.Mock(return_value=now + mydict.timeout + 1)):
            mydict._populate()
        assert mydict._local_cache == {'MYFLAG': 'value2'}

    def test_local_last_updated(self):
        cache.clear()
        mydict = CachedDict(timeout=100)
        mydict.remote_cache.set_many({
            mydict.remote_cache_key: {'MYFLAG': 'value1'},
            mydict.remote_cache_last_updated_key: 12345
        })
        # load the local cache from remote cache
        # this sets: mydict._local_last_updated = time.time()
        mydict._populate()
        local_last_updated = mydict._local_last_updated
        assert mydict._local_cache == {'MYFLAG': 'value1'}

        with mock.patch('time.time', mock.Mock(return_value=time.time() + 101)):
            mydict.remote_cache.set_many({
                mydict.remote_cache_key: {'MYFLAG': 'value2'},
                mydict.remote_cache_last_updated_key: time.time()
            })
            assert mydict.local_cache_has_expired()
            assert mydict.local_cache_is_invalid()

            mydict._populate()

            assert mydict._local_cache == {'MYFLAG': 'value2'}
            assert mydict._local_last_updated != local_last_updated

    def test_local_last_updated_not_updated_if_not_needed(self):
        cache.clear()
        mydict = CachedDict(timeout=100)
        mydict.remote_cache.set_many({
            mydict.remote_cache_key: {'MYFLAG': 'value1'},
            mydict.remote_cache_last_updated_key: 12345
        })
        # load the local cache from remote cache
        # this sets: mydict._local_last_updated = time.time()
        mydict._populate()
        local_last_updated = mydict._local_last_updated

        with mock.patch('time.time', mock.Mock(return_value=time.time() + 101)):
            assert mydict.local_cache_has_expired()
            assert not mydict.local_cache_is_invalid()

            mydict._populate()

            assert mydict._local_last_updated == local_last_updated
