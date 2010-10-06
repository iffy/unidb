from twisted.trial.unittest import TestCase
from twisted.python.filepath import FilePath as FP
from twisted.internet.defer import inlineCallbacks, Deferred, returnValue
from zope.interface.verify import verifyClass
import web

import sqlite3

from smd.unidb import AsyncSqliteDB, AsyncDB, AsyncPostgresDB
from smd.unidb import SyncSqliteDB
from smd.unidb import IDeferredReturningDB, ISynchronousDB

#------------------------------------------------------------------------------
# postgres skip?
#------------------------------------------------------------------------------
try:
    skipPostgres = None
    import psycopg2
except Exception, e:
    skipPostgres = str(e)


class IDeferredReturningDBMixin:
    
    timeout = 3
    
    def getDB(self):
        """
        I return an instance of an IDeferredReturningDB implementor
        """
        raise NotImplementedError('You need to have this method return an instance '
            'of an IDeferredReturningDB-implementing class -- the one you want to test.')
    
    @inlineCallbacks
    def getReadyDB(self):
        """
        I create and delete the necessary things to run all the rest of the tests
        in this thing.
        """
        db = self.getDB()
        createsql = 'create table foobar (id integer primary key, value text)'
        try:
            b = yield db.dQuery(createsql)
        except Exception, e:
            _ = yield db.dQuery('drop table foobar')
            _ = yield db.dQuery(createsql)
        returnValue(db)
    
    @inlineCallbacks
    def test_verifyClass(self):
        """
        I verify that this class implements the IDeferredReturningDB.
        """
        s = yield self.getReadyDB()
        verifyClass(IDeferredReturningDB, s.__class__)

    @inlineCallbacks
    def test_query_is_deferred(self):
        """
        I test that .query returns a Deferred.
        """
        s = yield self.getReadyDB()
        a = s.dQuery('select 1 as foo;')
        self.assertTrue(isinstance(a, Deferred), a)
        a.addErrback(lambda x: None)
    
    @inlineCallbacks
    def test_query_base_test(self):
        """
        I test the very basic functionality of .query
        """
        s = yield self.getReadyDB()
        a = yield s.dQuery("select 'hey' as foo;")
        self.assertEqual(len(a), 1)
        self.assertEqual(a[0].foo, 'hey')
    
    @inlineCallbacks
    def test_insert_is_deferred(self):
        """
        I test that .insert returns a Deferred.
        """
        s = yield self.getReadyDB()
        a = s.dInsert('foobar', value='hey')
        self.assertTrue(isinstance(a, Deferred), a)
        a.addErrback(lambda x: None)
    
    @inlineCallbacks
    def test_insert_base_test(self):
        """
        I test that insert returns the last inserted row id
        """
        s = yield self.getReadyDB()
        a = yield s.dInsert('foobar', value='hey')
        self.assertEqual(a, 1)
    
    @inlineCallbacks
    def test_select_is_deferred(self):
        """
        I test that .select returns a Deferred
        """
        s = yield self.getReadyDB()
        a = s.dSelect('foobar')
        self.assertTrue(isinstance(a, Deferred), a)
        a.addErrback(lambda x: None)
    
    @inlineCallbacks
    def test_select_base_test(self):
        """
        I test that .select returns a list of web.Storage objects.
        """
        s = yield self.getReadyDB()
        a = yield s.dInsert('foobar', value='foo')
        b = yield s.dInsert('foobar', value='bar')
        c = yield s.dSelect('foobar', order='id asc')
        self.assertEqual(len(c), 2)
        self.assertTrue(isinstance(c[0], web.Storage), c[0])
        self.assertEqual(c[0].value, 'foo')
        self.assertEqual(c[1].value, 'bar') 
    
    def test_select_no_table(self):
        """
        I test that an error is returned for a select on a non-existant table
        """
        s = self.getReadyDB()
        def cb1(s):
            q1 = s.dSelect('garbage')
            def cb2(q2):
                self.fail('Expected an exception')
            def eb2(e):
                return None
            q1.addCallbacks(cb2, eb2)
        s.addCallback(cb1).addBoth(lambda x: None)
        return s
    
    @inlineCallbacks
    def test_update_is_deferred(self):
        """
        I test that .update returns a Deferred.
        """
        s = yield self.getReadyDB()
        a = s.dUpdate('foobar', where='1=1', value='something')
        self.assertTrue(isinstance(a, Deferred), a)
        a.addErrback(lambda x: None)
    
    @inlineCallbacks
    def test_update_base_test(self):
        """
        I test basic functionality of an update, namely that it returns the number of
        rows updated.
        """
        s = yield self.getReadyDB()
        a = yield s.dInsert('foobar', value='foo')
        b = yield s.dInsert('foobar', value='bar')
        c = yield s.dUpdate('foobar', where='value=$value', vars={'value':'foo'}, value='bar')
        self.assertEqual(c, 1)
        c = yield s.dUpdate('foobar', where='value=$value', vars={'value':'bar'}, value='sam')
        self.assertEqual(c, 2)
    
    @inlineCallbacks
    def test_delete_is_deferred(self):
        """
        I test that .delete returns a Deferred.
        """
        s = yield self.getReadyDB()
        a = s.dDelete('foobar', where='1=1')
        self.assertTrue(isinstance(a, Deferred), a)
        a.addErrback(lambda x: None)
    
    @inlineCallbacks
    def test_delete_base_test(self):
        """
        I test that delete returns the number of rows deleted.
        """
        i = []
        def f():
            i.append(True)
        s = yield self.getReadyDB()
        f()
        a = yield s.dInsert('foobar', value='foo')
        f()
        a = yield s.dInsert('foobar', value='bar')
        f()
        a = yield s.dInsert('foobar', value='bar')
        f()
        c = yield s.dDelete('foobar', where='value=$value', vars={'value':'foo'})
        f()
        self.assertEqual(c, 1)
        f()
        c = yield s.dDelete('foobar', where='value=$value', vars={'value':'bar'})
        f()
        self.assertEqual(c, 2)
            

#------------------------------------------------------------------------------

class TestAsyncPostgresDB(TestCase, IDeferredReturningDBMixin):

    skip = skipPostgres
    timeout = 3
    
    def test_basic(self):
        self.fail('You have psycopg2?.  Please write these tests!')
    
    def test_another(self):
        self.fail('You have psycopg2?.  Please write these tests!')

#------------------------------------------------------------------------------

class TestAsyncSqliteDB(TestCase, IDeferredReturningDBMixin):   
    
    timeout = 3

    def getDB(self):
        f = FP(self.mktemp())
        db = AsyncSqliteDB(f.path)
        return db
    
#------------------------------------------------------------------------------

class TestSyncSqliteDB(TestCase, IDeferredReturningDBMixin):
    
    timeout = 3
    
    def getDB(self):
        f = FP(self.mktemp())
        db = SyncSqliteDB(f.path)
        return db

    def test_syncQuery(self):
        db = self.getDB()
        db.query('create table foobar (name text)')
        db.insert('foobar', name='bar')
        a = db.select('foobar')
        self.assertEqual(len(a), 1)
        self.assertEqual(a[0].name, 'bar')

    def test_verifySyncClass(self):
        db = self.getDB()
        verifyClass(ISynchronousDB, db.__class__)







