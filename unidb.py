from zope.interface import Interface, implements
from twisted.enterprise import adbapi
from twisted.internet import defer
from twisted.python import failure

import sqlite3
sqlite3.paramstyle = 'qmark'
from web.db import DB, SqliteDB, PostgresDB
import web

#------------------------------------------------------------------------------
#
#------------------------------------------------------------------------------
class IDeferredReturningDB(Interface):
    """I define the combination web.py/twisted database api.
    """
    def dQuery(sql_query, vars=None, processed=False, _test=False):
        """Return as Deferred the result of web.db.DB.query though iterBetter
        is unwrapped into a plain list.
        """
    
    def dSelect(tables, vars=None, what='*', where=None, order=None, group=None,
            limit=None, offset=None, _test=False):
        """Return as Deferred the result of web.db.DB.select though iterBetter
        is unwrapped into a plain list.
        """
    
    def dInsert(tablename, seqname=None, _test=False, **values):
        """Return as Deferred the result of web.db.DB.insert
        """
    
    def dUpdate(tables, where, vars=None, _test=False, **values):
        """Return as Deferred the result of web.db.DB.update
        """
    
    def dDelete(table, where, using=None, vars=None, _test=False):
        """Return as Deferred the result of web.db.DB.delete
        """

class ISynchronousDB(Interface):
    """I define the web.py database api.
    """
    def query(sql_query, vars=None, processed=False, _test=False):
        """Return as Deferred the result of web.db.DB.query though iterBetter
        is unwrapped into a plain list.
        """
    
    def select(tables, vars=None, what='*', where=None, order=None, group=None,
            limit=None, offset=None, _test=False):
        """Return as Deferred the result of web.db.DB.select though iterBetter
        is unwrapped into a plain list.
        """
    
    def insert(tablename, seqname=None, _test=False, **values):
        """Return as Deferred the result of web.db.DB.insert
        """
    
    def update(tables, where, vars=None, _test=False, **values):
        """Return as Deferred the result of web.db.DB.update
        """
    
    def delete(table, where, using=None, vars=None, _test=False):
        """Return as Deferred the result of web.db.DB.delete
        """

#------------------------------------------------------------------------------
# Utility functions
#------------------------------------------------------------------------------

def unIter(rows):
    """
    Turn an iterator into a list.
    """
    if hasattr(rows, '__iter__'):
        return [x for x in rows]
    return rows

#------------------------------------------------------------------------------
# Database API for twisted applications
#------------------------------------------------------------------------------
class AsyncDB(adbapi.ConnectionPool):
    """
    I attempt to be an asynchronous web.py database interface.
    """
    implements(IDeferredReturningDB)
    
    fakedb = DB(None, {})
    paramstyle = 'pyformat'
    
    min = 3
    max = 5
    
    def dict_factory(self, cursor, row):
        d = web.Storage()
        for idx, col in enumerate(cursor.description):
            d[col[0]] = row[idx]
        return d
    
    def connect(self):
        conn = adbapi.ConnectionPool.connect(self)
        conn.row_factory = self.dict_factory
        return conn
    
    def errBack(self, res):
        return res
    
    def dQuery(self, *args, **kwargs):
        kwargs['_test'] = True
        q = self.fakedb.query(*args, **kwargs)
        return self.runQuery(q.query(self.paramstyle), q.values()).addErrback(self.errBack)
    
    def dSelect(self, *args, **kwargs):
        kwargs['_test'] = True
        q = self.fakedb.select(*args, **kwargs)
        return self.runQuery(q.query(self.paramstyle), q.values())
    
    def dInsert(self, *args, **kwargs):
        kwargs['_test'] = True
        q = self.fakedb.insert(*args, **kwargs)
        return self.runInteraction(self._execReturnAttrib, q, 'lastrowid')
    
    def getLastId(self, res):
        return res
    
    def dUpdate(self, *args, **kwargs):
        kwargs['_test'] = True
        query = self.fakedb.update(*args, **kwargs)
        return self.runInteraction(self._execReturnAttrib, query, 'rowcount')
    
    def dDelete(self, *args, **kwargs):
        kwargs['_test'] = True
        query = self.fakedb.delete(*args, **kwargs)
        return self.runInteraction(self._execReturnAttrib, query, 'rowcount')
    
    def _execReturnAttrib(self, txn, q, attrib):
        txn.execute(q.query(self.paramstyle), q.values())
        return getattr(txn, attrib)


class AsyncPostgresDB(AsyncDB):
    
    def __init__(self, *args, **kwargs):
        adbapi.ConnectionPool.__init__(self, *args, **kwargs)
        self.fakedb = PostgresDB()


class AsyncSqliteDB(AsyncDB):

    paramstyle = 'qmark'

    def __init__(self, *args, **kwargs):
        kwargs['check_same_thread'] = False
        adbapi.ConnectionPool.__init__(self, 'sqlite3', *args, **kwargs)
        self.fakedb = SqliteDB(db=':memory:')
        


#------------------------------------------------------------------------------
# Database api for synchronous applications
#------------------------------------------------------------------------------

def _Dwrap(func, *args, **kwargs):
    d = defer.Deferred()
    try:
        res = func(*args, **kwargs)
        if isinstance(res, defer.Deferred):
            return res
        d.callback(res)
    except Exception, e:
        d.errback(e)
    return d

class SyncDB(DB):

    implements(IDeferredReturningDB, ISynchronousDB)




class SyncSqliteDB(SqliteDB):

    implements(IDeferredReturningDB, ISynchronousDB)

    def __init__(self, dbname):
        SqliteDB.__init__(self, db=dbname)
        self.printing = False
    
    def dQuery(self, *args, **kwargs):
        return _Dwrap(SqliteDB.query, self, *args, **kwargs).addCallback(unIter)

    def dSelect(self, *args, **kwargs):
        return _Dwrap(SqliteDB.select, self, *args, **kwargs).addCallback(unIter)

    def dInsert(self, *args, **kwargs):
        return _Dwrap(SqliteDB.insert, self, *args, **kwargs)
    
    def dUpdate(self, *args, **kwargs):
        return _Dwrap(SqliteDB.update, self, *args, **kwargs)
    
    def dDelete(self, *args, **kwargs):
        return _Dwrap(SqliteDB.delete, self, *args, **kwargs)

    def query(self, *args, **kwargs):
        return unIter(SqliteDB.query(self, *args, **kwargs))

    def select(self, *args, **kwargs):
        return unIter(SqliteDB.select(self, *args, **kwargs))


















