/*jslint node: true */
"use strict";

var mongo           = require("mongodb"),
    u               = require("underscore"),
    q               = require("q"),
    config          = require("./driver.json"),
    defaults        = require("./defaults.json");

function premove(check) {
    var iargs = arguments;
    if (!check) {
        throw new Error("indiscriminate 'remove' not allowed");
    }
    return function (coll) {
        return q.npost(coll, "remove", iargs);
    };
}


function op(pobj, name) {
    var pf = pobj
        .then(function (obj) {
            return obj[name].bind(obj);
        });
    return function () {
        var iargs, d;
        d = q.defer();
        iargs = arguments;
        pf.then(function (f) {
            Array.prototype.push.call(iargs, d.makeNodeResolver());
            f.apply(null, iargs);
        });
        return d.promise;
    };
}

/**
 * find is weird op, returns a cursor from the function but also accepts a callback. the cb only tells us if there is an error.
 */
function findOp(pobj, name) {
    var pf = pobj
        .then(function (obj) {
            return obj[name].bind(obj);
        });
    return function (query, options) {
        return pf.then(function (f) {
            var cursor = f(query, options || {});
            return cursor;
        });
    };
}


/** Wrap collection with promise-based methods.
 * 
 * @param pcoll promise of a MongoDB collection.
 * 
 * @returns object providing promise-based versions of MongoDB ops.
 */
function wrapColl(pcoll) {

    var ops     = {},
        wrapper;

    config.opNames.forEach(function (name) {
        if (name === "find") {
            ops[name] = findOp(pcoll, name);
        } else {
            ops[name] = op(pcoll, name);
        }
    });

    function safeRemove(select, options) {
        if (!select) {
            throw new Error("selection required to remove");
        }
        options = options || {single: true };
        return ops.remove(select, options);
    }

    function adaptUpdate(selector, document, options) {
        if (selector.document) {
            document = selector.document;
            selector = selector.selector || {};
        }
        return ops.update(selector, document, options);
    }

    function arrayFind(query, options) {
        var d = q.defer();
        ops.find(query, options)
            .then(function (cursor) {
                cursor.toArray(d.makeNodeResolver());
            });
        return d.promise;
    }

    
    wrapper =  {
        insert: ops.insert,
        insertWith: function (options) {
            return function (docs) {
                return ops.insert(docs, options);
            };
        },
        remove: safeRemove,
        removeWith: function (options) {
            return function (select) {
                return safeRemove(select, options);
            };
        },
        update: adaptUpdate,
        updateWith: function (options) {
            return function (selector, document) {
                return adaptUpdate(selector, document, options);
            };
        },
        find: arrayFind,
        findWith: function (options) {
            return function (query) {
                return arrayFind(query, options);
            };
        },
        findAndModify: ops.findAndModify,
        findAndRemove: ops.findAndRemove,
        ensureIndex: ops.ensureIndex,
        findOne: ops.findOne,

        /*
         * unsafe is just all the operations we haven't reviewed or spun our own way.
         */
        unsafe: ops
    };

    wrapper.index = function (spec, options) {

        pcoll = pcoll.then(function (coll) {
            var d = q.defer();

            coll.ensureIndex(spec, options, function(err) {
                if (err) {
                    d.reject(err);
                } else {
                    d.resolve(coll);
                }
            });
            return d.promise;
        });
        return wrapper;
    };
    
    return wrapper;
    
}

/**
 * Connect to a mongo server.
 * 
 * @param connstr required connection string (see mongodb.MongoClient.connect)
 * 
 * @param options optional connection options  (see mongodb.MongoClient.connect)
 * 
 * @returns promise to connect. The promise value is a mongodb.Db object or something like it.
 * 
 */
function dbstart(connstr, options) {
    return q.ninvoke(mongo.MongoClient, "connect", connstr, options);
}

/**
 * Disconnect from a mongo server.
 * 
 * @param db mongodb.Db object to close
 * 
 * @returns promise to close the connection. The promise value is null.
 */
function dbstop(db) {
    return q.ninvoke(db, "close")
        .thenResolve(null);
}

/**
 * Attach to a collection.
 * 
 * @param db mongodb.Db object to manipulate
 * 
 * @param name name of collection
 * 
 * @param options attachment options
 * 
 * @returns promise to attach. The promise value is a mongodb.Collection object.
 */
function dbcoll(db, name, options) {
    return q.ninvoke(db, "collection", name, options);
}

/**
 * 
 * @param connstr
 *            connection string, see mongodb driver "connect" documentation.
 * 
 * @param options
 *            options for connection, see mongodb driver "connect"
 *            documentation.
 *            
 * @returns connection object
 */
function connect(connstr, options) {

    var conn = dbstart(connstr || defaults.connectionString, options || defaults.connectionOptions || {});

    function collection(name, options) {

        function mcoll(db) {
            return dbcoll(db, name, options || defaults.collectionOptions || {});
        }

        return wrapColl(conn.then(mcoll));

    }

    function shutdown() {
        return conn
            .then(dbstop);
    }

    return {
        collection: collection,
        shutdown: shutdown
    };
}

exports.connect = connect;

