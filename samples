/*jslint node: true */
"use strict";

var mongoq  = require("./index"),
    q       = require("q"),
    u       = require("underscore"),
    test,
    conn;

function log(v) {
    console.log(JSON.stringify(v, null, 4));
    return v;
}

conn = mongoq.connect();
test = conn.collection("spike");

q({one: 1, two: 2, three: "III"})
    .then(test.insert)
    .then(u.first)
    .then(log)
    .fail(function (o) {
        console.log("whoops.");
        return log(o);
    })
    .fin(function () {
        conn.shutdown().done();
        return null;
    })
    .done();











