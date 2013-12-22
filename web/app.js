#!/usr/bin/env node
'use strict';

var ArgumentParser = require('argparse').ArgumentParser;
var url = require('url');

var parser = new ArgumentParser({
    version: '2013.06.16',
    addHelp:true,
    description: 'TS Toolkit app'
});

parser.addArgument(
    [ '-b', '--backend'],
    { help: 'Heck backend API eg: http://localhost:98754321' }
);

var args = parser.parseArgs();
var express = require('express');
//var task = require('./task');
var explorer = require('./explorer');
//var status = require('./status');

// Routes

function verifyAuth (req, res, route)
{
    route();
    /*
    if (!req.headers['user-agent'].match(/Chrome/g))
    {
        res.redirect('/chrome');
    }
    else
    {
        if (!req.session || !req.session.user)
        {
            console.log('USER NOT AUTHD');
            res.redirect('/login');
        }
        else
        {
            route();
        }
    }*/
}

var app = module.exports = express();
app.configure(function() {
    app.set('views', __dirname + '/views');
    app.set('view engine', 'ejs');
    app.set('view options', {layout: false});
    app.use(express.bodyParser());
    app.use(express.methodOverride());
    app.use(app.router);
    app.use(express.static(__dirname + '/static'));
    app.use(express.static(__dirname + '/public'));
});

//app.get('/task', verifyAuth, task(taskDb, logDb, nodeDb, zClient).index);
app.get('/explorer', verifyAuth, explorer().index);
//app.post('/task/find', verifyAuth, task(taskDb, logDb, nodeDb, zClient).find);
//app.post('/task/detail', verifyAuth, task(taskDb, logDb, nodeDb, zClient).detail);
//app.post('/task/save', verifyAuth, task(taskDb, logDb, nodeDb, zClient).save);
//app.post('/task/update', verifyAuth, task(taskDb, logDb, nodeDb, zClient).update);
//app.post('/task/delete', verifyAuth, task(taskDb, logDb, nodeDb, zClient).delete);
//app.get('/task/queue', verifyAuth, task(taskDb, logDb, nodeDb, zClient).queue);
//app.post('/task/submit', verifyAuth, task(taskDb, logDb, nodeDb, zClient).submit);
//app.post('/task/define', verifyAuth, task(taskDb, logDb, nodeDb, zClient).define);
//app.get('/task/define', verifyAuth, task(taskDb, logDb, nodeDb, zClient).define);
//app.get('/status', verifyAuth, status(logDb, zClient).index);
//app.post('/status/find', verifyAuth, status(logDb, zClient).find);
    
var request = require('request');
//app.get(/^\/api\/^(.*)$/, verifyAuth, function (req, res)
app.get(/^\/api\/.*/, verifyAuth, function (req, res)
{
    request(url.resolve(args.backend, req.url)).pipe(res);
});

 
// Server
app.listen(3000, function(){
  console.log("express-bootstrap app running");
});

