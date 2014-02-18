#!/usr/bin/env node
'use strict';

var ArgumentParser = require('argparse').ArgumentParser;
var url = require('url');

var parser = new ArgumentParser({
    version: '2013.06.16',
    addHelp:true,
    description: 'taustack'
});

parser.addArgument(
    [ '-b', '--backend'],
    { help: 'Heck backend API eg: http://localhost:98754321' }
);

parser.addArgument(
    [ '-p', '--port'],
    { help: 'Port eg: 3000' }
);

var args = parser.parseArgs();
var express = require('express');
var index = require('./index');
var app = module.exports = express();

app.configure(function() {
    app.set('views', __dirname + '/view');
    app.set('view engine', 'ejs');
    app.set('view options', {layout: false});
    app.use(express.bodyParser());
    app.use(express.methodOverride());
    app.use(app.router);
    app.use(express.static(__dirname + '/static'));
});

app.get('/', index().index);
    
var request = require('request');
app.get(/^\/api\/.*/, function (req, res)
{
    request(url.resolve(args.backend, req.url)).pipe(res);
});

 
// Server
app.listen(3000, function(){
  console.log("taustack running");
});

