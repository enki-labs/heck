#!/usr/bin/env node
"use strict";

var ArgumentParser = require("argparse").ArgumentParser;
var url = require("url");
var passport = require("passport");
var LocalStrategy = require("passport-local").Strategy;
var ensureLoggedIn = require('connect-ensure-login').ensureLoggedIn;

var parser = new ArgumentParser({
    version: "2013.06.16",
    addHelp: true,
    description: "taustack"
});

parser.addArgument(
    [ "-b", "--backend"],
    { help: "Heck backend API eg: http://localhost:98754321" }
);

parser.addArgument(
    [ "-p", "--port"],
    { help: "Port eg: 3000" }
);

passport.use( new LocalStrategy(
    function(username, password, done) {
        if (username == "beta" && password == "beta")
        {
            return done(null, {id: 1, username: "beta", password: "beta", email: "beta@mail"});
        }
        else
        {
            return done(null, false);
        }
    })
);

passport.serializeUser(function (user, done){
    done(null, user.id);
});
passport.deserializeUser(function (id, done){
    done(null, {id:1});
});

var args = parser.parseArgs();
var express = require("express");
var data = require("./data");
var app = module.exports = express();

app.configure(function() {
    app.set("views", __dirname + "/view");
    app.set("view engine", "ejs");
    app.set("view options", {layout: false});
    app.use(express.bodyParser());
    app.use(express.methodOverride());
    app.use(express.logger("dev"));
    app.use(express.cookieParser());
    app.use(express.session({secret: "aabbcc"}));
    //app.use(express.cookieSession({secret: "aabbcc"}));
    app.use(passport.initialize());
    app.use(passport.session());

    app.use(app.router);
    app.use(express.static(__dirname + "/static"));
});

app.post("/login", 
passport.authenticate("local"),
function (req, res) { res.redirect("/"); });

app.get("/excel-search", data().search);

app.get("/", ensureLoggedIn("index.html"), data().view);

app.get("/data-view", ensureLoggedIn("index.html"), data().view);

app.get("/data-process", ensureLoggedIn("index.html"), data().process);
    
var request = require("request");
//app.get(/^\/api\/.*/, ensureLoggedIn("index.html"), function (req, res)
app.get(/^\/api\/.*/, function (req, res)
{
    request(url.resolve(args.backend, req.url)).pipe(res);
});

app.post(/^\/api\/.*/, function (req, res)
{
    req.pipe(request.post(url.resolve(args.backend, req.url))).pipe(res);
    //request.post({url: url.resolve(args.backend, req.url), headers: req.headers, body: req.body}).pipe(res);
});

// Server
app.listen(3000, function(){
  console.log("taustack running");
});

