$.getScript("//cdnjs.cloudflare.com/ajax/libs/moment.js/2.5.1/moment.min.js", function () {
    moment.fromTickTime = function (ticktime) {
        var mo = moment(ticktime / 1000000);
        mo.tickTime = ticktime;
        return mo;        
    };
});
