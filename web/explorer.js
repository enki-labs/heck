
var json2csv = require('json2csv');

module.exports = function () {

    return {

        /**
         * Display default task page.
         */
        index: function (req, res) {
            res.render('explorer.ejs', { locals : { title: 'working' } });            
        },
        
        /**
         * Detail.
         */
        detail: function (req, res) {
            /*var searchTags = JSON.parse(req.body.content);
            zootClient.findTasks(searchTags, function (tasks) {
                res.send(JSON.stringify(tasks));
            });*/
        },

        /**
         * Retrieve a list of tasks.
         */
        find: function (req, res) {/*
            this.request = JSON.parse(req.body.content);
            this.res = res;            
            persist.find(this.request.find, this.request.limit).toArray(function(err, docs) {
                console.log(docs);
                this.res.send(JSON.stringify(docs));
            }.bind(this));*/
        },

        /**
         * Add/modify a task.
         */
        save: function (req, res) {

            /*this.res = res;
            this.request = JSON.parse(req.body.content);
            this.currentId = this.request._id == '' ? new ObjectID() : new ObjectID(this.request._id);
            this.request._id = currentId;
            console.log(this.request);
            persist.update({_id: this.currentId}, this.request, {upsert:true, safe:true}, function(err) {
                if (err)
                {
                    console.warn(err.message);
                    this.res.send(JSON.stringify(err));
                }
                else
                {
                    console.log("add zoot");
                    zootClient.addTask(this.request.detail, this.request.searchTags, this.request.addTags, this.request.removeTags, true, function () {
                        console.log("zooted");
                        this.res.send(JSON.stringify(this.request));
                    }.bind(this));
                }
            }.bind(this));*/
        },

        /**
         * Delete an existing filter task.
         */
        delete: function (req, res) {
            /*console.log("/filter/delete");
            persist.FilterTask.remove(  {'_id' : mongoose.Types.ObjectId(req.body._id)}, 
                                        function (err, count) { res.send({}); }  
            );*/
        }
    }
};

