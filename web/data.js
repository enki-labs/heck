
module.exports = function () {

    return {

        /**
         * Display default task page.
         */
        view: function (req, res) {
            res.render("data_view.ejs", {});            
        },

        process: function (req, res) {
            res.render("data_process.ejs", {});
        },

        search: function (req, res) {
            res.render("excel_search.ejs", {});
        }        
    }
};

