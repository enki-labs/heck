
module.exports = function () {

    return {

        /**
         * Display default task page.
         */
        index: function (req, res) {
            res.render('index.ejs', {});            
        }
        
    }
};

