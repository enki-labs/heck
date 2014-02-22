
var tauApp = angular.module("tauApp", ["ngSanitize", "ngTagsInput", "ngGrid", "JSONedit"]);

tauApp.controller("SeriesSummary", function ($scope, $http) {
    $http.get("api/summary").success(function(data) {
        $scope.series = {"count": data.count, "first": moment.fromTickTime(data.min).format("YYYY-MM-DD"), "last": moment.fromTickTime(data.max).format("YYYY-MM-DD")};
    });
});

tauApp.controller("SeriesViews", function ($scope, $http) {
    $scope.series = [];

    $scope.$on("view-series", function (event, result) { 
        $scope.series.push(result); 
    });

    $scope.$on("view-series-remove", function (event, index) {
        $scope.series.splice(index, 1);
    });
});

tauApp.controller("SeriesView", function ($rootScope, $scope, $http, $timeout, $q) {

    $scope.state = {
        view: "plot",
        current: 0,
        window: 1000,
        shadow_window: 1000,
        date: 0,
        shadow_date: 0,
        refresh: false,
        data: null,
        summary: null
    };

    $scope.refresh = function () {
        $scope.state.refresh = true;
    };

    $scope.closeView = function ($index) {
        $rootScope.$broadcast("view-series-remove", $index);
    };

    $scope.start = function () {
        $scope.state.current = 0;
        $scope.refresh();
    };

    $scope.end = function () {
        $scope.state.current = Math.max(0, ($scope.state.summary.imax - $scope.state.window));
        $scope.refresh();
    };

    $scope.move = function (direction) {
        $scope.state.current = Math.max(0, Math.min(($scope.state.current + (direction * $scope.state.window)), $scope.state.summary.imax));
        $scope.refresh();
    };

    $scope.unzoom = function () {
        console.log("unzoom");
    };

    $scope.toggle = function () {
        if ($scope.state.view == "plot") { $scope.state.view = "grid"; }
        else { $scope.state.view = "plot"; }
    };

    var timer = false;
    $scope.$watch("state.window", function () {
        if (timer) { $timeout.cancel(timer); }
        timer = $timeout(function () {
            if ($scope.state.window != $scope.state.shadow_window)
            {
                $scope.state.shadow_window = $scope.state.window;
                $scope.refresh();
            }
            }, 500);
    });

    $scope.dateChange = function () {
        console.log("date changed " + $scope.state.date);
    };

    $scope.$watch("state.refresh", function () {
        if (!$scope.state.refresh) return;
        $scope.state.refresh = false;

        var format = $scope.instance.tags.format;
        if (format == "ohlc") { format = "ohlcv"; }

        if ($scope.state.summary == null)
        {
            var params = {params: { id: $scope.instance.id,
                                    format: format,
                                    resolution: 2000 
                                  }
                         };
            $http.get("/api/summary", params).success( function (data) {
                $scope.state.summary = data;
                $scope.state.refresh = true;
            });
        }
        else
        {
            var params = { params: { id: $scope.instance.id,
                                 start: $scope.state.current,
                                 end: Math.min($scope.state.summary.imax, ($scope.state.current + $scope.state.window)),
                                 format: format
                               }
                     };
            $http.get("/api/data", params).success( function (data) {
                $scope.state.data = data;
            });
        }
    });

    $scope.plotOhlc = function (renderTarget, attrs, series, updateFunction) {

        var options = {
            credits: { enabled: false },
            chart: {
                renderTo: renderTarget,
                type: "ohlc",
                height: attrs.height || null,
                width: attrs.width || null,
                zoomType: "x"
            },
            navigator : {
                adaptToUpdatedData: false,
                series: {
                    dataGrouping: { enabled: false },
                    data: $scope.state.summary.summary
                }
            },
            rangeSelector : {
                selected: 1,
                enabled: false
            },
            title: {
                text: attrs.title || null
            },
            tooltip: {
                valueDecimals: 6,
                xDateFormat: "%Y-%m-%d %H:%M:%S%L"
            },
            scrollbar : {
                enabled: false
            },
            series: [{  
                type: "ohlc",
                name: "Price",
                data : $scope.state.data.data,
                dataGrouping : { enabled: false }
            },
            {  
                type: "column",
                name: "Volume",
                data: $scope.state.data.volume,
                dataGrouping: {enabled: false},
                yAxis: 1
            }],
            xAxis: {  
                events: {setExtremes: $scope.setZoom},
                minRange: 1,
                plotBands: []
            },
            yAxis: [{  
                title: {text: "Price"},
                height: 300
            },
            {  
                title: {text: "Volume"},
                top: 300,
                height: 100,
                offset: 0
            }],
            plotOptions: {  
                ohlc: {turboThreshold: 10000},
                series: {marker: {enabled: true}},
                scatter: {tooltip: {pointFormat: "{point.name}"}}
            }
        };//options
        updateFunction(options);
    };


    $scope.plotTick = function (renderTarget, attrs, series, updateFunction) {

        var indata = [];

        $scope.state.data.data.forEach(function (tick) {
            if (tick[6] != "") {
                indata.push({x:tick[0], y:tick[1], marker: { radius: 2, fillColor: "red" }});
            } else { indata.push({x:tick[0], y:tick[1], marker: { enabled: false }}); }
        });

        var options = {
            credits: { enabled: false },
            chart: {
                renderTo: renderTarget,
                type: "line",
                height: attrs.height || null,
                width: attrs.width || null,
                zoomType: "x"
            },
            navigator : {
                adaptToUpdatedData: false,
                series: { 
                    dataGrouping: { enabled: false },
                    data: $scope.state.summary.summary
                }
            },
            rangeSelector : {
                selected: 1,
                enabled: false
            },
            title: {
                text: attrs.title || null
            },
            tooltip: {
                valueDecimals: 6,
                xDateFormat: "%Y-%m-%d %H:%M:%S%L"
            },
            scrollbar : {
                enabled: false
            },
            series: [{
                type: "line",
                name: "Price",
                data : indata,
                dataGrouping : { enabled: false }
            },
            {  
                type: "column",
                name: "Volume",
                data: $scope.state.data.volume,
                dataGrouping: {enabled: false},
                yAxis: 1
            }],
            xAxis: {
                events: {setExtremes: $scope.setZoom}, 
                minRange: 1, 
                plotBands: []
            },
            yAxis: [{
                title: {text: "Price"}, 
                height: 300
            }, 
            {
                title: {text: "Volume"}, 
                top: 300, 
                height: 100, 
                offset: 0
            }],
            plotOptions: {
                line: { turboThreshold: 10000 },
                series: {marker: {enabled: true}}, 
                scatter: {tooltip: {pointFormat: "{point.name}"}}
            } 
        };//options
        updateFunction(options);             
    };
    
    $scope.plotHandler = function (renderTarget, attrs, series, updateFunction) {
        if (series.tags.format == "tick")
        {
            return $scope.plotTick(renderTarget, attrs, series, updateFunction);
        }
        else
        {
            return $scope.plotOhlc(renderTarget, attrs, series, updateFunction);
        }
    };

    $scope.gridHandler = function (series) { //(renderTarget, attrs, series, updateFunction) {
        if (series.tags.format == "tick")
        {
            return { data: "state.data.data", 
                     enableSorting: false,
                     enableColumnResize: true,
                     rowHeight: 20,
                     columnDefs: [{field: "0", displayName: "Time", width: "10%", cellTemplate: "<div cell-datetime value=row.entity[0]></div>"}, 
                                  {field: "1", displayName: "Price", width: "5%"},
                                  {field: "2", displayName: "Volume", width: "5%"},
                                  {field: "3", displayName: "Event", cellTemplate: "<div cell-event value=row.entity[3]></div>"},
                                  {field: "4", displayName: "Qualifier", width: "35%"},
                                  {field: "5", displayName: "Acc. Volume", width: "5%"},
                                  {field: "6", displayName: "Filter Class", width: "20%"},
                                  {field: "7", displayName: "Filter Detail", width: "20%"}
                                 ]
                   };
        }
        else
        {
            return { data: "state.data.data",
                     enableSorting: false,
                     enableColumnResize: true,
                     rowHeight: 20,
                     columnDefs: [{field: "0", displayName: "Time", width: "25%", cellTemplate: "<div cell-datetime value=row.entity[0]></div>"},
                                  {field: "1", displayName: "Open", width: "15%"},
                                  {field: "2", displayName: "High", width: "15%"},
                                  {field: "3", displayName: "Low", width: "15%"},
                                  {field: "4", displayName: "Close", width: "15%"},
                                  {field: "5", displayName: "Volume", width: "15%"},
                                 ]
                   };
        }
    };

    $scope.refresh();

});

tauApp.controller("DataProcess", function ($scope) {

    $scope.view = null;

    $scope.show = function (view) {
        $scope.view = view;
    };

});

tauApp.controller("TaskActive", function ($scope, $http) {
    var params = { params: { action: "active" } };
    $http.get("api/task", params).success(function(data) {
        $scope.active = data;
    });
});

tauApp.controller("TaskConfiguration", function ($scope, $http) {
    $scope.items = [];
    $scope.showEditContent = false;
    $scope.editcontent = null;
    $scope.editentity = null;

    var params = { params: { action: "list", item: "config" } };
    $http.get("api/task", params).success(function(data) {
        $scope.items = data;
    });

    $scope.editContent = function (row) {
        $scope.editcontent = $.extend(true, {}, row.entity.content);
        $scope.editentity = row.entity;
        $scope.showEditContent = true;
    };

    $scope.saveTags = function (scope, ok) {
        scope.$emit('ngGridEventEndCellEdit');
    };

    $scope.saveContent = function (scope, ok) {
        if (ok) {
            $scope.editentity.content = $scope.editcontent;
        }
        $scope.showEditContent = false;
    };

    $scope.gridHandler = function () {
        return { data: "items",
                 enableSorting: false,
                 enableColumnResize: true,
                 enableRowSelection: false,
                 canSelectRows: false,
                 rowHeight: 40,
                 columnDefs: [{field: "tags", 
                               displayName: "Tags", 
                               cellTemplate: "<span class='label label-primary row-tag' ng-repeat='(name, value) in row.entity.tags'>{{name}}:{{value}}</span>",
                               enableCellEdit: true,
                               editableCellTemplate: "<tags-input ng-model='row.entity.tags' ng-input='row.entity.tags' save='saveTags' custom-class='tag-edit' placeholder='...'></tags-input>"
                              }
                             ,{field: "content",
                               displayName: "Content",
                               cellTemplate: "<span class='truncate small-text' ng-dblclick='editContent(row)'>{{row.entity.content}}</span>"}
                             ]
               };
    };
});

tauApp.controller("TaskProcess", function ($scope, $http) {
    var params = { params: { action: "list", item: "process" } };
    $http.get("api/task", params).success(function(data) {
        $scope.items = data;
    });
});

tauApp.controller("SeriesSearch", function ($rootScope, $scope, $http, $q) {

    $scope.search = function () {
        var tagSearch = {};
        $scope.tags.forEach(function(fullTag){
            var tagParts = fullTag.split(":");
            tagSearch[tagParts[0]] = tagParts[1];
        });
        $http.get("/api/find", {params: {tags: JSON.stringify(tagSearch)}}).success( function(data) {
            $scope.search_result = data;
        });
    };

    $scope.highlight = function (result, state) {
        result.ng_selected = state;
    };

    $scope.view = function (result) {
        $rootScope.$broadcast("view-series", result);
    };
    
    $http.get("api/tag").success(function(data) {
        $scope.tag_autocomplete = function ($query) {
            if ($query.indexOf(":") == -1)
            {
                var deferred = $q.defer();
                deferred.resolve(data.tags);
                return deferred.promise;
            }
            else
            {
                var deferred = $q.defer();
                var name = $query.substr(0, $query.indexOf(":"));
                var part = $query.substr($query.indexOf(":")+1, $query.length-1);
                $http.get("api/tag?target=" + name + "&part=" + part).success(function(data) {
                    var tags = [];
                    data.values.forEach(function(value){tags.push(name + ":" + value);});
                    deferred.resolve(tags);
                });
                return deferred.promise;
            }
        };
    });
});

tauApp.directive('stock', function () {
    return {
        restrict: 'E',
        template: '<div></div>',
        scope: {
            series: '=',
            handler: '=',
            watcher: '='
        },
        transclude:true,
        replace: true,

        link: function (scope, element, attrs) {

            Highcharts.setOptions({ global : { useUTC : true } });

            scope.$watch( function () { return scope.watcher }, function (value) {
                if (!value) return;

                scope.handler(element[0], attrs, scope.series, function (options) {
                    var chart = new Highcharts.StockChart(options);
                });                
            });
        }
    };
});

tauApp.directive('spark', function () {
    return {
        restrict: 'ECMA',
        template: '<img class="search-result-spark" ng-src="/api/spark?width={{sparkwidth}}&height={{sparkheight}}&id={{series.id}}&format={{format}}&resolution={{resolution}}" alt="sparkline" />',
        scope: {
            series: '=',
            sparkwidth: '@',
            sparkheight: '@',
            resolution: '@'            
        },
        transclude: false,
        replace: true,

        link: function (scope, element, attrs) {
            scope.$watch("series", function (newval) {
                if (typeof scope.format === "undefined")
                {
                    if (scope.series.tags.format == "ohlc") scope.format = "ohlcv";
                    else scope.format = scope.series.tags.format;
                }
            });        
        }
    };
});

tauApp.directive('grid', function () {
    return {
        restrict: 'E',
        template: '<div></div>',
        scope: {
            series: '=',
            handler: '=',
            watcher: '='
        },
        transclude:true,
        replace: true,

        link: function (scope, element, attrs) {

            scope.$watch( function () { return scope.watcher }, function (value) {
                if (!value) return;

                scope.handler(element[0], attrs, scope.series, function (data, columns, options) {
                    var grid = new Slick.Grid(element[0], data, columns, options);
                    grid.setSelectionModel(new Slick.RowSelectionModel());
                    grid.invalidate();
                    scope.watcher = false;
                });
            });

            scope.watcher = true;
        }
    };
});

tauApp.directive('cellDatetime', function () {
    return {
        restrict: 'A',
        replace: true,
        transclude: true,
        scope: { value: '=' },
        template: "<div class='ngCellText'>{{formatValue}}</div>",
        link: function (scope, elem, attrs) {
            scope.$watch("value", function (newval) {
                if (newval == scope.formatValue) return;
                scope.formatValue = Highcharts.dateFormat("%Y-%m-%d %H:%M:%S.%L", newval);
            });
        }
    };
});

tauApp.directive('cellEvent', function () {
    return {
        restrict: 'A',
        replace: true,
        transclude: true,
        scope: { value: '=' },
        template: "<div class='ngCellText'>{{formatValue}}</div>",
        link: function (scope, elem, attrs) {
            scope.$watch("value", function (newval) {
                if (newval == scope.formatValue) return;
                if (newval == 0) { scope.formatValue = "trade"; }
                else if (newval == 100) { scope.formatValue = "settle"; }
                else if (newval == 200) { scope.formatValue = "open interest"; }
                else { scope.formatValue = "unknown" }
            });
        }
    };
});

