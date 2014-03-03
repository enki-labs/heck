
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

tauApp.controller("TaskActive", function ($scope, $http, $interval) {
   
    $scope.isEmptyObject = function (obj) { return $.isEmptyObject(obj); };
    $scope.isArray = function (obj) { return $.isArray(obj); }
 
    $scope.update = function () {
        var params = { params: { action: "active" } };
        $http.get("api/task", params).success(function(data) {
            $scope.active = data;
        });
    };

    $scope.update();
    var updatePromise = $interval($scope.update, 5000);
    $scope.$on("$destroy", function () { $interval.cancel(updatePromise); });
});

tauApp.controller("TaskStatus", function ($scope, $http, $interval) {

    $scope.init = function (filter) {
        $scope.filter = filter;
    };

    $scope.tasks = {};
    $scope.gridOptions = {};

    $scope.update = function () {
        if ($scope.filter) {
        var params = { params: { action: $scope.filter } };
        $http.get("api/task", params).success(function(data) {
            $scope.tasks = data;
        });
        } else { console.log("no filter"); }
    };

    $scope.gridHandler = function () {
        $scope.isEmptyObject = function (obj) { return $.isEmptyObject(obj); };
        $scope.statusLabel = function (status) {
            if (status == "SUCCESS") return "label-success";
            else if (status == "FAILURE") return "label-danger";
            else if (status == "START") return "label-primary";
            else if (status == "PENDING") return "label-default";
            else return "label-default";
        };
        $scope.gridOptions = { data: "tasks",
                 selectedItems: [],
                 enableSorting: false,
                 enableColumnResize: true,
                 enableRowSelection: false,
                 canSelectRows: false,
                 rowHeight: 30,
                 columnDefs: [{field: "node",
                               displayName: "Node",
                               cellTemplate: "<span class='cell-div'>{{row.entity.node}}</span>",
                               enableCellEdit: true,
                               width: "30%"
                              },
                              {field: "name",
                               displayName: "Task",
                               cellTemplate: "<span class='cell-div'>{{row.entity.name}}</span>",
                               enableCellEdit: true,
                               width: "30%"
                              },
                              {field: "time_start",
                               displayName: "Started",
                               width: "20%",
                               cellTemplate: "<div class='cell-div' cell-ticktime value='row.entity.time_start'></div>"}
                             ,{field: "",
                               displayName: "Progress",
                               width: "18%",
                               cellTemplate: "<span class='cell-div' ng-if='row.entity.status!=\"PROGRESS\"'><span class='label' ng-class='statusLabel(row.entity.status)'>{{row.entity.status}}</span></span><div class='progress progress-striped active' ng-if='row.entity.status==\"PROGRESS\"' style='margin-top: 5px; margin-left: 20px; margin-right: 20px;'><div style='position: absolute; margin-left: 70px; margin-top: 0px; color: rgba(60, 60, 60, 0.95); font-size: 85%;'>{{row.entity.per_second}}/sec (about {{row.entity.estimate}})</div><div class='progress-bar' role='progressbar' aria-valuemin='0' aria-valuemax='100' style='width: {{row.entity.progress}}%'></div></div>"}
                             ]
        };
        return $scope.gridOptions;
    };

    $scope.update();
    var updatePromise = $interval($scope.update, 5000);
    $scope.$on("$destroy", function () { $interval.cancel(updatePromise); });
});

tauApp.controller("TaskConfiguration", function ($scope, $http, $q) {
    $scope.items = [];
    $scope.gridOptions = {};
    $scope.showEditContent = false;
    $scope.editcontent = null;
    $scope.editentity = null;
    $scope.searchTags = {};

    /*var params = { params: { action: "list", item: "config" } };
    $http.get("api/task", params).success(function(data) {
        $scope.items = data;
    });*/

    $scope.editContent = function (row) {
        $scope.editcontent = $.extend(true, {}, row.entity.content);
        $scope.editentity = row.entity;
        $scope.showEditContent = true;
    };

    $scope.saveTags = function (scope, ok) {
        scope.$emit('ngGridEventEndCellEdit');
        if (ok) {
            $scope.saveConfigHandler(scope.$parent.row.entity);
        }
    };

    $scope.saveContent = function (scope, ok) {
        if (ok) {
            $scope.editentity.content = $scope.editcontent;
            $scope.saveConfigHandler($scope.editentity);
        }
        $scope.showEditContent = false;
    };

    $scope.addConfigHandler = function (clone) {
        if (clone) {
            if ($scope.gridOptions.selectedItems.length > 0) {
                var newItem = $.extend(true, {}, $scope.gridOptions.selectedItems[0]);
                newItem.id = null;
                newItem.last_modified = 0;
                $scope.items.splice(0, 0, newItem);
            }
        }
        else {
            $scope.items.splice(0, 0, {id: null, tags: {}, content: {}, last_modified: 0});
        }
    };

    $scope.searchTagsHandler = function () {
        var params = { params: { action: "list", item: "config", filter: JSON.stringify($scope.searchTags) } };
        $http.get("api/task", params).success(function(data) {
            $scope.items = data;
        });
    };

    $scope.deleteConfigHandler = function () {
        var params = { params: { action: "delete", item: "config", target: JSON.stringify($scope.gridOptions.selectedItems) } };
        $http.get("api/task", params).success(function(data) {
            $scope.gridOptions.selectedItems.forEach( function(rowItem) {
                var index = $scope.items.indexOf(rowItem);
                $scope.items.splice(index, 1);
            });
            $scope.gridOptions.selectAll(false);
        });
    };

    $scope.saveConfigHandler = function (entity) {
        var params = { params: { action: "save", item: "config", target: JSON.stringify([entity]) } };
        $http.get("api/task", params).success(function(data) {
            //$scope.searchTagsHandler();
        });
    };

    $scope.searchTags = {};
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


    $scope.gridHandler = function () {
        $scope.isEmptyObject = function (obj) { return $.isEmptyObject(obj); };
        $scope.gridOptions = { data: "items",
                 selectedItems: [],
                 enableSorting: false,
                 enableColumnResize: true,
                 enableRowSelection: true,
                 canSelectRows: true,
                 rowHeight: 40,
                 columnDefs: [{field: "tags", 
                               displayName: "Tags", 
                               cellTemplate: "<span class='small-text' ng-if='isEmptyObject(row.entity.tags)'>double click to edit</span><span class='label label-primary row-tag' ng-repeat='(name, value) in row.entity.tags'>{{name}}:{{value}}</span>",
                               enableCellEdit: true,
                               width: "40%",
                               editableCellTemplate: "<tags-input ng-model='row.entity.tags' ng-input='row.entity.tags' save='saveTags' custom-class='tag-edit' placeholder='...'></tags-input>"
                              }
                             ,{field: "content",
                               displayName: "Content",
                               width: "40%",
                               cellTemplate: "<span class='small-text' ng-if='isEmptyObject(row.entity.content)' ng-dblclick='editContent(row)'>double click to edit</span><span class='truncate small-text' ng-if='!isEmptyObject(row.entity.content)' ng-dblclick='editContent(row)'>{{row.entity.content}}</span>"}
                             ,{field: "last_modified",
                               displayName: "Last Modified",
                               width: "20%",
                               cellTemplate: "<div cell-ticktime class='small-text' value=row.entity.last_modified></div>"}
                             ]
        };
        return $scope.gridOptions;
    };
});

tauApp.controller("TaskProcess", function ($scope, $http) {

    $scope.searchName = "";
    $scope.searchProcessor = "";
    $scope.items = [];
    $scope.gridOptions = {};
    $scope.showJsonEdit = false;

    $scope.searchHandler = function () {
        var params = { params: { action: "list", item: "process", filter: JSON.stringify({name: $scope.searchName, processor: $scope.searchProcessor}) } };
        $http.get("api/task", params).success(function(data) {
            $scope.items = data;
        });
    };

    $scope.addProcessHandler = function (clone) {
        if (clone) {
            if ($scope.gridOptions.selectedItems.length > 0) {
                var newItem = $.extend(true, {}, $scope.gridOptions.selectedItems[0]);
                newItem.id = null;
                newItem.last_modified = 0;
                $scope.items.splice(0, 0, newItem);
            }
        }
        else {
            $scope.items.splice(0, 0, {id: null, name: "", processor: "", search: {}, output: {}, last_modified: 0});
        }
    };

    $scope.editJson = function (row, field) {
        $scope.jsonField = field;
        $scope.jsonValue = $.extend(true, {}, row.entity[field]);
        $scope.jsonEntity = row.entity;
        $scope.showJsonEdit = true;
    };

    $scope.saveValue = function (ok, scope, value) {
        scope.$emit('ngGridEventEndCellEdit');
        if (ok) {
            scope.$parent.row.entity[scope.$parent.col.field] = value;
            $scope.saveHandler(scope.$parent.row.entity);
        }
    };    

    $scope.saveJson = function (scope, ok) {
        if (ok) {
            $scope.jsonEntity[$scope.jsonField] = $scope.jsonValue;
            $scope.saveHandler($scope.jsonEntity);
        }
        $scope.showJsonEdit = false;
    };

    $scope.saveHandler = function (entity) {
        var params = { params: { action: "save", item: "process", target: JSON.stringify([entity]) } };
        $http.get("api/task", params).success(function(data) {
        });
    };

    $scope.gridHandler = function () {
        $scope.isEmptyObject = function (obj) { return $.isEmptyObject(obj); };
        $scope.gridOptions = { data: "items",
                 selectedItems: [],
                 enableSorting: false,
                 enableColumnResize: true,
                 enableRowSelection: true,
                 canSelectRows: true,
                 rowHeight: 40,
                 columnDefs: [{field: "name",
                               displayName: "Name",
                               cellTemplate: "<span class='small-text'>{{row.entity.name.length ? row.entity.name : 'double click to edit'}}</span>",
                               enableCellEdit: true,
                               width: "15%",
                               editableCellTemplate: "<div cell-input model='row.entity.name' save='saveValue'></div>"
                              }
                             ,{field: "processor",
                               displayName: "Processor",
                               cellTemplate: "<span class='small-text'>{{row.entity.processor.length ? row.entity.processor : 'double click to edit'}}</span>",
                               enableCellEdit: true,
                               width: "15%",
                               editableCellTemplate: "<div cell-input model='row.entity.processor' save='saveValue'></div>"
                              }
                             ,{field: "search",
                               displayName: "Search",
                               width: "30%",
                               cellTemplate: "<span class='small-text' ng-if='isEmptyObject(row.entity.search)' ng-dblclick='editJson(row,\"search\")'>double click to edit</span><span class='truncate small-text' ng-if='!isEmptyObject(row.entity.search)' ng-dblclick='editJson(row,\"search\")'>{{row.entity.search}}</span>"}
                             ,{field: "output",
                               displayName: "Output",
                               width: "30%",
                               cellTemplate: "<span class='small-text' ng-if='isEmptyObject(row.entity.output)' ng-dblclick='editJson(row,\"output\")'>double click to edit</span><span class='truncate small-text' ng-if='!isEmptyObject(row.entity.output)' ng-dblclick='editJson(row,\"output\")'>{{row.entity.output}}</span>"}
                             ,{field: "last_modified",
                               displayName: "Last Modified",
                               width: "10%",
                               cellTemplate: "<div cell-ticktime class='small-text' value=row.entity.last_modified></div>"}
                             ]
        };
        return $scope.gridOptions;
    };

});

tauApp.controller("SeriesSearch", function ($rootScope, $scope, $http, $q) {

    $scope.tags = {};

    $scope.search = function () {
        /*var tagSearch = {};
        $scope.tags.forEach(function(fullTag){
            var tagParts = fullTag.split(":");
            tagSearch[tagParts[0]] = tagParts[1];
        });*/
        $http.get("/api/find", {params: {tags: JSON.stringify($scope.tags)}}).success( function(data) {
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

tauApp.directive('cellInput', function () {
    return {
        restrict: 'A',
        template: '<div class="input-group cellinput" ng-blur="save(false, this, model)"><input ng-model="model"><span class="pull-right"><a class="tag-save" ng-click="save(true, this, model)"><i class="fa fa-save"> save</i><a class="tag-save" ng-click="save(false, this, model)"><i class="fa fa-undo"> undo</i></a></span></div>',
        scope: {
            model: '&',
            save: '='
        },
        transclude: true,
        replace: true
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

tauApp.directive('cellTicktime', function () {
    return {
        restrict: 'A',
        replace: true,
        transclude: true,
        scope: { value: '=' },
        template: "<div class='ngCellText'>{{formatValue}}</div>",
        link: function (scope, elem, attrs) {
            scope.$watch("value", function (newval) {
                if (newval == scope.formatValue) return;
                scope.formatValue = moment.fromTickTime(newval).format("YYYY-MM-DD HH:mm:ss.SSS");
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


