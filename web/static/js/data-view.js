
var tauApp = angular.module("tauApp", ["ngTagsInput"]);

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
});

tauApp.controller("SeriesView", function ($scope, $http) {
    $scope.count;
    
    $scope.handler = function (chartDefault, series) {
        var url = "/api/data?id=" + $scope.instance.id;
        
        return chartDefault;
    };
});

tauApp.controller("SeriesSearch", function ($rootScope, $scope, $http, $q) {

    $scope.search = function () {
        var tagSearch = {};
        $scope.tags.forEach(function(fullTag){
            var tagParts = fullTag.split(":");
            tagSearch[tagParts[0]] = tagParts[1];
        });
        console.log(tagSearch);
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
            handler: '='
        },
        transclude:true,
        replace: true,

        link: function (scope, element, attrs) {
            var chartDefault = {
                chart: {
                    renderTo: element[0],
                    type: attrs.type || null,
                    height: attrs.height || null,
                    width: attrs.width || null,
                    zoomType: attrs.zoomtype || null
                },
                navigator : {
                    adaptToUpdatedData: false,
                    series: { dataGrouping: { enabled: false } }
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
                }       
            };
          
            chartDefault = scope.handler(chartDefault, scope.series);
            //Update when charts data changes
            scope.$watch(function() { return scope.series }, function(value) {
                if(!value) return;
                // We need deep copy in order to NOT override original chart object.
                // This allows us to override chart data member and still the keep
                // our original renderTo will be the same
                var newSettings = {};
                angular.extend(newSettings, chartDefault);//, scope.series);
                var chart = new Highcharts.StockChart(newSettings);
            });
        }
    };
});
