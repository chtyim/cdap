/*
 * Copyright © 2015 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

angular.module(PKG.name + '.feature.adapters')
  .controller('AdaptersDetailLogController', function($scope, AdapterDetail, $timeout) {

    $scope.loadingNext = true;
    var logsParams = {};
    angular.copy(AdapterDetail.logsParams, logsParams);
    logsParams.scope = $scope;

    AdapterDetail.logsApi.runs(logsParams)
      .$promise
      .then(function (runs) {
        if (runs.length === 0) { return; }

        logsParams.runId = runs[0].runid;

        console.info('Polling for prev logs');
        AdapterDetail.logsApi.pollNextLogs(logsParams)
          .$promise
          .then(function (logs) {
            $scope.logs = logs;
            if ($scope.logs.length >= 50) {
              console.info('Stopped polling for prev logs');
              AdapterDetail.logsApi.stopPollNextLogs(logsParams);
            }
            $scope.loadingNext = false;
          });
      });


    $scope.loadNextLogs = function () {
      if ($scope.loadingNext) {
        return;
      }
      console.info('Loading next set of logs');
      $scope.loadingNext = true;
      logsParams.fromOffset = $scope.logs[$scope.logs.length-1].offset;

      AdapterDetail.logsApi.nextLogs(logsParams)
        .$promise
        .then(function (res) {
          $scope.logs = _.uniq($scope.logs.concat(res));
          $scope.loadingNext = false;
        });
    };

    $scope.loadPrevLogs = function () {
      if ($scope.loadingPrev) {
        return;
      }
      console.info('Loading next set of logs');
      $scope.loadingPrev = true;
      logsParams.fromOffset = $scope.logs[0].offset;

      AdapterDetail.logsApi.prevLogs(logsParams)
        .$promise
        .then(function (res) {
          $scope.logs = _.uniq(res.concat($scope.logs));
          $scope.loadingPrev = false;

          $timeout(function() {
            var container = angular.element(document.querySelector('[infinite-scroll]'))[0];
            var logItem = angular.element(document.getElementById(logsParams.fromOffset))[0];
            container.scrollTop = logItem.offsetTop;
          });
        });
    };

  });
