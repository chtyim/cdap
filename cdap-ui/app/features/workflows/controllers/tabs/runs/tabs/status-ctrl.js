// FIXME: Needs to be re-thought.
var runparams = {},
    params;

class WorkflowsRunsStatusController {
  constructor($state, $scope, myWorkFlowApi, $filter, $alert, GraphHelpers, MyDataSource, myMapreduceApi, mySparkApi) {
    this.dataSrc = new MyDataSource($scope);
    this.$state = $state;
    this.$scope = $scope;
    this.myWorkFlowApi = myWorkFlowApi;
    this.$alert = $alert;
    this.GraphHelpers = GraphHelpers;
    this.myMapreduceApi = myMapreduceApi;
    this.mySparkApi = mySparkApi;
    this.$filter = $filter;
    this.runsCtrl = $scope.RunsController;
    this.onChangeFlag = 1;
    this.showSidepanel = false;
    this.activeTab = 0;

    this.data = {
      metrics: {},
      current: {},
    };

    params = {
      namespace: this.$state.params.namespace,
      appId: this.$state.params.appId,
      workflowId: this.$state.params.programId,
      scope: this.$scope
    };

    this.data = {
      current: {},
      metrics: {}
    };

    this.myWorkFlowApi.get(params)
      .$promise
      .then( res => {
        var edges = [],
            nodes = [],
            nodesFromBackend = angular.copy(res.nodes);

        this.GraphHelpers.addStartAndEnd(nodesFromBackend);
        this.GraphHelpers.expandNodes(nodesFromBackend, nodes);
        this.GraphHelpers.convertNodesToEdges(angular.copy(nodes), edges);

        nodes = nodes.map( item => {
          return angular.extend({
            name: item.program.programName + item.nodeId,
            type: item.nodeType
          }, item);
        });

        this.data['nodes'] = nodes;
        this.data['edges'] = edges;
        this.onChangeFlag += 1;
        var programs = [];
        angular.forEach(res.nodes, value => programs.push(value.program));

        this.actions = programs;

        // This needs to be rethought.
        this.pollNodes();

      });
  }

  // Need to make sure that the list of nodes is already generated
  pollNodes() {

    if (this.runsCtrl.runs.length === 0) {
      return;
    }
    runparams = angular.extend(
      {
        runId: this.runsCtrl.runs.selected.runid
      },
      params
    );

    this.myWorkFlowApi
      .pollRunDetailOften(runparams)
      .$promise
      .then( response => {
        this.runStatus = response.status;

        var pastNodes = Object.keys(response.properties);
        this.runsCtrl.runs.selected.properties = response.properties;

        var activeNodes = this.$filter('filter')(this.data.nodes , node => pastNodes.indexOf(node.nodeId) !== -1);

        angular.forEach(activeNodes, node => {
          var runid = response.properties[node.nodeId];
          var mapreduceParams;

          if (node.program.programType === 'MAPREDUCE') {
            mapreduceParams = {
              namespace: this.$state.params.namespace,
              appId: this.$state.params.appId,
              mapreduceId: node.program.programName,
              runId: runid,
              scope: this.$scope
            };
            this.myMapreduceApi.runDetail(mapreduceParams)
              .$promise
              .then( result => {
                this.data.current[node.name] = result.status;
                this.onChangeFlag += 1;
              });
          } else if (node.program.programType === 'SPARK') {

            var sparkParams = {
              namespace: this.$state.params.namespace,
              appId: this.$state.params.appId,
              sparkId: node.program.programName,
              runId: runid,
              scope: this.$scope
            };

            this.mySparkApi.runDetail(sparkParams)
              .$promise
              .then( (result) => {
                this.data.current[node.name] = result.status;
                this.onChangeFlag += 1;
              });
          }
        });

        if (['STOPPED', 'KILLED', 'COMPLETED'].indexOf(this.runStatus) !== -1) {
          this.myWorkFlowApi.stopPollRunDetail(runparams);
        }

      });

  }

  workflowProgramClick(instance) {
    if (['START', 'END'].indexOf(instance.type) > -1 ) {
      return;
    }
    if (this.runsCtrl.runs.length) {
      let stateParams = {
        programId: instance.program.programName,
        runid: this.runsCtrl.runs.selected.properties[instance.nodeId],
        sourceId: this.$state.params.programId,
        sourceRunId: this.$scope.RunsController.runs.selected.runid
      };
      if (instance.program.programType === 'MAPREDUCE' &&
         this.runsCtrl.runs.selected.properties[instance.nodeId]
        ) {
        stateParams.destinationType = 'Mapreduce';
        this.$state.go('mapreduce.detail.runs.run', stateParams);

      } else if (instance.program.programType === 'SPARK' &&
                this.runsCtrl.runs.selected.properties[instance.nodeId]
               ) {

        stateParams.destinationType = 'Spark';
        this.$state.go('spark.detail.runs.run', stateParams);
      }
    }
  }



  workflowTokenClick(node) {
    let tokenparams = angular.extend(
      {
        runId: this.runsCtrl.runs.selected.runid,
        nodeId: node.nodeId
      },
      params
    );

    this.selectedNode = node.nodeId;

    this.myWorkFlowApi.getUserNodeToken(tokenparams)
      .$promise
      .then (res => {
        delete res.$promise;
        delete res.$resolved;

        this.usertokens = [];
        angular.forEach(res, (value, key) => {
          console.info('test', value, key);
          this.usertokens.push({
            key: key,
            value: value
          });
        });

        this.showSidepanel = true;

      });

    this.myWorkFlowApi.getSystemNodeToken(tokenparams)
      .$promise
      .then (res => {
        delete res.$promise;
        delete res.$resolved;

        this.systemtokens = [];
        angular.forEach(res, (value, key) => {
          this.systemtokens.push({
            key: key,
            value: value
          });
        });

      });

  }

  closeSidepanel() {
    this.showSidepanel = false;
  }

}

WorkflowsRunsStatusController.$inject = ['$state', '$scope', 'myWorkFlowApi', '$filter', '$alert', 'GraphHelpers', 'MyDataSource', 'myMapreduceApi', 'mySparkApi'];
angular.module(`${PKG.name}.feature.workflows`)
  .controller('WorkflowsRunsStatusController', WorkflowsRunsStatusController);
