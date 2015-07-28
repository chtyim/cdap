angular.module(PKG.name + '.feature.adapters')
  .controller('CanvasController', function (myAdapterApi, MyPlumbService, $bootstrapModal, $state, $scope, $alert, myHelpers, CanvasFactory, MyPlumbFactory, $modalStack) {
    this.nodes = [];

    if ($scope.AdapterCreateController.data) {
      setNodesAndConnectionsFromDraft.call(this, $scope.AdapterCreateController.data);
    }

    this.pluginTypes = [
      {
        name: 'source',
        icon: 'icon-ETLsources',
        open: false
      },
      {
        name: 'transform',
        icon: 'icon-ETLtransforms',
        open: false
      },
      {
        name: 'sink',
        icon: 'icon-ETLsinks',
        open: false
      }
    ];

    this.canvasOperations = [
      {
        name: 'Publish',
        icon: 'fa fa-cloud-upload'
      },
      {
        name: 'Save Draft',
        icon: 'fa fa-save'
      },
      {
        name: 'Config',
        icon: 'fa fa-eye'
      },
      {
        name: 'Settings',
        icon: 'fa fa-sliders'
      }
    ];

    this.onCanvasOperationsClicked = function(group) {
      var config;
      switch(group.name) {
        case 'Config':
          config = angular.copy(MyPlumbService.getConfigForBackend());
          modalInstance = $bootstrapModal.open({
            templateUrl: '/assets/features/adapters/templates/create/viewconfig.html',
            size: 'lg',
            keyboard: true,
            controller: ['$scope', 'config', function($scope, config) {
              $scope.config = JSON.stringify(config);
            }],
            resolve: {
              config: function() {
                return config;
              }
            }
          });
          break;
        case 'Publish':
          MyPlumbService
            .save()
            .then(
              function sucess(adapter) {
                $alert({
                  type: 'success',
                  content: adapter + ' successfully published.'
                });
                $state.go('apps.list');
              },
              function error(errorObj) {
                console.error('ERROR!: ', errorObj);
              }.bind(this)
            );
          break;
        case 'Settings':
          $bootstrapModal.open({
            templateUrl: '/assets/features/adapters/templates/create/settings.html',
            size: 'lg',
            keyboard: true,
            controller: ['$scope', 'metadata', 'EventPipe', function($scope, metadata, EventPipe) {
              $scope.metadata = metadata
              var metadataCopy = angular.copy(metadata);
              $scope.reset = function() {
                $scope.metadata.template.schedule.cron = metadataCopy.template.schedule.cron;
                $scope.metadata.template.instance = metadataCopy.template.instance;
                EventPipe.emit('plugin.reset');
              };
            }],
            resolve: {
              'metadata': function() {
                return MyPlumbService.metadata;
              }
            }
          });
          break;
        case 'Save Draft':
          MyPlumbService
            .saveAsDraft()
            .then(
              function success() {
                $alert({
                  type: 'success',
                  content: MyPlumbService.metadata.name + ' successfully saved as draft.'
                });
                $state.go('adapters.list');
              },
              function error(message) {
                $alert({
                  type: 'danger',
                  content: message
                });
              }
            )
      }
    };

    this.plugins= {
      items: []
    };

    this.onPluginTypesClicked = function(group) {
      var prom;
      switch(group.name) {
        case 'source':
          prom = myAdapterApi.fetchSources({ adapterType: MyPlumbService.metadata.template.type }).$promise;
          break;
        case 'transform':
          prom = myAdapterApi.fetchTransforms({ adapterType: MyPlumbService.metadata.template.type }).$promise;
          break;
        case 'sink':
          prom = myAdapterApi.fetchSinks({ adapterType: MyPlumbService.metadata.template.type }).$promise;
          break;
      }
      prom.then(function(res) {
        this.plugins.items = [];
        res.forEach(function(plugin) {
          this.plugins.items.push(
            angular.extend(
              {
                type: group.name,
                icon: MyPlumbFactory.getIcon(plugin.name)
              },
              plugin
            )
          );
        }.bind(this));
      }.bind(this));
    };

    this.onPluginItemClicked = function(event, item) {
      if (item.type === 'source' && this.pluginTypes[0].error) {
        delete this.pluginTypes[0].error;
      } else if (item.type === 'sink' && this.pluginTypes[2].error) {
        delete this.pluginTypes[2].error;
      }

      // TODO: Better UUID?
      var id = item.name + '-' + item.type + '-' + Date.now();
      event.stopPropagation();
      var config = {
        id: id,
        name: item.name,
        icon: item.icon,
        description: item.description,
        type: item.type
      };
      MyPlumbService.addNodes(config, config.type);
    };

    function errorNotification(errors) {
      angular.forEach(this.pluginTypes, function (type) {
        if (errors[type.name]) {
          type.error = errors[type.name];
        }
      });
    }

    MyPlumbService.errorCallback(errorNotification.bind(this));

    function setNodesAndConnectionsFromDraft(data) {
      var ui = data.ui;
      var config = data.config;
      var nodes;
      // Purely for feeding my-plumb to draw the diagram
      // if I already have the nodes and connections
      if (ui && ui.nodes) {
        nodes = ui.nodes;
        angular.forEach(nodes, function(value) {
          this.nodes.push(value);
        }.bind(this));
      } else {
        this.nodes = CanvasFactory.getNodes(config);
      }
      this.nodes.forEach(function(node) {
        MyPlumbService.addNodes(node, node.type);
      });

      if (ui && ui.connections) {
        MyPlumbService.connections = ui.connections;
      } else {
        MyPlumbService.connections = CanvasFactory.getConnectionsBasedOnNodes(this.nodes);
      }

      var config = CanvasFactory.extractMetadataFromDraft(data.config, data);

      MyPlumbService.metadata.name = config.name;
      MyPlumbService.metadata.description = config.description;
      MyPlumbService.metadata.template = config.template;
    }

    $scope.$on('$destroy', function() {
      $modalStack.dismissAll();
    });

  });
