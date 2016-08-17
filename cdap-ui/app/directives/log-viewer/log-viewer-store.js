/*
 * Copyright © 2016 Cask Data, Inc.
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

var LogViewerStore = (LOGVIEWERSTORE_ACTIONS, Redux, ReduxThunk) => {
  const startTime = (state = new Date(), action = {}) => {
    switch(action.type) {
      case LOGVIEWERSTORE_ACTIONS.START_TIME:
        if(!action.payload.startTime) {
          return state;
        }
        return action.payload.startTime;
      case LOGVIEWERSTORE_ACTIONS.RESET:
        return new Date();
      default:
        return state;
    }
  };

  //Scroll Position Reducer
  const scrollPosition = (state = Date.now(), action = {}) => {
    switch(action.type) {
      case LOGVIEWERSTORE_ACTIONS.SCROLL_POSITION:
        if(!action.payload.scrollPosition) {
          return state;
        }
        return action.payload.scrollPosition;
      case LOGVIEWERSTORE_ACTIONS.RESET:
        return Date.now();
      default:
        return state;
    }
  };

  //Scroll Position Reducer
  const fullScreen = (state = false, action = {}) => {
    switch(action.type) {
      case LOGVIEWERSTORE_ACTIONS.FULL_SCREEN:
        return action.payload.fullScreen;
      case LOGVIEWERSTORE_ACTIONS.RESET:
        return false;
      default:
        return state;
    }
  };

  const searchResults = (state = [], action = {}) => {
    switch(action.type) {
      case LOGVIEWERSTORE_ACTIONS.SEARCH_RESULTS:
        if(!action.payload.searchResults) {
          return state;
        }
        return action.payload.searchResults;
      case LOGVIEWERSTORE_ACTIONS.RESET:
        return [];
      default:
        return state;
    }
  };

  const totalLogs = (state = 0, action = {}) => {
    switch(action.type) {
      case LOGVIEWERSTORE_ACTIONS.TOTAL_LOGS:
        if(!action.payload.totalLogs) {
          return state;
        }
        return action.payload.totalLogs;
      default:
        return state;
    }
  };

  const totalErrors = (state = 0, action = {}) => {
    switch(action.type) {
      case LOGVIEWERSTORE_ACTIONS.TOTAL_ERRORS:
        if(!action.payload.totalErrors) {
          return state;
        }
        return action.payload.totalErrors;
      default:
        return state;
    }
  };

  const totalWarnings = (state = 0, action = {}) => {
    switch(action.type) {
      case LOGVIEWERSTORE_ACTIONS.TOTAL_WARNINGS:
        if(!action.payload.totalWarnings) {
          return state;
        }
        return action.payload.totalWarnings;
      default:
        return state;
    }
  };

  //Combine the reducers
  let {combineReducers, applyMiddleware} = Redux;
  let combinedReducers = combineReducers({
    startTime,
    scrollPosition,
    fullScreen,
    searchResults,
    totalLogs,
    totalErrors,
    totalWarnings
  });

  let getInitialState = () => {
    return {
      startTime: Date.now(),
      scrollPosition: Date.now(),
      fullScreen: false,
      searchResults: [],
      totalLogs: 0,
      totalErrors: 0,
      totalWarnings: 0
    };
  };

  return Redux.createStore(
    combinedReducers,
    getInitialState(),
    Redux.compose(
      applyMiddleware(ReduxThunk.default)
    )
  );
};

LogViewerStore.$inject = ['LOGVIEWERSTORE_ACTIONS', 'Redux', 'ReduxThunk'];

angular.module(`${PKG.name}.commons`)
  .constant('LOGVIEWERSTORE_ACTIONS', {
    'START_TIME' : 'START_TIME',
    'SCROLL_POSITION' : 'SCROLL_POSITION',
    'SEARCH_RESULTS' : 'SEARCH_RESULTS',
    'FULL_SCREEN' : 'FULL_SCREEN',
    'RESET': 'RESET',
    'TOTAL_LOGS' : 'TOTAL_LOGS',
    'TOTAL_ERRORS' : 'TOTAL_ERRORS',
    'TOTAL_WARNINGS' : 'TOTAL_WARNINGS'
  })
  .factory('LogViewerStore', LogViewerStore);
