/*
 * Copyright © 2017 Cask Data, Inc.
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

import React, {Component, PropTypes} from 'react';
import {Link} from 'react-router';
import T from 'i18n-react';
import NamespaceStore from 'services/NamespaceStore';
import NamespaceDropdown from 'components/NamespaceDropdown';
import ProductDropdown from 'components/Header/ProductDropdown';
import MetadataDropdown from 'components/Header/MetadataDropdown';
import CaskMarketButton from 'components/Header/CaskMarketButton';
import {MyNamespaceApi} from 'api/namespace';
import NamespaceActions from 'services/NamespaceStore/NamespaceActions';
import classnames from 'classnames';

require('./Header.scss');

export default class Header extends Component {
  constructor(props) {
    super(props);
    this.state = {
      toggleNavbar: false,
      currentNamespace: null,
      metadataDropdown: false
    };
    this.namespacesubscription = null;
  }
  componentWillMount() {
    // Polls for namespace data
    this.namespacesubscription = MyNamespaceApi.pollList()
      .subscribe(
        (res) => {
          if (res.length > 0) {
            NamespaceStore.dispatch({
              type: NamespaceActions.updateNamespaces,
              payload: {
                namespaces : res
              }
            });
          } else {
            // To-Do: No namespaces returned ; throw error / redirect
          }
        }
      );
    this.nsSubscription = NamespaceStore.subscribe(() => {
      let selectedNamespace = NamespaceStore.getState().selectedNamespace;
      if (selectedNamespace !== this.state.currentNamespace) {
        this.setState({
          currentNamespace: selectedNamespace
        });
      }
    });
  }
  componentWillUnmount() {
    this.nsSubscription();
    if (this.namespacesubscription) {
      this.namespacesubscription();
    }
  }
  toggleNavbar() {
    this.setState({
      toggleNavbar: !this.state.toggleNavbar
    });
  }

  render() {
    let baseCDAPURL = window.getAbsUIUrl({
      namespace: this.state.currentNamespace
    });
    let overviewUrl = `${baseCDAPURL}/ns/${this.state.currentNamespace}`;
    let pipelinesUrl =  window.getHydratorUrl({
      stateName: 'hydrator.list',
      stateParams: {
        namespace: this.state.currentNamespace
      }
    });
    let oldUIUrl = `/oldcdap/ns/${this.state.currentNamespace}`;
    return (
      <div className="global-navbar">
        <div
          className="global-navbar-toggler float-xs-right btn"
          onClick={this.toggleNavbar.bind(this)}
        >
          {
            !this.state.toggleNavbar ?
              <i className="fa fa-bars fa-2x"></i>
            :
              <i className="fa fa-times fa-2x"></i>
          }
        </div>
        <div className="brand-section">
          <img src="/cdap_assets/img/company_logo.png" />
        </div>
        <ul className="navbar-list-section">
          <li>
            {
              !this.props.nativeLink ?
                <Link
                  activeClassName="active"
                  to={`/ns/${this.state.currentNamespace}`}
                >
                  {T.translate('features.Navbar.overviewLabel')}
                </Link>
              :
                <a href={overviewUrl}>
                  {T.translate('features.Navbar.overviewLabel')}
                </a>
            }
          </li>
          <li>
            <a
              href={pipelinesUrl}
              className={classnames({'active': pipelinesUrl.indexOf(location.pathname) !== -1})}
            >
              {T.translate('features.Navbar.pipelinesLabel')}
            </a>
          </li>
          <li>
            <MetadataDropdown />
          </li>
        </ul>
        <div className={classnames("global-navbar-collapse", {
            'minimized': this.state.toggleNavbar
          })}>
          <div className="navbar-right-section">
            <ul>
              <li>
                <a href={oldUIUrl}>
                  {T.translate('features.Navbar.RightSection.olduilink')}
                </a>
              </li>
              <li className="with-shadow">
                <CaskMarketButton>
                  <span className="fa icon-CaskMarket"></span>
                  <span>{T.translate('commons.market')}</span>
                </CaskMarketButton>
              </li>
              <li
                id="header-namespace-dropdown"
                className="with-shadow namespace-dropdown-holder">
                {
                  !this.props.nativeLink ?
                    <NamespaceDropdown />
                  :
                    <NamespaceDropdown tag="a"/>
                }
              </li>
              <li className="with-shadow cdap-menu clearfix">
                <ProductDropdown
                  nativeLink={this.props.nativeLink}
                />
              </li>
            </ul>
          </div>
        </div>
      </div>
    );
  }
}

Header.defaultProps = {
  nativeLink: false
};

Header.propTypes = {
  nativeLink: PropTypes.bool
};
