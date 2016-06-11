// Enable master mode with query param 'master', i.e. ?master
var Modes = (function(){

  'use strict';

  var secretKey = null,
      masterJs = '',
      clientJs = '',
      url = window.location.href;
  
  if(window.location.search.match(/(\?|\&)master/gi) || window.location.search.match(/(\?|\&)notes/gi)) {
    secretKey = '14230621003492058662';
    masterJs = 'plugin/multiplex/master.js';
    clientJs = 'plugin/multiplex/client.js';
  }

  // --------------------------------------------------------------------//
  // ------------------------------- API --------------------------------//
  // --------------------------------------------------------------------//
  return {
    getSecretKey: secretKey,
    getMasterJs: masterJs,
    getClientJs: clientJs
  }

})();