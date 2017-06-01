// Add translation navigation bar
// var languages = ['de-DE', 'es-ES', 'fr-FR', 'ja-JP', 'ko-KR', 'pt-PT', 'th-TH', 'tr-TR', 'ru-RU', 'vi-VN', 'zh-CN'];
// var langNavBar = document.createElement('div');
// langNavBar.className = 'lang-nav-bar';
// var langNavText = document.createElement('div');
// langNavText.className = 'lang-nav-text';
// langNavBar.appendChild(langNavText);
// createLangLinks(languages, langNavText);
// displayLangNavBar(langNavText);
// langNavBar.addEventListener('mouseout', function() { displayLangNavBar(langNavText) });

// var content = document.getElementsByClassName('reveal')[0];
// content.appendChild(langNavBar);


// Replace original slides with translated slides if 'lang' param specified
if(window.location.search.match(/lang/gi)) {
  var path = window.location.pathname;
  var page = path.split("/").pop();
  var langCode = getParameterByName("lang");
  var langDir = "i18n/" + langCode + "/" + page;
  replaceLangContent(langDir);
}


/**
 * Functions
**/
function createLangLinks(langs, parentDoc) {
  var langLinks = langs.map(function(lang) {
    var langDoc = document.createElement('a');
    langDoc.href = '?lang=' + lang;
    langDoc.innerHTML = lang;
    return langDoc;
  });

  langLinks.forEach(function(link, i) {
  parentDoc.appendChild(link);
    if(i + 1 < langLinks.length)
      parentDoc.appendChild(document.createTextNode(' | '));
  });

  return parentDoc;
}

function displayLangNavBar(doc) {
  doc.className += ' active';
  setTimeout(function() { doc.className = doc.className.replace(/\b active\b/,''); }, 5000);
}

function getParameterByName(name) {
  name = name.replace(/[\[]/, "\\[").replace(/[\]]/, "\\]");
  var regex = new RegExp("[\\?&]" + name + "=([^&#]*)"),
  results = regex.exec(location.search);
  return results === null ? "" : decodeURIComponent(results[1].replace(/\+/g, " "));
}

function fileExists(url) {
  var http = new XMLHttpRequest();
  http.open('HEAD', url, false);
  http.send();
  return http.status == 200;
}

function replaceLangContent(langDir) {
  var elems = document.getElementsByClassName("content");
  [].forEach.call(elems, function(item) {
    var langFile = item.getAttribute("id");
    // Only replace language if translation file exists
    // TODO: .html file format not supported yet
    var filename = langDir + "/" + langFile + ".md";
    if(fileExists(filename)) {
      item.setAttribute("data-markdown", filename);
    }
  });
}