// Validates the correct format of the slides
// Add this javascript to the HTML to run the validation

// Variables
var beginText = "trainivator-slide-begin";
var endText = "trainivator-slide-end";
var hideText = "trainivator-hide";

// Get data
var slides = document.getElementsByClassName('slides')[0];
var comments = getComments(slides, []);
var beginCommentCount = countCommentsByText(comments, beginText);
var endCommentCount = countCommentsByText(comments, endText);
var hideCommentCount = countCommentsByText(comments, hideText);
var sectionNodes = slides.getElementsByTagName("section");
var sectionArray = Array.prototype.slice.call(sectionNodes);
var sectionCountToBeHidden = sectionArray.filter(isNoContentSection).length;
var duplicateIds = getDuplicateIds(sectionArray);


// Validation
var errors = [];
if(beginCommentCount != 1)
  errors.push("The translation marker " + beginText + " has not been set exactly once.");
if(endCommentCount != 1)
  errors.push("The translation marker " + endText + " has not been set exactly once.");
if(hideCommentCount / 2 != sectionCountToBeHidden)
  errors.push("The slide count doesn't equal the number of HTML sections with id and class attribute 'content' OR the HTML sections without content are not marked with " + hideText + ".");
if(duplicateIds.length > 0)
  errors.push("The slides have duplicate ids: " + duplicateIds);	

if(errors.length > 0)
  alert("Validation of slides failed! Fix the errors before pushing the changes to master. \nIf in doubt read these instructions: https://github.com/typesafe-training/typesafe-training/blob/master/author-instructions.md. \n\nErrors: " + errors.map(function(error, i) {return "\n" + (i + 1) + ". " + error;}));


/**
 * Functions
 */

Node = Node || {
  COMMENT_NODE: 8
};

function getComments(elem, currentComments) {
  var children = elem.childNodes;

  for (var i=0, len=children.length; i<len; i++) {
    if (children[i].nodeType == Node.COMMENT_NODE) {
      currentComments.push(children[i]);
    }

    getComments(children[i], currentComments);
  }

  return currentComments;
}

function countCommentsByText(comments, text) {
  return comments.filter(function(comment) { return filterCommentByText(comment, text) }).length;
}

function filterCommentByText(comment, text) {
  return (comment.data.indexOf(text) > -1);
}

function isNoContentSection(element) {
  return !hasId(element) && !hasContent(element);
}

function hasId(element) {
  return element.hasAttribute("id");
}

function hasContent(element) {
  return element.className.indexOf("content") > -1;
}

function getDuplicateIds(elements) {
  var num = elements.length;
  var ids = [];
  var duplicateIds = [];
  for(i=0; i<num; i++ ) {
    var id = elements[i].getAttribute('id');
    if(id != null) {
      if(ids.indexOf(id) >=0 ) {
      	duplicateIds.push(id); // found in table
      } else {
        ids.push(id); // new id found, add it to array
      } 
    }
  }

  return duplicateIds;	
}

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


// // Replace original slides with translated slides if 'lang' param specified
// if(window.location.search.match(/lang/gi)) {
//   var path = window.location.pathname;
//   var page = path.split("/").pop();
//   var langCode = getParameterByName("lang");
//   var langDir = "i18n/" + langCode + "/" + page;
//   replaceLangContent(langDir);
// }


// /**
//  * Functions
// **/
// function createLangLinks(langs, parentDoc) {
//   var langLinks = langs.map(function(lang) {
//     var langDoc = document.createElement('a');
//     langDoc.href = '?lang=' + lang;
//     langDoc.innerHTML = lang;
//     return langDoc;
//   });

//   langLinks.forEach(function(link, i) {
//   parentDoc.appendChild(link);
//     if(i + 1 < langLinks.length)
//       parentDoc.appendChild(document.createTextNode(' | '));
//   });  

//   return parentDoc;
// }

// function displayLangNavBar(doc) {
//   console.log("asdsd")
//   doc.className += ' active';
//   setTimeout(function() { doc.className = doc.className.replace(/\b active\b/,''); }, 5000);
// }

// function getParameterByName(name) {
//   name = name.replace(/[\[]/, "\\[").replace(/[\]]/, "\\]");
//   var regex = new RegExp("[\\?&]" + name + "=([^&#]*)"),
//   results = regex.exec(location.search);
//   return results === null ? "" : decodeURIComponent(results[1].replace(/\+/g, " "));
// }

// function fileExists(url) {
//   var http = new XMLHttpRequest();
//   http.open('HEAD', url, false);
//   http.send();
//   return http.status == 200;
// }

// function replaceLangContent(langDir) {  
//   var elems = document.getElementsByClassName("content");
//   [].forEach.call(elems, function(item) {
//     var langFile = item.getAttribute("id");
//     // Only replace language if translation file exists
//     // TODO: .html file format not supported yet
//     var filename = langDir + "/" + langFile + ".md";
//     if(fileExists(filename)) {
//       item.setAttribute("data-markdown", filename);
//     }          
//   });
// }