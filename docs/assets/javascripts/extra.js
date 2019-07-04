/*
~   Copyright (c) WSO2 Inc. (http://wso2.com) All Rights Reserved.
~
~   Licensed under the Apache License, Version 2.0 (the "License");
~   you may not use this file except in compliance with the License.
~   You may obtain a copy of the License at
~
~        http://www.apache.org/licenses/LICENSE-2.0
~
~   Unless required by applicable law or agreed to in writing, software
~   distributed under the License is distributed on an "AS IS" BASIS,
~   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
~   See the License for the specific language governing permissions and
~   limitations under the License.
*/

var logo = document.querySelector('.md-logo');
var logoTitle = logo.title;
logo.setAttribute('href', 'https://siddhi.io/')

var header = document.querySelector('.md-header-nav__title');
var headerContent = document.querySelectorAll('.md-header-nav__title span')[1].textContent;
var url = document.querySelector('.md-nav__item a.md-nav__link').href
header.innerHTML = '<a class="extension-title" href="' + url + '">' + logoTitle + '</a>' +
    '<a class="extension-title-low">' + headerContent + '</a>'


/*
 * TOC position highlight on scroll
 */

var observeeList = document.querySelectorAll(".md-sidebar__inner > .md-nav--secondary .md-nav__link");
var listElems = document.querySelectorAll(".md-sidebar__inner > .md-nav--secondary > ul li");
var config = {attributes: true, childList: true, subtree: true};

var callback = function (mutationsList, observer) {
    for (var mutation of mutationsList) {
        if (mutation.type == 'attributes') {
            mutation.target.parentNode.setAttribute(mutation.attributeName,
                mutation.target.getAttribute(mutation.attributeName));
            scrollerPosition(mutation);
        }
    }
};
var observer = new MutationObserver(callback);

listElems[0].classList.add('active');

for (var i = 0; i < observeeList.length; i++) {
    var el = observeeList[i];

    observer.observe(el, config);

    el.onclick = function (e) {
        listElems.forEach(function (elm) {
            if (elm.classList) {
                elm.classList.remove('active');
            }
        });

        e.target.parentNode.classList.add('active');
    }
}

function scrollerPosition(mutation) {
    var blurList = document.querySelectorAll(".md-sidebar__inner > .md-nav--secondary > ul li > .md-nav__link[data-md-state='blur']");

    listElems.forEach(function (el) {
        if (el.classList) {
            el.classList.remove('active');
        }
    });

    if (blurList.length > 0) {
        if (mutation.target.getAttribute('data-md-state') === 'blur') {
            if (mutation.target.parentNode.querySelector('ul li')) {
                mutation.target.parentNode.querySelector('ul li').classList.add('active');
            } else {
                setActive(mutation.target.parentNode);
            }
        } else {
            mutation.target.parentNode.classList.add('active');
        }
    } else {
        if (listElems.length > 0) {
            listElems[0].classList.add('active');
        }
    }
}

function setActive(parentNode, i) {
    i = i || 0;
    if (i === 5) {
        return;
    }
    if (parentNode.nextElementSibling) {
        parentNode.nextElementSibling.classList.add('active');
        return;
    }
    setActive(parentNode.parentNode.parentNode.parentNode, ++i);
}
