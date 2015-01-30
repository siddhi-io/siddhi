/*

highlight v3

Highlights arbitrary terms.

<http://johannburkard.de/blog/programming/javascript/highlight-javascript-text-higlighting-jquery-plugin.html>

MIT license.

Johann Burkard
<http://johannburkard.de>
<mailto:jb@eaio.com>

*/

jQuery.fn.highlight = function(start,end) {
 function innerHighlight(node, start, end) {
  var skip = 0;
  if (node.nodeType == 3) {
   
   
   if (start >= 0 && start < node.data.length && end >= 0 && end <= node.data.length) {
    var spannode = document.createElement('span');

	spannode.className = 'highlight';
    var middlebit = node.splitText(start);
    var endbit = middlebit.splitText(end-start)//pat.length);
    var middleclone = middlebit.cloneNode(true);
    spannode.appendChild(middleclone);
    middlebit.parentNode.replaceChild(spannode, middlebit);
    skip = 1;
   }
  }
  else if (node.nodeType == 1 && node.childNodes && !/(script|style)/i.test(node.tagName)) {
   
    innerHighlight(node.childNodes[0], start, end);
   
  }
  return skip;
 }
 return this.each(function() {
  innerHighlight(this, start, end);
 });
};

jQuery.fn.removeHighlight = function() {
	console.log('remove')
 return this.find("span.highlight").each(function() {
  this.parentNode.firstChild.nodeName;
  with (this.parentNode) {
   replaceChild(this.firstChild, this);
   normalize();
  }
 }).end();
};

Highlight = {}
Highlight.highlight = function(node, start, end, options){

	if (node.nodeType == 3) {


	   if (start >= 0 && start < node.data.length && end >= 0 && end <= node.data.length) {
	    var spannode = document.createElement('span');
	    spannode.className = options.highlightClass + ' highlight';
	    var middlebit = node.splitText(start);

		var endbit = middlebit.splitText(end-start)


		

		var middleclone = middlebit.cloneNode(true);
		// if(start===end){
		// 		var div = document.createElement('div')
		// 		div.style.width = 3;
		// 		div.style.height = 3;
		// 		div.style.display = 'inline'
		// 		spannode.appendChild(div)
		// 	}
		// 	else{
			spannode.appendChild(middleclone);
//		}
	    middlebit.parentNode.replaceChild(spannode, middlebit);
	    
	   }
	 }
	
	
}

Highlight.removeHighlight = function(node) {

 jQuery(node).find("span.highlight").each(function() {

  this.parentNode.firstChild.nodeName;
  with (this.parentNode) {
   replaceChild(this.firstChild, this);
   normalize();
  }
 });
};
