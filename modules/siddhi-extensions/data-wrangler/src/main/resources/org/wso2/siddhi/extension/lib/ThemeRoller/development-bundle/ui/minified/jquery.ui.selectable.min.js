/*
 * jQuery UI Selectable 1.8.1
 *
 * Copyright (c) 2010 AUTHORS.txt (http://jqueryui.com/about)
 * Dual licensed under the MIT (MIT-LICENSE.txt)
 * and GPL (GPL-LICENSE.txt) licenses.
 *
 * http://docs.jquery.com/UI/Selectables
 *
 * Depends:
 *	jquery.ui.core.js
 *	jquery.ui.mouse.js
 *	jquery.ui.widget.js
 */
(function(e){e.widget("ui.selectable",e.ui.mouse,{options:{appendTo:"body",autoRefresh:true,distance:0,filter:"*",tolerance:"touch"},_create:function(){var d=this;this.element.addClass("ui-selectable");this.dragged=false;var f;this.refresh=function(){f=e(d.options.filter,d.element[0]);f.each(function(){var c=e(this),b=c.offset();e.data(this,"selectable-item",{element:this,$element:c,left:b.left,top:b.top,right:b.left+c.outerWidth(),bottom:b.top+c.outerHeight(),startselected:false,selected:c.hasClass("ui-selected"),
selecting:c.hasClass("ui-selecting"),unselecting:c.hasClass("ui-unselecting")})})};this.refresh();this.selectees=f.addClass("ui-selectee");this._mouseInit();this.helper=e(document.createElement("div")).css({border:"1px dotted black"}).addClass("ui-selectable-helper")},destroy:function(){this.selectees.removeClass("ui-selectee").removeData("selectable-item");this.element.removeClass("ui-selectable ui-selectable-disabled").removeData("selectable").unbind(".selectable");this._mouseDestroy();return this},
_mouseStart:function(d){var f=this;this.opos=[d.pageX,d.pageY];if(!this.options.disabled){var c=this.options;this.selectees=e(c.filter,this.element[0]);this._trigger("start",d);e(c.appendTo).append(this.helper);this.helper.css({"z-index":100,position:"absolute",left:d.clientX,top:d.clientY,width:0,height:0});c.autoRefresh&&this.refresh();this.selectees.filter(".ui-selected").each(function(){var b=e.data(this,"selectable-item");b.startselected=true;if(!d.metaKey){b.$element.removeClass("ui-selected");
b.selected=false;b.$element.addClass("ui-unselecting");b.unselecting=true;f._trigger("unselecting",d,{unselecting:b.element})}});e(d.target).parents().andSelf().each(function(){var b=e.data(this,"selectable-item");if(b){b.$element.removeClass("ui-unselecting").addClass("ui-selecting");b.unselecting=false;b.selecting=true;b.selected=true;f._trigger("selecting",d,{selecting:b.element});return false}})}},_mouseDrag:function(d){var f=this;this.dragged=true;if(!this.options.disabled){var c=this.options,
b=this.opos[0],g=this.opos[1],h=d.pageX,i=d.pageY;if(b>h){var j=h;h=b;b=j}if(g>i){j=i;i=g;g=j}this.helper.css({left:b,top:g,width:h-b,height:i-g});this.selectees.each(function(){var a=e.data(this,"selectable-item");if(!(!a||a.element==f.element[0])){var k=false;if(c.tolerance=="touch")k=!(a.left>h||a.right<b||a.top>i||a.bottom<g);else if(c.tolerance=="fit")k=a.left>b&&a.right<h&&a.top>g&&a.bottom<i;if(k){if(a.selected){a.$element.removeClass("ui-selected");a.selected=false}if(a.unselecting){a.$element.removeClass("ui-unselecting");
a.unselecting=false}if(!a.selecting){a.$element.addClass("ui-selecting");a.selecting=true;f._trigger("selecting",d,{selecting:a.element})}}else{if(a.selecting)if(d.metaKey&&a.startselected){a.$element.removeClass("ui-selecting");a.selecting=false;a.$element.addClass("ui-selected");a.selected=true}else{a.$element.removeClass("ui-selecting");a.selecting=false;if(a.startselected){a.$element.addClass("ui-unselecting");a.unselecting=true}f._trigger("unselecting",d,{unselecting:a.element})}if(a.selected)if(!d.metaKey&&
!a.startselected){a.$element.removeClass("ui-selected");a.selected=false;a.$element.addClass("ui-unselecting");a.unselecting=true;f._trigger("unselecting",d,{unselecting:a.element})}}}});return false}},_mouseStop:function(d){var f=this;this.dragged=false;e(".ui-unselecting",this.element[0]).each(function(){var c=e.data(this,"selectable-item");c.$element.removeClass("ui-unselecting");c.unselecting=false;c.startselected=false;f._trigger("unselected",d,{unselected:c.element})});e(".ui-selecting",this.element[0]).each(function(){var c=
e.data(this,"selectable-item");c.$element.removeClass("ui-selecting").addClass("ui-selected");c.selecting=false;c.selected=true;c.startselected=true;f._trigger("selected",d,{selected:c.element})});this._trigger("stop",d);this.helper.remove();return false}});e.extend(e.ui.selectable,{version:"1.8.1"})})(jQuery);
