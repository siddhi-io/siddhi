

var dw = {};

dw.version = {
  major: 0,
  minor: 1
};

dw.error = function(e) {
  (typeof console == "undefined") ? alert(e) : console.error(e);
};

dw.listen = function(target, type, listener) {
  listener = dw.listener(listener);
  return target.addEventListener
      ? target.addEventListener(type, listener, false)
      : target.attachEvent("on" + type, listener);
};

dw.listener = function(f) {
  return f.$listener || (f.$listener = function(e) {
      try {
        dw.event = e;
        return f.call(this, e);
      } finally {
        delete dw.event;
      }
    });
};












dw.jq = function(e){
	return jQuery(document.createElement(e))
}

dw.add_select_option = function(select, key, value){
	if(arguments.length < 3) value = key;
	var option = document.createElement("option");
	option.value = value;
	option.text = key;
	select[0].options.add(option);
}

dw.progress_call = function(callback, varargs) {

  var_args = Array.prototype.slice.call(arguments).slice(2);
  callback.apply(var_args[0], var_args);
  return this;
}

// Set DW_LOG to intended server to enable logging.
// Example: http://www.mydomain.com/logger.
// See log.php for example of handling log entries.
DW_LOG = undefined

dw.log = function(e){
  if (!DW_LOG) {
    return;
  }

	e.dt = new Date();

	var table = e.table;
	e.table = [];

	var x = JSON.stringify(e);
	e.table = table;

	jQuery.ajax({
		  type: 'POST',
		  url: DW_LOG,
		  data: {token:'something',msg:x}
		});


}





dw.date_parse = function(s) {
	s = '' + s;






	if (s.match(/\d+[-]\d+[-]\d+/) ||
	    s.match(/\d+[/]\d+[/]\d+/) ||
	    s.match(/\d+[.]\d+[.]\d+/)) {
		return Date.parse(s);
	}

	return NaN;
}


dw.merge_sort =  function(array,comparison)
{

	comparison = comparison || dw.merge_sort.stringCompare;

	if(array.length < 2)
		return array;
	var middle = Math.ceil(array.length/2);
	return dw.merge_sort.merge(dw.merge_sort(array.slice(0,middle),comparison),
			dw.merge_sort(array.slice(middle),comparison),
			comparison);
}


dw.merge_sort.merge = function(left,right,comparison)
{
	var result = new Array();
	while((left.length > 0) && (right.length > 0))
	{
		if(comparison(left[0],right[0]) < 0)
			result.push(left.shift());
		else
			result.push(right.shift());
	}
	while(left.length > 0)
		result.push(left.shift());
	while(right.length > 0)
		result.push(right.shift());
	return result;
}


dw.merge_sort.stringCompare = function(left, right){

	left = ""+left
	right = "" + right

	return dw.merge_sort.compare(left,right)
}
dw.merge_sort.compare = function(left, right)
{
	if(left == right)
		return 0;
	else if(left < right)
		return -1;
	else
		return 1;
}
dw.merge_sort.numberCompare = function(left, right)
{
	if(left===undefined&&right===undefined) return 0;
	if(left===undefined) return -1;
	if(right===undefined) return 1;

	leftNo = Number(left)
	rightNo = Number(right)
	if(isNaN(leftNo)&&isNaN(rightNo)) return dw.merge_sort.compare(left, right)
	else if(isNaN(leftNo)) return -1
	else if(isNaN(rightNo)) return 1
	return dw.merge_sort.compare(leftNo, rightNo)

}

dw.merge_sort.dateCompare = function(left, right)
{

	leftNo = Number(left)
	rightNo = Number(right)
	if(isNaN(leftNo)&&isNaN(rightNo)) return dw.merge_sort.compare(left, right)
	else if(isNaN(leftNo)) return rightNo
	else if(isNaN(rightNo)) return leftNo
	return dw.merge_sort.compare(leftNo, rightNo)

}

dw.merge_sort.getDateComparison = function(preferred, strict){

	var getParsedDate = function(d){






		return dw.date_parse(d);
	}



	var compare = function(left, right){
		leftNo = getParsedDate(left)
		rightNo = getParsedDate(right)
		if(leftNo && rightNo){
			if(leftNo < rightNo) return -1;
			if(rightNo > leftNo) return 1;
			return 0;
		}
		else if(leftNo) return -1
		else if(rightNo) return 1
		return dw.merge_sort.compare(leftNo, rightNo)
	}

	return compare;





}

dw.display_name = function(name){

	if(name && name[0]==='_') return name.substr(1);
	return name
}




































    dw.JSON = {};


(function () {
    "use strict";

    function f(n) {

        return n < 10 ? '0' + n : n;
    }

    if (typeof Date.prototype.toJSON !== 'function') {

        Date.prototype.toJSON = function (key) {

            return isFinite(this.valueOf()) ?
                this.getUTCFullYear()     + '-' +
                f(this.getUTCMonth() + 1) + '-' +
                f(this.getUTCDate())      + 'T' +
                f(this.getUTCHours())     + ':' +
                f(this.getUTCMinutes())   + ':' +
                f(this.getUTCSeconds())   + 'Z' : null;
        };

        String.prototype.toJSON      =
            Number.prototype.toJSON  =
            Boolean.prototype.toJSON = function (key) {
                return this.valueOf();
            };
    }

    var cx = /[\u0000\u00ad\u0600-\u0604\u070f\u17b4\u17b5\u200c-\u200f\u2028-\u202f\u2060-\u206f\ufeff\ufff0-\uffff]/g,
        escapable = /[\\\"\x00-\x1f\x7f-\x9f\u00ad\u0600-\u0604\u070f\u17b4\u17b5\u200c-\u200f\u2028-\u202f\u2060-\u206f\ufeff\ufff0-\uffff]/g,
        gap,
        indent,
        meta = {
            '\b': '\\b',
            '\t': '\\t',
            '\n': '\\n',
            '\f': '\\f',
            '\r': '\\r',
            '"' : '\\"',
            '\\': '\\\\'
        },
        rep;


    function quote(string) {






        escapable.lastIndex = 0;
        return escapable.test(string) ? '"' + string.replace(escapable, function (a) {
            var c = meta[a];
            return typeof c === 'string' ? c :
                '\\u' + ('0000' + a.charCodeAt(0).toString(16)).slice(-4);
        }) + '"' : '"' + string + '"';
    }


    function str(key, holder) {



        var i,
            k,
            v,
            length,
            mind = gap,
            partial,
            value = holder[key];



        if (value && typeof value === 'object' &&
                typeof value.toJSON === 'function') {
            value = value.toJSON(key);
        }




        if (typeof rep === 'function') {
            value = rep.call(holder, key, value);
        }



        switch (typeof value) {
        case 'string':
            return quote(value);

        case 'number':



            return isFinite(value) ? String(value) : 'null';

        case 'boolean':
        case 'null':





            return String(value);




        case 'object':




            if (!value) {
                return 'null';
            }



            gap += indent;
            partial = [];



            if (Object.prototype.toString.apply(value) === '[object Array]') {




                length = value.length;
                for (i = 0; i < length; i += 1) {
                    partial[i] = str(i, value) || 'null';
                }




                v = partial.length === 0 ? '[]' : gap ?
                    '[\n' + gap + partial.join(',\n' + gap) + '\n' + mind + ']' :
                    '[' + partial.join(',') + ']';
                gap = mind;
                return v;
            }



            if (rep && typeof rep === 'object') {
                length = rep.length;
                for (i = 0; i < length; i += 1) {
                    k = rep[i];
                    if (typeof k === 'string') {
                        v = str(k, value);
                        if (v) {
                            partial.push(quote(k) + (gap ? ': ' : ':') + v);
                        }
                    }
                }
            } else {



                for (k in value) {
                    if (Object.hasOwnProperty.call(value, k)) {
                        v = str(k, value);
                        if (v) {
                            partial.push(quote(k) + (gap ? ': ' : ':') + v);
                        }
                    }
                }
            }




            v = partial.length === 0 ? '{}' : gap ?
                '{\n' + gap + partial.join(',\n' + gap) + '\n' + mind + '}' :
                '{' + partial.join(',') + '}';
            gap = mind;
            return v;
        }
    }

    /* The method dw.JSON.stringify is adapted from Douglas Crockford's JSON library.
    ** https://github.com/douglascrockford/JSON-js
    */
    if (typeof dw.JSON.stringify !== 'function') {
        dw.JSON.stringify = function (value, replacer, space) {







            var i;
            gap = '';
            indent = '';




            if (typeof space === 'number') {
                for (i = 0; i < space; i += 1) {
                    indent += ' ';
                }



            } else if (typeof space === 'string') {
                indent = space;
            }




            rep = replacer;
            if (replacer && typeof replacer !== 'function' &&
                    (typeof replacer !== 'object' ||
                    typeof replacer.length !== 'number')) {
                throw new Error('dw.JSON.stringify');
            }




            return str('', {'': value});
        };
    }




    if (typeof dw.JSON.parse !== 'function') {
        dw.JSON.parse = function (text, reviver) {




            var j;

            function walk(holder, key) {




                var k, v, value = holder[key];
                if (value && typeof value === 'object') {
                    for (k in value) {
                        if (Object.hasOwnProperty.call(value, k)) {
                            v = walk(value, k);
                            if (v !== undefined) {
                                value[k] = v;
                            } else {
                                delete value[k];
                            }
                        }
                    }
                }
                return reviver.call(holder, key, value);
            }






            text = String(text);
            cx.lastIndex = 0;
            if (cx.test(text)) {
                text = text.replace(cx, function (a) {
                    return '\\u' +
                        ('0000' + a.charCodeAt(0).toString(16)).slice(-4);
                });
            }














            if (/^[\],:{}\s]*$/
                    .test(text.replace(/\\(?:["\\\/bfnrt]|u[0-9a-fA-F]{4})/g, '@')
                        .replace(/"[^"\\\n\r]*"|true|false|null|-?\d+(?:\.\d*)?(?:[eE][+\-]?\d+)?/g, ']')
                        .replace(/(?:^|:|,)(?:\s*\[)+/g, ''))) {






                j = eval('(' + text + ')');




                return typeof reviver === 'function' ?
                    walk({'': j}, '') : j;
            }



            throw new SyntaxError('dw.JSON.parse');
        };
    }
}());


dw.wrangle = function(){
	var w = [];
	
	w.apply = function(tables){
		if(typeOf(tables)==='string'){
			tables = dv.table(tables)
		}
		if(typeOf(tables)!='array'){
			tables = [tables]	
		}
		w.forEach(function(t){
			
			if(t.active() || t.invalid()){
				var status = t.check_validity(tables);
				if(status.valid){
					t.sample_apply(tables);	
					
					var typeTransforms = dw.infer_type_transforms(tables[0]);
					typeTransforms.forEach(function(tt){
						tt.sample_apply([tables[0]])
					})
						
					t.validate();		
				}
				else{
					t.invalidate(status.errors);
				}
			}
			

		})
		dw.summary.clear_cache();
		return w;
	}
	
	w.add = function(t){
		w.push(t)
		return w;
	}
	
	return w;
}
var dw = dw || {};
dw.regex = function(){
	var r = {};

	var numberRecord = function(m){



		var r = [
			new RegExp(m),
			/\d+/
		];
		return r;
	}

	var stringRecord = function(m){



		var r = [
			new RegExp(m),
			/[a-zA-Z]+/
		];

		if(m.toLowerCase()===m){
			r.push(/[a-z]+/)
		}
		else if(m.toUpperCase()===m){
			r.push(/[A-Z]+/)
		}
		return r;

	}

	var symbolRecord = function(m){
		var regex

		if(['|'].indexOf(m)!=-1){
			m = '\\'+m
		}

		if (m == '.') {
			m = '\\.';
		}

		try{
			regex = new RegExp(m);
		}
		catch(e){
			regex = new RegExp('\\'+m);
		}

		var r = [
			regex
		];
		return r;
	}


	r.candidates = function(records, o){

		if(records.length){
			var enumerations = r.parse(records[0].text, records[0].start, records[0].end),
				tests = records.slice(0), on, between, match, candidates;




			on = (enumerations.on || []).map(function(c){
				return {on:c}
			});


			var before = enumerations.before, after = enumerations.after;



			if(before === undefined || before.length === 0){
				between = after.map(function(a){return {after:a, on:/.*/}});
			}
			else if(after === undefined || after.length === 0){
				between = before.map(function(a){return {before:a,on:/.*/}});
			}
			else{
				between = (before||[]).reduce(function(x, b){
					return x.concat((after||[]).map(function(a){
						return {before:b, after:a, on:/.*/}
					}))
				}, [])
			}





			candidates = on.concat(between);


			if(tests.length===0) return candidates;

			candidates = candidates.filter(function(candidate){
				return tests.filter(function(test){
					match = dw.regex.match(test.text, candidate);
					return(match.length < 2 || match[1].start!=test.start||match[1].end!=test.end)
				}).length===0
			})
			return candidates
		}

		return []

	}

	var collapse = function(regexArray){
		var joined = regexArray.map(function(r){
			return r.toString().replace(/^\/|\/$/g,'')
		}).join('')

		return new RegExp(joined)
	}


	r.parse = function(str, startCursor, endCursor, o){
		str = ''+str

		var token = /([a-zA-Z]+)|([0-9]+)|([^a-zA-Z0-9])/g;

		var match = (str.substring(0, startCursor).match(token) || [])
					.concat(str.substring(startCursor, endCursor).match(token) || [])
					.concat(str.substring(endCursor).match(token) || [])



		var	o = o || {}, code, records, startIndex, endIndex, index = 0,
			on, before, after,
			matchAfter = o.matchAfter||3, matchBefore=o.matchBefore||3; /*candidates*/



		match = match.filter(function(m){return m!=null})



		records = match.map(function(m, ind){
			code = m.charCodeAt(0);
			if(startCursor >= index && startCursor < index+m.length){
				startIndex = ind;
			}
			if(endCursor > index && endCursor <= index+m.length){
				endIndex = ind;
			}
			index+=m.length;

			if((code > 64 && code < 91) || (code > 96 && code < 123)){
				return stringRecord(m);
			}else if(code > 47 && code < 58){
				return numberRecord(m);
			}
			else{
				return symbolRecord(m);
			}
		})




		if(startIndex===undefined) startIndex = match.length-1;
		if(endIndex===undefined) endIndex = match.length-1


		on = records.slice(startIndex, endIndex+1).reduce(function(a, b){
			var cross = [];
			a.forEach(function(i){
				b.forEach(function(j){
					cross.push(i.concat(j))
				})
			})
			return cross;
		}, [[]])




		var enumerate = function(a, b){
			var cross = [];
			if(a.length){
				a.forEach(function(i){
					b.forEach(function(j){
						cross.push(i.concat(j))
					})
				})
				return a.concat(cross);
			}
			else{
				return b.map(function(j){
					return [j];
				})
			}
		}




			after = records.slice(Math.max(startIndex-matchAfter-1, 0), startIndex).reverse().reduce(enumerate, [])

			before = records.slice(endIndex+1, Math.min(endIndex+matchBefore+1, records.length)).reduce(enumerate, [])




		return {on:on.map(collapse), after:(after||[]).map(function(x){return collapse(x.reverse())}), before:(before||[]).map(function(x){return collapse(x)})}

	}

	return r;
}

dw.regex.record = function(text, start, end, col, row, table){
	return {text:text, start:start, end:end, col:col, row:row, table:table}
}

dw.regex.friendly_description = function(regex){

	var regex = regex.toString().replace(/^\/|\/$/g,'')
	regex = regex.replace(/\n/g, 'newline')
	regex = regex.replace(/ /g, ' ')
	regex = regex.replace(/\t/g, 'tab')
	regex = regex.replace(/\(?(\[0\-9\]|\\d)\+\)?/g, ' any number ')
	regex = regex.replace(/\(?(\[a\-z\A\-Z\]|\[A\-Z\a\-\z\])\+\)?/g, ' any word ')
	regex = regex.replace(/\(?(\[a\-z\])\+\)?/g, ' any lowercase word ')
	regex = regex.replace(/\(?(\[A\-Z\])\+\)?/g, ' any uppercase word ')
	regex = regex.replace(/\$$/, '{end}')
	regex = regex.replace(/^\^/, '{begin}')

	regex = regex.replace('\\','')


	if(regex === 'newline') return regex

	return "'"+regex+"'";
}

dw.regex.description_length = function(regex){
	if(!regex) return 0;

	regex = regex.toString().replace(/^\/|\/$/g,'');







	regex = regex.replace(/\\n/g, 'n')
	regex = regex.replace(/ /g, ' ')
	regex = regex.replace(/\t/g, 't')
	regex = regex.replace(/\(?(\[0\-9\]|\\d)\+\)?/g, 'n')
	regex = regex.replace(/\(?(\[A\-Z\])\+\)?/g, ' w')
	regex = regex.replace(/\(?(\[a\-z\])\+\)?/g, 'w')
	regex = regex.replace(/\(?(\[a\-z\A\-Z\]|\[A\-Z\a\-\z\])\+\)?/g, 'w')
	regex = regex.replace(/\$$/, 'e')
	regex = regex.replace(/^\^/, 'b')
	regex = regex.replace('\\','')

	var match = regex.match(/([a-zA-Z]+)|([0-9]+)|([^a-zA-Z0-9])/g)

	return match.length+1


	return regex.length;
}
dw.regex.match = function(value, params){
	
	if(!value) return ""
	
	var max_splits = params.max_splits;
	if(max_splits===undefined) max_splits = 1;

	var remainder_to_split = {start:0, end:value.length,value:value}
	var splits = []
	var numSplit = 0;
	var which = Number(params.which)
	if(isNaN(which)) which = 1

	while(max_splits <= 0 || numSplit < max_splits*which){
		var s = dw.regex.matchOnce(remainder_to_split.value, params)

		if(s.length > 1){

				remainder_to_split = s[2];
				splits.push(s[0])
				splits.push(s[1])
				occurrence = 0


		}
		else{
			break
		}
		numSplit++;
		if(numSplit > 1000){
			
			break;
		}
	}
	
	splits.push(remainder_to_split)




	var occurrence = 0;
	var newSplits = []
	var prefix = ''
	var i;
	for(i = 0; i < splits.length; ++i){
		if(i%2===1){
			occurrence++;
			if(occurrence===which){
				newSplits.push({value:prefix, start:0, end:prefix.length})
				newSplits.push({start:prefix.length, end:prefix.length+splits[i].value.length, value:splits[i].value})
				occurrence = 0;
				prefix = ''
				continue
			}
		}
		prefix += splits[i].value
	}
	newSplits.push({start:0, end:prefix.length, value:prefix})

	

	return newSplits;
}

dw.regex.matchOnce = function(value, params){
	
	var positions = params.positions;
	var splits = [];	

	if(positions && positions.length){
		if(positions.length==2){
			if(value.length >= positions[1]){
				var split_start = positions[0]
				var split_end = positions[1]
				splits.push({start:0, end:split_start, value:value.substr(0, split_start)});
				splits.push({start:split_start, end:split_end, value:value.substr(split_start, split_end-split_start)})			
				splits.push({start:split_end, end:value.length, value:value.substr(split_end)})

				return splits;
			}
			return [{start:0, end:value.length, value:value}]

		}
	}
	
	
	
	
	var before = params.before;
	var after = params.after;
	var on = params.on
	var ignore_between = params.ignore_between;
	

	var remainder = value;
	var remainder_offset = 0;
	var start_split_offset = 0;
	var add_to_remainder_offset = 0;
	
	
	while(remainder.length){

		var valid_split_region = remainder;
		var valid_split_region_offset = 0;
		

		start_split_offset = remainder_offset;
		
		
		if(ignore_between){
			
			var match = remainder.match(ignore_between);
			if(match){
				
				valid_split_region = valid_split_region.substr(0, match.index)
				remainder_offset += match.index+match[0].length;
				remainder = remainder.substr(match.index+match[0].length)
				
			}
			else{
				remainder = ''
			}
		
		}
		else{
			remainder = ''
		}
	
		if(after){
			var match = valid_split_region.match(after)
			if(match){
				valid_split_region_offset = match.index+match[0].length;
				valid_split_region = valid_split_region.substr(valid_split_region_offset)

			}
			else{
				continue;
			}
		}
		if(before){
			var match = valid_split_region.match(before)
			if(match){
				valid_split_region = valid_split_region.substr(0, match.index)
			}
			else{
				continue;
			}
		}
	

		var match = valid_split_region.match(on)
		if(match){
			
			var split_start = start_split_offset + valid_split_region_offset+match.index;
			var split_end = split_start + match[0].length;
			
			splits.push({start:0, end:split_start, value:value.substr(0, split_start)});
			splits.push({start:split_start, end:split_end, value:value.substr(split_start, split_end-split_start)})			
			splits.push({start:split_end, end:value.length, value:value.substr(split_end)})
			return splits;
			
		}
		continue;
	
	}

	return [{start:0, end:value.length, value:value}]
	
	
}
dw.copy = function(column){
	var t = dw.map(column);

	t.well_defined = function(table){
		return t._column.length === 1;
	}
	
	t.transform = function(values){



		return values;
	}


	t.description = function(){
		return [
			'Copy',
			dw.column_clause(t, t._column, 'column')
		]
	}
	


	t.name = dw.transform.COPY;
	
	return t;
}
dw.cut = function(column){
	var t = dw.textPattern(column);
	t._drop = false;
	t._update = true;
	t.transform = function(values){
		
		
		if(values[0]===undefined) return []
		if(t._positions && t._positions.length){
			var val = ""+values[0]
			var indices = t._positions;
			var startIndex = indices[0], endIndex = indices[1] || indices[0];
			var splitValues = [];
			splitValues.push(val.substring(0,startIndex) + val.substring(endIndex))
			splitValues.stats = [{splits:[{start:startIndex, end:endIndex}]}]
			return splitValues;
		}
		else{
			
			var val;
			var z = [];
			for(var v = 0; v < values.length; ++v){
				val = values[v];
				var params = {which:t._which, max_splits:t._max, before:t._before,after:t._after,on:t._on,ignore_between:t._ignore_between}
				var cuts = dw.regex.match(val, params);
				var cutValues = [];
				cutValues.stats = [];
				for(var i = 0; i < cuts.length; ++i){
					if(i%2==0){
						cutValues.push(cuts[i].value)					

					}
					else{
						cutValues.stats.push({splits:[{start:cuts[i].start, end:cuts[i].end}]})					
					}
				}

				z.push(cutValues.join(''));
				
				if(!v) z.stats = cutValues.stats;
				
			}			

			return z;
		}
	}
	
	t.description = function(){

		var cutStart = (t._column && t._column.length) ? 'Cut from' : 'Cut';

		var description = [
			cutStart,
			dw.column_clause(t, t._column, 'column')
		]

		regex = t.match_description();

		description = description.concat(regex)


		return description;

	}

		
	

	t.name = dw.transform.CUT;
	
	return t;
}
dw.drop = function(column){
	var t = dw.transform(column);
	t._drop = true;
	dv.ivar(t, [])
	
	t.description = function(){
		return [
			'Drop',
			dw.column_clause(t, t._column, 'column', {editor_class:'droppedColumn'})		]
	}

	t.apply = function(tables){

		var table = t.getTable(tables),
			columns = t.columns(table);


		if(t._drop){
			columns.forEach(function(col){

				table.removeColumn(col.name);
			})
		}

			

	
		return {droppedCols:columns}
		
	}
	t.name = dw.transform.DROP;
	return t;
}
dw.extract = function(column){
	var t = dw.textPattern(column);
	t.transform = function(values){
		if(values[0]===undefined) return []
		if(t._positions && t._positions.length){
			var val = ""+values[0]
			var indices = t._positions;
			var startIndex = indices[0], endIndex = indices[1] || indices[0];
			var splitValues = [];
			if(endIndex <= val.length){
				splitValues.push(val.substring(startIndex, endIndex))
				splitValues.stats = [{splits:[{start:startIndex, end:endIndex}]}]
			}
			return splitValues;
		}
		else{
			var params = {which:t._which, max_extracts:t._max, before:t._before,after:t._after,on:t._on,ignore_between:t._ignore_between}
			var extracts = dw.regex.match(values[0], params);
			var extractValues = [];
			extractValues.stats = [];
			for(var i = 0; i < extracts.length; ++i){
				if(i%2==1){
					extractValues.push(extracts[i].value)
					extractValues.stats.push({splits:[{start:extracts[i].start, end:extracts[i].end}]})
				}
			}
			

			
			return extractValues;
		}
	}
	
	t.description = function(){
		
		var description = [
			'Extract from',
			dw.column_clause(t, t._column, 'column', {editor_class:'none'})
		]
		
		regex = t.match_description({editor_class:'extract'});
		
		description = description.concat(regex)
		
		
		return description;
		
	}

	t.name = dw.transform.EXTRACT;
	
	return t;
}
dw.LEFT = 'left';
dw.UP = 'up';
dw.DOWN = 'down';
dw.RIGHT = 'right';
dw.COPY = 'copy';
dw.INTERPOLATE = 'interpolate';

dw.fill = function(column){
	var t = dw.transform(column);
	dv.ivar(t, [
		{name:'direction', initial:dw.DOWN},{name:'method', initial:dw.COPY},{name:'row', initial:undefined}
	])

	t.description_length = function(){
		if(t._row){
			return t._row.description_length();
		}
		return 0;
	}
	
	t.description = function(){
		return [
			'Fill',
			dw.column_clause(t, t._column, 'column', {all_columns:true}),
			dw.row_clause(t, t._row, 'row', {editor_class:'updatedColumn'}),
			'with',
			
			
			'values from',
			dw.select_clause(t, {select_options:{'right':'the left', 'left':'the right', 'up':'below', 'down':'above'}, param:'direction'})
			
		]
	}

	t.apply = function(tables, options){
		options = options || {};
		var table = t.getTable(tables),
			columns = t.columns(table),
			rows = table.rows(),
			row = t._row || dw.row(),
			values,
			start_row =  0,
			end_row =  rows,
			method = t._method,
			direction = t._direction;


		if(method === dw.COPY){
			var col, v, fillValue;
			if(direction === dw.DOWN){
				for(var c = 0; c < columns.length; ++c){
					col = columns[c];
					fillValue = undefined;
					for(var i = start_row; i < end_row; ++i){
						v = col[i];
						if(dw.is_missing(v)) {
							if(row.test(table, i)){
								col[i] = fillValue	
							}
							
						}
						else fillValue = v;
					}
				}
			}
			else if(direction === dw.RIGHT){
				for(var i = start_row; i < end_row; ++i){
					if(row.test(table, i)){
						fillValue = undefined;
						for(var c = 0; c < columns.length; ++c){
							col = columns[c];
							v = col[i];
							if(dw.is_missing(v)) col[i] = fillValue;
							else fillValue = v;
						}
					}
				}
			}
			else if(direction === dw.LEFT){
				for(var i = start_row; i < end_row; ++i){
					if(row.test(table, i)){
						fillValue = undefined;
						for(var c = columns.length-1; c >= 0; --c){
							col = columns[c];
							v = col[i];
							if(dw.is_missing(v)) col[i] = fillValue;
							else fillValue = v;
						}		
					}
				}
			}
			else if(direction === dw.UP){
				for(var c = 0; c < columns.length; ++c){
					col = columns[c]
					fillValue = undefined;					
					for(var i = end_row-1; i >= start_row; --i){
						v = col[i];
						if(dw.is_missing(v)){
							
							if(row.test(table, i)){
								col[i] = fillValue	
							}
							
						}
						else fillValue = v;
					}
				}
			}
		}	
		
		return {updatedCols:columns}
		
	}
	
	
	t.horizontal = function(){
		return t._direction===dw.LEFT || t._direction===dw.RIGHT;
	}
	
	t.well_defined = function(table){
		
		
	
	
		var columns = t.columns(table);
		
		var horizontal = t.horizontal();
	
	
					if(t._row){
						var conditions = t._row.conditions();

						if(conditions.length===1){
							if(conditions[0].name===dw.row.INDEX){
							
							}
							else if(conditions[0].name===dw.row.EMPTY){
								return false;
							}
						}
					}
	
		
		if(t.horizontal()){
	
	
	
			if(columns.length === 1){
						return false;
			}
			
			
			
			var col, seenMissingAfterNonMissing=false, seenNonMissing=false;
			

			
			if(t._row===undefined){
				

			
				if(t._direction===dw.LEFT){
					for(var i = 0; i < columns.length;++i){
						col = columns[i];
						if(dw.summary(col)['missing'].length===0){
							if(seenNonMissing){
								seenMissingAfterNonMissing=true;
								break;
							}
						}
						else{
							seenNonMissing=true;
						}
					}
				
				}
				else if(t._direction===dw.RIGHT){
					for(var i = columns.length-1; i >=0 ;--i){
						col = columns[i];
						if(dw.summary(col)['missing'].length===0){
							if(seenNonMissing){
								seenMissingAfterNonMissing=true;
								break;
							}
						}
						else{
							seenNonMissing=true;
						}
					}
				
				}
			

				if(!seenMissingAfterNonMissing) return false;
			}
			
			
		
			
			
		}
		else{
			
			var missingCols = columns.filter(function(col){
				var missing = dw.summary(col)['missing'];
				return missing.length === 0;
			});
			if(missingCols.length)
				return false;
			
	

		}
		
		
		
		
	
	
		
		return true;
	}
	
	t.enums = function(table){
		return ['direction'];
	}
	
	t.name = dw.transform.FILL;
	return t;
}
dw.filter = function(row){
	var t = dw.transform();


	row = dw.row(row);

	dv.ivar(t, [
		{name:'row', initial:row}
	]);

	t.description = function(){

		return [
			'Delete',
			dw.row_clause(t, t._row, 'row')
		]
	}

	t.description_length = function(){
		if(t._row)
			return t._row.description_length();

		return 0;
	}



	t.apply = function(tables, options){
		options = options || {};
		var table = t.getTable(tables),
			cols = table.cols(),
			rows = table.rows(),
			row = t._row,
			filteredTable = table.slice(0,0),
			effectedRows = [],
			start_row = options.start_row || 0,
			end_row = options.end_row || rows;




		for(var r = start_row; r < end_row; ++r){
			if(row.test(table, r)){
				effectedRows.push(r)
			}
			else{
				for(var c = 0; c < cols; ++c){
					col = filteredTable[c];
					col.push(table[c][r])
				}
			}
		}


		var l = table.cols();
		var names = table.names();
		var types = table.types();
		for(var c = 0; c < l; ++c){
			table.removeColumn(0);
		}
		for(var c = 0; c < l; ++c){
			table.addColumn(names[c], filteredTable[c], types[c])
		}

		return {effectedRows:effectedRows}
	}

	t.valid_columns = function(tables){
		if(t._row)
			return t._row.valid_columns(tables);

		return {valid:true}
	}


	t.well_defined = function(){
		return t._row.conditions().length
	}

	t.name = dw.transform.FILTER;
	return t;
}
dw.fold = function(column){
	var t = dw.transform(column);
	dv.ivara(t, {name:'keys', initial:[-1]})

	t.description = function(){
		return [
			'Fold',
			dw.column_clause(t, t._column, 'column'),
			' using ',
			dw.key_clause(t, t._keys.map(function(c){return c===-1?'header' : c }), 'keys', {editor_class:'fold', clean_val:function(x){return Number(x)}}),
			
			(t._keys.length===1? 'as a key' : ' as keys ')
		]
	}


	t.apply = function(tables, options){
		options = options || {};
		var table = t.getTable(tables),
			columns = t.columns(table),
			names = columns.map(function(c){return c.name}),
			rows = table.rows(),
			newIndex = 0,
			col,
			values,
			newCols,
			start_row = options.start_row || 0,
			end_row = options.end_row || rows;			


		end_row = Math.min(end_row, rows)


		var keys = columns.map(function(c){
			return t._keys.reduce(function(a, b){
				if(b===-1) a.push(dw.display_name(c.name));
				else a.push(c[b])
				return a;
			}, [])
		})
		

		

		var keyCols = dv.range(keys[0].length).map(function(k){
			var x = [];
			x.name = 'fold';
			x.type = dv.type.nominal;
			return x;
		})

		var valueCol = []; valueCol.name = 'value'; valueCol.type = dv.type.nominal;

		var updateCol;
		var foundLeft = false;
		var cols = table.filter(function(c){
			if(names.indexOf(c.name) === -1){
				return true;
			}
			else{
				if(!foundLeft) updateCol = c;
			}
			foundLeft = true;
			return false;
		}).map(function(c){
			var x = [];
			x.name = c.name;
			x.type = c.type;
			return x;
		})
		


		var v;
		
		for(var row = start_row; row < end_row; ++row){

			if(t._keys.indexOf(row)===-1){
				for(var k = 0; k < columns.length; ++k){
					for(var c = 0; c < cols.length; ++c){
						col = cols[c];
						col[newIndex] = table[col.name][row];
					}
					for(var j = 0; j < keyCols.length; ++j){
						keyCols[j][newIndex] = keys[k][j]
					}
					valueCol[newIndex] = columns[k][row]
					++newIndex;
				}
			}
		}
		


		var updateIndex = updateCol ? updateCol.index() : 0;
		

		
		while(table.cols()){
			table.removeColumn(0);
		}
		cols.forEach(function(c){
			table.addColumn(c.name, c, c.type)
		})



		keyCols.concat([valueCol]).forEach(function(c, i){

			table.addColumn(c.name, c, c.type, c.wrangler_type, c.wrangler_role, {index:updateIndex+i})
		})
			
		return {keyCols:keyCols, valueCols:[valueCol], toValueCols:columns, keyRows:t._keys}
		
		
	}

	t.well_defined = function(table){
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		return true;
	}
	



	t.name = dw.transform.FOLD;	
	return t;
}
dw.map = function(column){
	var t = dw.transform(column);
	dv.ivar(t, [{name:'result', initial:dw.COLUMN},{name:'update', initial:false},{name:'insert_position', initial:dw.INSERT_RIGHT},{name:'row', initial:undefined}])
	
	
	t.apply = function(tables, options){
		options = options || {};
		var table = t.getTable(tables),
			columns = t.columns(table),
			rows = table.rows(),
			values, valueStats = [], transformedValues, transformStats,start_row = options.start_row || 0,
			end_row = options.end_row || rows,
			row = t._row;

		
		var updater = dw.transform.tableUpdate(t, table, columns), numrows;

		
		for(var i = start_row; i < end_row; ++i){
			if(!row || row.test(table, i)){
				values = [];
				for(var c = 0; c < columns.length; ++c){
					values.push(columns[c][i]);
				}
				transformedValues = t.transform(values, table);
				valueStats.push(transformedValues.stats);
				numrows = updater.update(i, transformedValues);

				if(numrows > 1000){
					break;
				}
			}

		}
		
		updater.finish();
		
		transformStats = updater.stats();
		transformStats.valueStats = valueStats;
		return transformStats;
	}
	
	return t;
}
dw.merge = function(column){
	var t = dw.map(column);
	dv.ivar(t, [{name:'glue', initial:''}])
	
	t.transform = function(values){

		
		var	glue = t.glue();
		
		
		
		
		var v = values.filter(function(v){return v!=undefined}).join(glue)

		return [v];
	}


	t.description = function(){
		return [
			'Merge',
			dw.column_clause(t, t._column, 'column'),
			' with glue ',
			dw.input_clause(t, 'glue')
		]
	}
	
	t.well_defined = function(table){
		return t._column.length > 1;
	}

	t.name = dw.transform.MERGE;
	
	return t;
}
dw.wrap = function(row){
	var t = dw.transform();


	row = dw.row(row);

	dv.ivar(t, [
		{name:'row', initial:row}
	]);

	t.description = function(){

		return [
			'Wrap',
			dw.row_clause(t, t._row, 'row')
		]
	}

	t.description_length = function(){
		if(t._row)
			return t._row.description_length();

		return 0;
	}



	t.apply = function(tables, options){
		options = options || {};
		var table = t.getTable(tables),
			columns = t.columns(table),
			cols = table.cols(),
			rows = table.rows(),
			row = t._row,
			start_row = options.start_row || 0,
			end_row = options.end_row || rows, acc = [], data = [], newrows = 0, effectedRows = [];

				end_row = Math.min(end_row, rows)

		for(var r = start_row; r < end_row; ++r){
			if(row.test(table, r)){
				effectedRows.push(r)
				dv.range(acc.length-data.length).forEach(function(){data.push([])})
				if(acc.length){
					acc.forEach(function(d, i){
						data[i][newrows] = d
					})
					acc  = [];
					newrows++;
				}
			}
			for(var c = 0; c < cols; ++c){
				acc.push(table[c][r])
			}


		}
		dv.range(acc.length-data.length).forEach(function(){data.push([])})
		if(acc.length){
			acc.forEach(function(d, i){
				data[i][newrows] = d
			})
			acc  = [];
			newrows++;
		}





		var l = table.cols();

		for(var c = 0; c < l; ++c){
					table.removeColumn(0);
				}

		data.map(function(d,i){
			table.addColumn("wrap", d, 'nominal')
		})



		return {effectedRows:effectedRows, keyCols:dv.range(cols).map(function(c){return table[c]})}
	}

	t.valid_columns = function(tables){
		if(t._row)
			return t._row.valid_columns(tables);

		return {valid:true}
	}


	t.well_defined = function(){
		return t._row.conditions().length
	}

	t.name = dw.transform.WRAP;
	return t;
}
dw.reduce = function(column){
	var t = dw.transform(column);
	dv.ivara(t, [{name:'measures', initial:[]}])
	t.description = function(){
		return [
			'reduce',
			dw.column_clause(t, t._column),
			' with aggregates ',
			dw.column_clause(t, t._measures)
		]
	}


	t.apply = function(tables){

		var table = t.getTable(tables),
			compress = table.slice(0, table.rows(), {compress:true}),
			columns = t.columns(table),
			names = columns.map(function(c){return c.name}),
			rows = table.rows(), newIndex = 0, col, values, reduce = {};			
		
		
		var x = table.query({dims:names, vals:t._measures.map(function(m){return dv.first(m)})});



		table = dv.table([]);
		

			
		return {};
		
		
	}
	t.name = dw.transform.UNFOLD;	
	return t;
}
dw.row = function(conditions){

	var t = dw.transform();
	dv.ivara(t, [
		{name:'conditions', initial:conditions || []}
	])
	
	t.description_length = function(){
			
			if(t._conditions.length===0){
				return 0;
			}
			
			if(t._conditions.length === 1){
				switch(t._conditions[0].name){
					case dw.row.INDEX:
						return 1				
					case dw.row.EMPTY:
						return 2;
					default:
						break
				}


			}

			return 3;
	}
	

	
	t.description = function(){
		if(t._conditions.length === 1){
			switch(t._conditions[0].name){
				case dw.row.INDEX:
				case dw.row.CYCLE:
				case dw.row.EMPTY:
					return t._conditions[0].description({simple:true})				
				default:
					break
			}


		}

		return [
			' rows where ' + t._conditions.map(function(c){return c.description()}).join(' and ')
		]
	}
	
	t.formula = function(){
		return t._conditions.map(function(condition){
			return condition.description();
		}).join(' and ')
	}
	
	t.valid_columns = function(tables){
		
		var conds = t._conditions, cond, v;
		for(var i = 0; i < conds.length; ++i){
			cond = conds[i];
			v = cond.valid_columns(tables)
			if(!v.valid){
				return v;
			}
		}

		return {valid:true}
	}

	t.test = function(tables, row){
		
		var conds = t._conditions, cond;
		for(var i = 0; i < conds.length; ++i){
			cond = conds[i];
			if(!cond.test(tables, row)){
				return 0;
			}
		}
		return 1;
	}

	t.name = dw.transform.ROW;
	return t;
}

dw.row.fromFormula = function(formula){
	
	if(formula===''){
		return dw.row([])
	}
	
	var preds = formula.split(/ & /g)
	var index;
	preds = preds.map(function(pred){
		if(pred === 'row is empty'){
			return dw.empty();
		}
		if(index = pred.indexOf( 'index in (') != -1){

			var indices = pred.substring(index+9, pred.length-1);
			indices = indices.split(/,/g).map(function(i){return Number(i)-1});
			return dw.rowIndex(indices);
		}
		
		var match = pred.match(/\=|<\=|>\=|!=|is null|is not|matches role|matches type|like/)
		var op = match[0], index = match.index, cond, lhs = pred.substr(0, index).replace(/^ */, '').replace(/ *$/,''), rhs = pred.substr(index+op.length).replace(/^ * /,'').replace(/ *$/, '');
		
		
		
		
		switch(rhs){
			case 'a number':
				rhs = dw.number();
				break
			case 'a date':
				rhs = dw.date();
				break
			case 'a string':
				rhs = dw.string();
				break;
			case 'a integer':
				rhs = dw.integer();
				break;
			default:
				if(rhs[0]==="'") rhs = rhs.substring(1, rhs.length-1);
				else rhs = Number(rhs)
		}
		
		
		switch(op){
			case "=":
				cond = dw.eq(lhs, rhs, true);
				break;
			case "<":
				cond = dw.lt(lhs, rhs, true);
				break;
			case "<=":
				cond = dw.le(lhs, rhs, true);
				break;
			case ">":
				cond = dw.gt(lhs, rhs, true);
				break;
			case ">=":
				cond = dw.ge(lhs, rhs, true);
				break;
			case "!=":
				cond = dw.neq(lhs, rhs, true);
				break;	
			case "is null":
				cond = dw.is_null(lhs);
				break;				
			case "matches role":
				cond = dw.matches_role(lhs);
				break;				
			case "is not":
				
				cond = dw.matches_type(lhs, rhs);
				break;
			case "matches type":
				
				cond = dw.matches_type(lhs);
				break;
			case "~":
				cond = dw.like(lhs, rhs, true);
				break;
			default:
				throw "Invalid row predicates"
		}
		return cond;
	})

	return dw.row(preds);
}

dw.row.INDEX = 'rowIndex'
dw.row.CYCLE = 'rowCycle'
dw.row.EMPTY = 'empty'
dw.row.IS_NULL = 'is_null'
dw.row.IS_VALID = 'is_valid'
dw.row.MATCHES_ROLE = 'is_role'
dw.row.MATCHES_TYPE = 'is_type'
dw.row.STARTS_WITH = 'starts_with'
dw.row.LIKE = 'like'
dw.row.EQUALS = 'eq'
dw.row.NOT_EQUALS = 'neq'
dw.row.CONTAINS = 'contains'
dw.rowIndex = function(indices){
	var t = dw.transform();
	dv.ivara(t, [
		{name:'indices', initial:indices || []}
	])
	t.test = function(table, row){

		return t._indices.indexOf(row) != -1
	}
	
	t.description = function(o){
		o = o || {}, indices = t._indices;

		var simple = o.simple || false;
		if(simple){

			return (indices.length === 1 ? (indices[0]===-1 ? '' : 'row ') : 'rows ') + indices.map(function(i){return i===-1?'header':(i+1)}).join(',')	
		}
		else{
			
			
			
			return 'index in (' + indices.map(function(i){return i+1}).join(',') + ')'	
		}
	}
	
	t.valid_columns = function(){
		return {valid:true};
	}
	
	t.name = dw.row.INDEX;
	
	return t;
}

dw.rowCycle = function(cycle, start, end){
	var t = dw.transform();

	dv.ivar(t, [
		{name:'cycle', initial:cycle != undefined ? cycle : 1},
		{name:'start', initial:start || 0},
		{name:'end', initial:end}
	])
	

	
	t.test = function(table, row){
		var e = t.end(), s = t.start();
		if((s === undefined || row >= s) && (e === undefined || row <= e))
		return (row-s) % t.cycle() === 0;
	}
	t.description = function(o){
		o = o || {}, indices = t._indices;

		var simple = o.simple || false;
		if(simple){
			
			var qualifier = '';
			
			if(t.start() && t.end()!=undefined){
				qualifier = ' between ' + (t.start()+1) + ',' + (t.end()+1);
			}
			else if(t.start()){
				qualifier = ' starting with ' + (t.start()+1);
			}
			else if(t.end()){
				qualifier = ' before ' + (t.end()+1);
			}
			
			
			return 	' every ' + t.cycle() + ' rows ' + qualifier
		}
		else{
			
			var qualifier = '';
			
			if(t.start() && t.end()!=undefined){
				qualifier = ' between ' + (t.start()+1) + ',' + (t.end()+1);
			}
			else if(t.start()){
				qualifier = ' after ' + (t.start()+1);
			}
			else if(t.end()){
				qualifier = ' before ' + (t.end()+1);
			}
			
			
			return 'every ' + t.cycle() + ' rows' + qualifier;	
		}
	}
	
	t.valid_columns = function(){
		return {valid:true};
	}
	
	t.name = dw.row.CYCLE;
	
	return t;
}

dw.vcompare = function(lcol, value){
	var t = dw.transform();
	
	dv.ivar(t, [
		{name:'lcol', initial:lcol},{name:'value', initial:value}
	])
	
	
	t.test = function(table, row){
		return t.compare(table[t._lcol][row], value)
	}
	
	t.description = function(){
		return dw.display_name(t._lcol) + " " + t._op_str + " '"  + t._value + "'";
	}
	
	t.valid_columns = function(tables){
		if(tables[0][lcol])
			return {valid:true};
		return {valid:false, errors:['Invalid left hand side']}
	}
	
	return t;
}

dw.ccompare = function(lcol, rcol){
	var t = dw.transform();
	
	dv.ivar(t, [
		{name:'lcol', initial:lcol},{name:'rcol', initial:rcol}
	])
	
	
	t.test = function(table, row){
		return t.compare(table[lcol][row], table[rcol][row])
	}
	
	t.description = function(){
		return dw.display_name(t._lcol) + " " + t._op_str + " "  + t._rcol;
	}

	t.valid_columns = function(tables){
		if(tables[0][lcol] && tables[0][rcol])
			return {valid:true};
		return {valid:false, errors:['Invalid comparison']}
	}

	
	return t;
}

dw.compare = function(lcol, rcol, value){
	

	
	var t = value ? dw.vcompare(lcol, rcol) : dw.ccompare(lcol, rcol);
	t.default_transform = function(){
		return dw[t.name](lcol, rcol, value)
	}
	return t;
}

dw.eq = function(l, r, v){
	
	var t = dw.compare(l ,r, v);
	
	t._op_str = '='
	
	t.compare = function(a, b){
		return a === b;
	}
	
	t.name = dw.row.EQUALS;
	
	return t;
}

dw.neq = function(l, r, v){
	
	var t = dw.compare(l ,r, v);
	
	t._op_str = '!='
	
	t.compare = function(a, b){
		return a != b;
	}
	
	t.name = dw.row.NOT_EQUALS;
	
	return t;
}


dw.starts_with = function(l, r, v){
	var t = dw.compare(l ,r, v);
	
	t._op_str = 'starts with'
	
	t.compare = function(a, b){
		a = ""+a
		b = ""+b

		return a.indexOf(b)==0;
	}
	
			t.name = dw.row.STARTS_WITH;
	
	return t;
}

dw.like = function(l, r, v){
	var t = dw.compare(l ,r, true);
	t._op_str = '~'
	t.compare = function(a, b){
		a = ""+a
		b = ""+b
		return a.match(b)!=null;
	}
	
	t.name = dw.row.LIKE;
	
	return t;
}


dw.contains = function(l, r, v){
	var t = dw.compare(l ,r, v);
	
	t._op_str = 'contains'
	
	t.compare = function(a, b){
		a = ""+a
		b = ""+b

		return a.indexOf(b)!=-1;
	}
	
	t.name = dw.row.CONTAINS;
	
	return t;
}

dw.is_null = function(l, r, v){
	var t = dw.compare(l ,r, true);
	
	t._op_str = 'is null'
	
	t.compare = function(a, b){
		return dw.is_missing(a);
	}
	
	t.description = function(){

		return dw.display_name(t._lcol) + ' ' +  t._op_str;

		
	}
		t.name = dw.row.IS_NULL;	
	return t;
}



dw.matches_role = function(lcol){
	var t = dw.transform();
	
	dv.ivar(t, [
		{name:'lcol', initial:lcol}
	])
		
	t.test = function(table, row){
		return (table[lcol].wrangler_role.parse(table[lcol][row])===undefined);
	}
	
	t.description = function(){
		return dw.display_name(t._lcol) + ' does not match role';
	}
	t.name = dw.row.MATCHES_ROLE;
	
	
	t.valid_columns = function(tables){
		if(tables[0][lcol])
			return {valid:true};
		return {valid:false, errors:['Invalid comparison']}
	}
	
			
	return t;
}

dw.matches_type = function(lcol, type){
	var t = dw.transform();
	dv.ivar(t, [
		{name:'lcol', initial:lcol},
		{name:'type', initial:type}
	])
	

	t.test = function(table, row){
		var wt = t._type || table[lcol].wrangler_type;
		return !wt || (wt.parse(table[t._lcol][row])===undefined);
	}
	
	t.description = function(){
		return dw.display_name(t._lcol) + ' is not a ' + type.name;
	}

	t.valid_columns = function(tables){
		if(tables[0][lcol])
			return {valid:true};
		return {valid:false, errors:['Invalid comparison']}
	}


		t.name = dw.row.MATCHES_TYPE;	
	return t;
}




dw.is_missing = function(v){
	return v === undefined || (''+v).replace(/[ \t\n]/g, '').length === 0;
}

dw.empty = function(){
	var t = dw.transform();

	dv.ivar(t, [
		{name:'percent_valid', initial:0},
		{name:'num_valid', initial:0}
	])
	
	t.test = function(table, row){
		var v;

		var total = table.cols();
		var num_missing = 0;
		var percent_valid = t.percent_valid();
		var num_valid = t.num_valid();

		for(var c = 0; c < total; ++c){
			v = (table[c][row])
			if (dw.is_missing(v)) num_missing++;

			if ((num_missing >= (total - num_valid)) ||
					(num_missing / total) >= ((100 - percent_valid) / 100)) {
				return 1;
			}
		}

		return 0;
	}
	
	t.description = function(o){
		o = o || {};
		var simple = o.simple || false;

		var percent_valid = t.percent_valid();
		var num_valid = t.num_valid();

		if(simple){
			if (percent_valid > 0) {
				return 'rows with <= ' + percent_valid + '% values'
			}
			else if (num_valid > 0) {
				return 'rows with <= ' + num_valid + ' values'
			}
			else {
				return 'empty rows'	
			}
		}
		else{
			if (percent_valid > 0) {
				return ' row is (sort of) empty '
			}
			else if (num_valid > 0) {
				return ' row is (sort of) empty '
			}
			else {
				return ' row is empty '
			}
		}
		

	}
	
	t.valid_columns = function(tables){
	
			return {valid:true};
	
	}
	
	
	t.name = dw.row.EMPTY;
	
	return t;
}
dw.set_role = function(column, roles){
	var t = dw.transform(column);
	t._drop = true;
	dv.ivara(t, [{name:'roles', initial:roles || []}])

	t.transform = function(values){

	}


	t.description = function(){
		return [
			'Merge',
			dw.column_clause(t, t._column),
			' with glue ',
			dw.input_clause(t, t._glue)
		]
	}

	t.apply = function(tables){
		var table = t.getTable(tables),
			columns = t.columns(table);


		columns.forEach(function(col, i){
			col.wrangler_role = t._roles[i]
		})


		return {}

	}

	t.name = dw.transform.SET_ROLE;

	return t;
}

dw.set_type = function(column, types){
	var t = dw.transform(column);
	dv.ivara(t, [{name:'types', initial:types || []}])

	t.transform = function(values){

	}


	t.description = function(){
		return [
			'Merge',
			dw.column_clause(t, t._column),
			' with glue ',
			dw.input_clause(t, t._glue)
		]
	}

	t.apply = function(tables){
		var table = t.getTable(tables),
			columns = t.columns(table);


		columns.forEach(function(col, i){
			col.wrangler_type = t._types[i]
		})


		return {}

	}

	t.name = dw.transform.SET_TYPE;

	return t;
}

dw.set_name = function(column, names){
	var t = dw.transform(column);
	t._drop = true;
	dv.ivara(t, [{name:'names', initial:names || []}])
	dv.ivar(t, [{name:'header_row', initial:undefined}])
	t.transform = function(values){

	}

	t.well_defined = function(){
		return t._names.length || t._header_row != undefined
	}

	t.description = function(){
		if(t._header_row!=undefined){
			row = t._header_row;

			if(typeOf(row)==='number') row = dw.row(dw.rowIndex([t._header_row]))
			return [
				'Promote row',
				dw.key_clause(t, [t._header_row], 'header_row'),
				' to header'
			];
		}
		else{
			return [
				'Set ',
				dw.column_clause(t,  t._column, 'column', {extra_text:""}),
				' name to ',
				dw.input_clause(t, 'names')
			];
		}
	}

	t.apply = function(tables){
		var table = t.getTable(tables),
			columns = t.columns(table);

		if(t._header_row!=undefined){
			var row = table.row(t._header_row);
			table.forEach(function(c, i){
				var val = row[i];
				if(dw.is_missing(val)) val = 'undefined';
				c.setName(val)

				c.nameIsMeaningful = true;

				if(t._drop){
					c.splice(t._header_row, 1)
				}

			})

			return {promoteRows:[-1, t._header_row]}

		}
		else{
			columns.forEach(function(col, i){
				col.setName(names[i])

				col.nameIsMeaningful = true;
			})
		}

		return {}

	}

	t.name = dw.transform.SET_NAME;

	return t;
}



dw.role = function(){
	var t = dw.transform();

	t.parse = function(v){
		return true;
	}

	return t;
}

dw.role.GEO = 'geo'
dw.role.STATE = 'state'
dw.role.COUNTRY = 'country'
dw.geo = function(){

	var t = dw.role();
	t.name = dw.role.GEO;
	return t;

}

dw.geo.countries = ["AFGHANISTAN", "ÅLAND ISLANDS", "ALBANIA", "ALGERIA", "AMERICAN SAMOA",
"ANDORRA", "ANGOLA", "ANGUILLA", "ANTARCTICA", "ANTIGUA AND BARBUDA",
"ARGENTINA", "ARMENIA", "ARUBA", "AUSTRALIA", "AUSTRIA", "AZERBAIJAN", "BAHAMAS",
"BAHRAIN", "BANGLADESH", "BARBADOS", "BELARUS", "BELGIUM", "BELIZE", "BENIN",
"BERMUDA", "BHUTAN", "BOLIVIA, PLURINATIONAL STATE OF",
"BONAIRE, SINT EUSTATIUS AND SABA", "BOSNIA AND HERZEGOVINA",
"BOTSWANA", "BOUVET ISLAND", "BRAZIL", "BRITISH INDIAN OCEAN TERRITORY",
"BRUNEI DARUSSALAM",
"BULGARIA", "BURKINA FASO", "BURUNDI", "CAMBODIA", "CAMEROON", "CANADA",
"CAPE VERDE", "CAYMAN ISLANDS", "CENTRAL AFRICAN REPUBLIC", "CHAD", "CHILE", "CHINA", "CHRISTMAS ISLAND",
"COCOS (KEELING) ISLANDS", "COLOMBIA", "COMOROS", "CONGO", "CONGO, THE DEMOCRATIC REPUBLIC OF THE", "COOK ISLANDS", "COSTA RICA", "CÔTE DIVOIRE", "CROATIA", "CUBA", "CURAÇAO", "CYPRUS", "CZECH REPUBLIC", "DENMARK", "DJIBOUTI", "DOMINICA", "DOMINICAN REPUBLIC", "ECUADOR", "EGYPT",
"EL SALVADOR", "EQUATORIAL GUINEA", "ERITREA", "ESTONIA", "ETHIOPIA", "FALKLAND ISLANDS (MALVINAS)", "FAROE ISLANDS", "FIJI", "FINLAND", "FRANCE",
"FRENCH GUIANA", "FRENCH POLYNESIA", "FRENCH SOUTHERN TERRITORIES", "GABON", "GAMBIA", "GEORGIA", "GERMANY", "GHANA", "GIBRALTAR", "GREECE", "GREENLAND", "GRENADA", "GUADELOUPE", "GUAM", "GUATEMALA", "GUERNSEY", "GUINEA", "GUINEA-BISSAU", "GUYANA", "HAITI", "HEARD ISLAND AND MCDONALD ISLANDS", "HONDURAS",
"HONG KONG", "HUNGARY", "ICELAND", "INDIA", "INDONESIA", "IRAN, ISLAMIC REPUBLIC OF", "IRAQ", "IRELAND", "ISLE OF MAN", "ISRAEL", "ITALY", "JAMAICA", "JAPAN", "JERSEY", "JORDAN",
"KAZAKHSTAN", "KENYA", "KIRIBATI", "KOREA, DEMOCRATIC PEOPLES REPUBLIC OF", "KOREA, REPUBLIC OF", "KUWAIT", "KYRGYZSTAN", "LAO PEOPLES DEMOCRATIC REPUBLIC", "LATVIA", "LEBANON", "LESOTHO", "LIBERIA",
"LIBYAN ARAB JAMAHIRIYA", "LIECHTENSTEIN", "LITHUANIA", "LUXEMBOURG", "MACAO", "MACEDONIA, THE FORMER YUGOSLAV REPUBLIC OF", "MADAGASCAR", "MALAWI", "MALAYSIA", "MALDIVES", "MALI", "MALTA", "MARSHALL ISLANDS", "MARTINIQUE", "MAURITANIA", "MAURITIUS", "MAYOTTE", "MEXICO", "MICRONESIA, FEDERATED STATES OF", "MOLDOVA, REPUBLIC OF", "MONACO", "MONGOLIA", "MONTENEGRO", "MONTSERRAT", "MOROCCO", "MOZAMBIQUE", "MYANMAR",
"NAMIBIA", "NAURU", "NEPAL", "NETHERLANDS", "NEW CALEDONIA", "NEW ZEALAND", "NICARAGUA", "NIGER", "NIGERIA",
"NIUE", "NORFOLK ISLAND", "NORTHERN MARIANA ISLANDS", "NORWAY", "OMAN", "PAKISTAN", "PALAU", "PALESTINIAN TERRITORY, OCCUPIED", "PANAMA", "PAPUA NEW GUINEA", "PARAGUAY", "PERU", "PHILIPPINES", "PITCAIRN", "POLAND", "PORTUGAL", "PUERTO RICO", "QATAR", "RÉUNION", "ROMANIA", "RUSSIA", "RUSSIAN FEDERATION", "RWANDA",
"SAINT BARTHÉLEMY", "SAINT HELENA, ASCENSION AND TRISTAN DA CUNHA", "SAINT KITTS AND NEVIS", "SAINT LUCIA", "SAINT MARTIN (FRENCH PART)", "SAINT PIERRE AND MIQUELON", "SAINT VINCENT AND THE GRENADINES", "SAMOA", "SAN MARINO", "SAO TOME AND PRINCIPE", "SAUDI ARABIA", "SENEGAL", "SERBIA", "SEYCHELLES", "SIERRA LEONE", "SINGAPORE", "SINT MAARTEN (DUTCH PART)", "SLOVAKIA", "SLOVENIA", "SOLOMON ISLANDS", "SOMALIA", "SOUTH AFRICA",
"SOUTH GEORGIA AND THE SOUTH SANDWICH ISLANDS", "SPAIN", "SRI LANKA", "SUDAN", "SURINAME", "SVALBARD AND JAN MAYEN", "SWAZILAND", "SWEDEN", "SWITZERLAND", "SYRIAN ARAB REPUBLIC",
"TAIWAN, PROVINCE OF CHINA", "TAJIKISTAN", "TANZANIA, UNITED REPUBLIC OF", "THAILAND", "TIMOR-LESTE", "TOGO", "TOKELAU", "TONGA", "TRINIDAD AND TOBAGO", "TUNISIA", "TURKEY",
"TURKMENISTAN", "TURKS AND CAICOS ISLANDS", "TUVALU", "UGANDA", "UKRAINE", "UNITED ARAB EMIRATES",
"UNITED KINGDOM", "UNITED STATES", "UNITED STATES MINOR OUTLYING ISLANDS", "URUGUAY", "UZBEKISTAN", "VANUATU",
"VATICAN CITY STATE", "VENEZUELA, BOLIVARIAN REPUBLIC OF", "VIET NAM", "VIRGIN ISLANDS, BRITISH", "VIRGIN ISLANDS, U.S.", "WALLIS AND FUTUNA", "WESTERN SAHARA", "YEMEN", "ZAMBIA", "ZIMBABWE"]

dw.geo.countries_abbrev = ["AF", "AX", "AL", "DZ", "AS", "AD", "AO", "AI", "AQ", "AG", "AR", "AM", "AW", "AU", "AT", "AZ", "BS", "BH", "BD", "BB", "BY", "BE", "BZ", "BJ", "BM", "BT", "BO", "BQ", "BA", "BW", "BV", "BR", "IO", "BN", "BG", "BF", "BI", "KH",
"CM", "CA", "CV", "KY", "CF", "TD", "CL", "CN", "CX", "CC", "CO", "KM", "CG", "CD", "CK", "CR", "CI", "HR", "CU", "CW", "CY", "CZ", "DK", "DJ", "DM", "DO", "EC", "EG", "SV", "GQ", "ER", "EE", "ET", "FK", "FO", "FJ", "FI", "FR", "GF", "PF", "TF", "GA", "GM", "GE", "DE", "GH", "GI", "GR", "GL", "GD", "GP", "GU", "GT",
"GG", "GN", "GW", "GY", "HT", "HM", "HN", "HK", "HU", "IS", "IN", "ID", "IR", "IQ", "IE", "IM", "IL", "IT", "JM", "JP", "JE", "JO", "KZ", "KE", "KI", "KP", "KR", "KW", "KG", "LA", "LV", "LB", "LS", "LR", "LY", "LI", "LT", "LU", "MO", "MK", "MG", "MW", "MY",
"MV", "ML", "MT", "MH", "MQ", "MR", "MU", "YT", "MX", "FM", "MD", "MC", "MN", "ME", "MS", "MA", "MZ", "MM", "NA", "NR", "NP", "NL", "NC", "NZ", "NI", "NE", "NG", "NU", "NF", "MP", "NO", "OM", "PK", "PW", "PS", "PA", "PG", "PY", "PE", "PH", "PN", "PL", "PT", "PR", "QA", "RE", "RO", "RU",
"RU", "RW", "BL", "SH", "KN", "LC", "MF", "PM", "VC", "WS", "SM", "ST", "SA", "SN", "RS", "SC", "SL", "SG", "SX", "SK", "SI", "SB", "SO", "ZA", "GS", "ES", "LK", "SD", "SR", "SJ", "SZ", "SE", "CH", "SY", "TW", "TJ", "TZ", "TH", "TL", "TG", "TK", "TO", "TT",
"TN", "TR", "TM", "TC", "TV", "UG", "UA", "AE", "GB", "US", "USA", "UM", "UY", "UZ", "VU", "VA", "VE", "VN", "VG", "VI", "WF", "EH", "YE", "ZM", "ZW"]

dw.country = function(){

	var t = dw.geo();

	t.parse = function(v){
	  if (v) {
	    v = ('' + v).toUpperCase();
	  }
		return dw.geo.countries.indexOf(v) != -1 || dw.geo.countries_abbrev.indexOf(v) != -1
	}

	t.name = dw.role.COUNTRY;
	return t;

}

dw.geo.states = ['Alabama', 'Alaska',
'American Samoa',
'Arizona',
'Arkansas',
'California',
'Colorado',
'Connecticut',
'Delaware',

'Florida',
'Georgia',
'Guam',
'Hawaii',
'Idaho',
'Illinois',
'Indiana',
'Iowa',
'Kansas',
'Kentucky',
'Louisiana',
'Maine',
'Maryland',
'Massachusetts',
'Michigan',
'Minnesota',
'Mississippi',
'Missouri',
'Montana',
'Nebraska',
'Nevada',
'New Hampshire',
'New Jersey',
'New Mexico',
'New York',
'North Carolina',
'North Dakota',
'Northern Marianas Islands',
'Ohio',
'Oklahoma',
'Oregon',
'Pennsylvania',
'Puerto Rico',
'Rhode Island',
'South Carolina',
'South Dakota',
'Tennessee',
'Texas',
'Utah',
'Vermont',
'Virginia',
'Virgin Islands',
'Washington',
'West Virginia',
'Wisconsin',
'Wyoming']

dw.geo.states_abbrev = [
'AL',
'AK',
'AS',
'AZ',
'AR',
'CA',
'CO',
'CT',
'DE',
'DC',
'FM',
'FL',
'GA',
'GU',
'HI',
'ID',
'IL',
'IN',
'IA',
'KS',
'KY',
'LA',
'ME',
'MH',
'MD',
'MA',
'MI',
'MN',
'MS',
'MO',
'MT',
'NE',
'NV',
'NH',
'NJ',
'NM',
'NY',
'NC',
'ND',
'MP',
'OH',
'OK',
'OR',
'PW',
'PA',
'PR',
'RI',
'SC',
'SD',
'TN',
'TX',
'UT',
'VT',
'VI',
'VA',
'WA',
'WV',
'WI',
'WY'];

dw.state = function(){

	var t = dw.geo();

	t.parse = function(v){
		return dw.geo.states.indexOf(v)!=-1
	}

	t.name = dw.role.STATE;
	return t;

}


dw.type = function(){
	var t = dw.transform();

	t.missing = dw.is_missing;

	return t;
}


dw.number = function(){

	var t = dw.type();

	t.parse = function(v){
		var n = Number(v)
		if(isNaN(n) || (!n && (v+'').indexOf('0')===-1)) return undefined
		return n;
	}

	t.comparison = function(){
		return dw.merge_sort.numberCompare;
	}

	t.name = dw.transform.NUMBER;
	return t;

}

dw.string = function(){
	var t = dw.type();

	t.parse = function(v){
		return v;
	}
	t.name = dw.transform.STRING;

	t.comparison = function(){
		return dw.merge_sort.stringCompare;
	}
	return t;
}

dw.date = function(){
	var t = dw.type();

	t.parse = function(v){



		var n = dw.date_parse(v);
		if(isNaN(n)) return undefined;
		return n;
	}

	t.comparison = function(){
		return dw.merge_sort.getDateComparison();
	}

	t.name = dw.transform.DATE;
	return t;
}

dw.integer = function(){
	var t = dw.type();

	t.parse = function(v){
		var n = Number(v)
		if(isNaN(n) || (!n && (v+'').indexOf('0')===-1) || parseInt(n)!=n) return undefined
		return n;
	}
	t.comparison = function(){
		return dw.merge_sort.numberCompare;
	}
	t.name = dw.transform.INT;

	return t;
}




dw.summary = function(col){

	var summary;
	if(summary = cache.get(undefined, col)){

		return summary;
	}



	var type = col.wrangler_type || dw.string(), badParse = [], badRole = [], missing = [], valid = [], unique = {};
	var role = col.wrangler_role;
	for(var i = 0; i < col.table.rows(); ++i){
		var v = col[i];
		var vwrap = {index:i, value:v}

		if(type.missing(v)){
			missing.push(i)
		}
		else if(type.parse(v)===undefined){
			badParse.push(i)
		}



		else{
			valid.push(i)
		}

		unique[v] = 1;
	}

	summary = {missing:missing, bparse:badParse, brole:badRole, valid:valid, unique:unique};
	cache.set(undefined, col, summary)
	return summary;
}

dw.summary.clear_cache = function(){
	cache.clear();
}

dw.summary.cache = function(){
	var cache = [];

	cache.set = function(table, col, summary){
		cache[col.name] = summary;
		return cache;
	}

	cache.get = function(table, col){
		return cache[col.name];
	}

	cache.clear = function(){
		cache = [];
	}

	return cache;
}

var cache = dw.summary.cache();

dw.LEFT = 'left';
dw.UP = 'up';
dw.DOWN = 'down';
dw.RIGHT = 'right';

dw.translate = function(column){
	var t = dw.transform(column);
	dv.ivar(t, [
		{name:'direction', initial:dw.DOWN},{name:'values', initial:1}
	])

	t.description_length = function(){
		if(t._row){
			return t._row.description_length();
		}
		return 0;
	}
	
	t.description = function(){
		return [
			'Translate',
			dw.column_clause(t, t._column),
			dw.select_clause(t, {select_options:{'up':'up', 'down':'down'}, param:'direction'})
			
			
		]
	}

	t.apply = function(tables){
		var table = t.getTable(tables),
			columns = t.columns(table),
			rows = table.rows(),
			row = t._row || dw.row(),
			values,
			method = t._method,
			direction = t._direction;
		
		var newCols = []
		columns.forEach(function(col){
			var index = col.index();
			
			if(t._direction===dw.DOWN)
			var newValues;
			switch(t._direction){
				case dw.DOWN:
					newValues = col.slice(0);
					newValues.unshift(undefined);
					break
				case dw.UP:
					newValues = col.slice(1);
					break;
			}

			newValues.name = 'translate'
			newValues.type = col.type;
			newValues.wrangler_type = col.wrangler_type;
			newValues.wrangler_role = col.wrangler_role;
			table.addColumn(newValues.name, newValues, newValues.type, newValues.wrangler_type, newValues.wrangler_role, {index:index+1});	
			newCols.push(newValues)
		})
		
		return {newCols:newCols}
		
	}
	
	

	
	
	t.enums = function(table){
		return ['direction'];
	}
	
	t.name = dw.transform.TRANSLATE;
	return t;
}
dw.split = function(column){
	var t = dw.textPattern(column);

	dv.ivar(t, [
		{name:'quote_character', initial:undefined}
	])



	t._drop = true;
	t.transform = function(values){
		if(t._positions && t._positions.length){
			if(values[0]===undefined) return []
			var val = ""+values[0]
			var indices = t._positions;
			var startIndex = indices[0], endIndex = indices[1] || indices[0];
			var splitValues = [];
			splitValues.push(val.substring(0, startIndex))
			splitValues.push(val.substring(endIndex))
			splitValues.stats = [{splits:[{start:startIndex, end:endIndex}]}]
			return splitValues;
		}
		else{
			var ignore_between, qc;
			if((qc = t._quote_character) != undefined){
				ignore_between = new RegExp(qc+'[^'+qc+']*'+qc);
			}
			var params = {which:t._which, max_splits:t._max, before:t._before,after:t._after,on:t._on,ignore_between:ignore_between || t._ignore_between}
			var splits = dw.regex.match(values[0], params);
			

			
			var splitValues = [];
			splitValues.stats = [];
			for(var i = 0; i < splits.length; ++i){
				if(i%2==0){
					splitValues.push(splits[i].value)
				}
				else{
					splitValues.stats.push({splits:[{start:splits[i].start, end:splits[i].end}]})
				}
			}
			
			
			return splitValues;
		}
	}
	
	t.description = function(table){
		
		var description = [
			'Split',
			dw.column_clause(t, t._column, 'column', {editor_class:'none'})
		]


		if(Number(t._max) === 0){

			description = description.concat(dw.select_clause(t, {select_options:{'0':'repeatedly','1':'once'},param:'max'}))
		}
		regex = t.match_description();

	
		description = description.concat(regex)

		if(t._result === dw.ROW){
			description = description.concat(' into ')
			description = description.concat(dw.select_clause(t, {select_options:{'row':'rows'}, param:'result'}))
		}

		
		
		return description;
		
	}
	
	

	t.name = dw.transform.SPLIT;
	
	return t;
}
dw.textPattern = function(column){
	var t = dw.map(column);
	dv.ivar(t, [
		{name:'on', initial:undefined},{name:'before', initial:undefined},{name:'after', initial:undefined},{name:'ignore_between', initial:undefined},
		{name:'which', initial:1},{name:'max', initial:1}
	])
	dv.ivara(t, [{name:'positions', initial:undefined}])


	t.well_defined = function(){
		return ((t._positions && !t._on && !t._before && !t._after) || (!t._positions && (t._on || t._before || t._after))) && !t._row;
	}
	
	t.description_length = function(){
		if(t._positions) return 0;
		var score = dw.regex.description_length(t._on)+ dw.regex.description_length(t._before) + dw.regex.description_length(t._after)
		


		return score;
		
	}
	
	
	t.check_validity = function(tables){
		var x = t.valid_columns(tables);
		if(x.valid) {
			
			if(t.well_defined()){
				return {valid:true}
			}
			else{
				return {valid:false, errors:['Must define split criteria']}
			}
		}
		else{
			return x;
		}
	}
	
	t.match_description = function(options){
		options = options || {}
		var description = [];
		
		if(t._positions){
			return [
				'between positions',
				dw.column_clause(t, t._positions, options)
			]
		}
		
		
		if(t._on && t._on.toString()!="/.*/"){

			description = description.concat(
				[
					'on',
					dw.regex_clause(t,'on', options)
				]
				
			)
		}
		if(t._before && !t._after){
			description = description.concat(
				[
					'before',
					dw.regex_clause(t, 'before', options)
				]
			)
		}		
		if(t._after && !t._before){
			description = description.concat(
				[
					'after',
					dw.regex_clause(t, 'after', options)
				]
			)
		}
		if(t._after && t._before){
			description = description.concat(
				[
					'between',
					dw.regex_clause(t, 'after', options),
					'and',
					dw.regex_clause(t, 'before', options)
				]
			)
		}
		return description;
	}
			
	return t;
}
dw.edit = function(column){
	var t = dw.textPattern(column);
	t._update = true;
	dv.ivar(t, [{name:'to', initial:undefined},{name:'update_method', initial:undefined}])
	

	
	
	t.transform = function(values){
		var mapper;
		if(t._to != undefined){
			mapper = function(){return t._to;};
		}
		else if(t._update_method){
			
			switch(t._update_method){
				case dw.edit.upper:
					mapper = function(v){
						if(v!=undefined){}
							return v.toUpperCase();						
					}
					break
				case dw.edit.lower:
					mapper = function(v){
						if(v!=undefined){
							return v.toLowerCase();	
						}
							
					}
					break
				case dw.edit.capitalize:
					mapper = function(v){
						if(v!=undefined)
							if(v.length >= 1)
								return v[0].toUpperCase()+v.substr(1);						
						}
					break
				case dw.edit.uncapitalize:
					mapper = function(v){
						if(v!=undefined)
							if(v.length >= 1)
								return v[0].toLowerCase()+v.substr(1);						
						}
					break
				default:
					throw('Illegal update update_method');
			}
		}
		
		return values.map(function(v){return v!=undefined?''+v:undefined}).map(mapper);
		
	}
	
	t.description = function(){
		
		var description = [
			'Edit',
			dw.column_clause(t, t._column, 'column', {editor_class:'none'})
		]
		
		regex = t.match_description();

	
		description = description.concat(regex)

		if(t._row){
			description.push(dw.row_clause(t, t._row, 'row'))
		}
		
		if(t._to != undefined){
			description.push(' to \'')
			description.push(dw.input_clause(t, 'to'));			
			description.push('\'')
		}
		
		else if(t._update_method){
			description.push(dw.select_clause(t, {select_options:{'LOWER':' to lowercase','CAPITALIZE':' capitalize','UNCAPITALIZE':' uncapitalize', 'UPPER':'to uppercase'}, param:'update_method'}))
		}
		
		
		
		return description;
		
	}
	
	t.well_defined = function(){

		return (t._column.length===1 && (t._to != undefined || t._update_method != undefined));
	}
	
	

	t.name = dw.transform.EDIT;
	
	return t;
}

dw.edit.upper = 'UPPER';
dw.edit.lower = 'LOWER';
dw.edit.capitalize = 'CAPITALIZE';
dw.edit.uncapitalize = 'UNCAPITALIZE';
dw.INSERT_RIGHT = 'right';
dw.INSERT_END = 'end';


dw.ROW = 'row';
dw.COLUMN = 'column';

dw.clause = {
	column: 'column',
	regex: 'regex',
	input: 'input',
	array: 'array',
	select: 'select'
};

dw.status = {
	active: 'active',
	inactive: 'inactive',
	deleted: 'deleted',
	invalid: 'invalid'
}

dw.transform = function(column){
	var t = {};

	t.is_transform = true;

	dv.ivara(t, {name:'column', initial:(column!=undefined)?column:[]})
	dv.ivar(t, [{name:'table', initial:0},{name:'status', initial:dw.status.active},{name:'drop', initial:false}])
	t.getTable = function(tables){
		return tables[t.table()]
	}


	t.show_details = false;

	t.active = function(){
		return t._status === dw.status.active;
	}

	t.inactive = function(){
		return t._status === dw.status.inactive;
	}

	t.deleted = function(){
		return t._status === dw.status.deleted;
	}

	t.invalid = function(){
		return t._status === dw.status.invalid;
	}

	t.toggle = function(){
		t.active() ? t.status(dw.status.inactive) : t.status(dw.status.active);
	}

	t.delete_transform = function(){
		t._status = dw.status.deleted;
	}

	t.errors = [];

	t.invalidate = function(errors){
		t._status = dw.status.invalid;
		t.errors = errors;
	}

	t.validate = function(){
		t._status = dw.status.active;
		t.errors = []
	}

	t.errorMessage = function(){
		return t.errors.join('\n')
	}

	t.columns = function(table){
		if(t._column && t._column.length)
			return t._column.map(function(c){
				return table[c];
			})
		return table.map(function(c){return c})

	}

	t.has_parameter = function(p){
		return t[p] != undefined;
	}

	t.well_defined = function(table){
		return true;
	}

	t.params = function(){
		return dv.keys(t).filter(function(k){return k[0]==='_'})
	}

	t.enums = function() {
		return [];
	}


	t.param_equals = function(l, r){

		if(l===undefined || r === undefined) return l === r;
		ktype = typeOf(l);
		switch(ktype){
			case 'function':
				return l.toString() === r.toString();
			case 'array':
				if(l.length!=r.length) return false;
				for(var i = 0; i < l.length; ++i){
					if(!t.param_equals(l[i], r[i])) return false;
				}
				return true;
			case 'object':
				if(l.equals) return l.equals(r);
				return l.toString() === r.toString();
			case 'number':
			case 'string':
			case 'boolean':
				return l === r;
			default:
				return l === r;
		}
	}

	t.similarity = function(other){
		var nameShift = (t.name!=other.name) ? -1 : 0;
		var tkeys = t.params(), okeys = other.params(), l, r, ktype, equalCount=0;
		for(var i = 0; i < tkeys.length; ++i){
			l = t[tkeys[i]], r = other[okeys[i]];
			if(t.param_equals(l, r)){
				equalCount++;
			}
		}
		return nameShift+(equalCount/tkeys.length);
	}

	t.equals = function(other){
		if(t.name!=other.name) return false;
		var tkeys = t.params(), okeys = other.params(), l, r, ktype;
		for(var i = 0; i < tkeys.length; ++i){
			l = t[tkeys[i]], r = other[okeys[i]];
			if(!t.param_equals(l, r)){
				return false;
			}
		}
		return true;
	}

	t.check_validity = function(tables){
		return t.valid_columns(tables)
	}

	t.valid_columns = function(tables){
		var table = t.getTable(tables);
		var columns = t.columns(table).filter(function(c){return c===undefined});
		if(columns.length){
			return {valid:false, errors : ['Invalid columns']}
		}
		return {valid:true}

	}

	t.clone_param = function(param){
			var ktype = typeOf(param);
			switch(ktype){
				case 'function':
					return param
				case 'array':
					return param.map(function(p){return t.clone_param(p)})
				case 'object':
					if(param.clone) return param.clone();
					return param;
				case 'number':
				case 'string':
				case 'boolean':
					return param;
				default:
					return param;
			}
	}


	t.default_transform = function(){
		return dw.transform.create(t.name)
	}

	t.clone = function(){
		var other = t.default_transform(),
				tkeys = t.params(), param;
		for(var i = 0; i < tkeys.length; ++i){
			param = t[tkeys[i]];
			other[tkeys[i]] = t.clone_param(param);
		}
		return other;
	}

	t.description_length = function(){
		return 0;
	}

	t.comment = function(){
		var clauses = t.description();

		return clauses.map(function(clause){
			if(typeOf(clause)==='string')
				return clause

			return clause.description();

		}).join(' ')


	}

	t.sample_apply = function(tables){
		return t.apply(tables, {max_rows:1000, warn:true})
	}

	return t;
}

dw.transform.create = function(name){
	return dw[name]()
}



dw.transform.tableUpdate = function(transform, table, columns){

	var tu = {}, result = transform.result();



	if(result === dw.COLUMN){
		tu.finish = function(){
			if(transform._drop){
				transform.columns(table).forEach(function(c){
					table.removeColumn(c.name)
				})
			}
		}
		if(transform.update()){
			tu.update = function(row, values){
				for(var i = 0; i < values.length; ++i){
					columns[i][row] = values[i];
				}
			}
			tu.stats = function(){
				return {updatedCols:columns}
			}
		}
		else{
			var newCols = [];
			var insertPreference = transform.insert_position()
			var insertPosition;

			switch(insertPreference){
				case dw.INSERT_RIGHT:
					insertPosition = columns[columns.length-1].index();
					break
				case dw.INSERT_END:
					insertPosition = table.length;
					break
			}

			tu.update = function(row, values){
				var i =0;
				for(i; i < values.length; ++i){
					if(i === newCols.length){
						var newCol = [];
						newCols.push(newCol)
						table.addColumn(transform.name, newCol, dv.type.nominal, dw.string(), dw.role(), {index:insertPosition+newCols.length});
					}
					newCols[i][row] = values[i];
				}


				if (values.length===0) {
					for (var i = 0; i < newCols.length; i++) {
						newCols[i][row] = undefined;
					}
				}

			}
			tu.stats = function(){
				return {newCols:newCols}
			}
		}
	}
	else if(result === dw.ROW){
		var newIndex = 0;
		var cols = table.map(function(c){
			var x = [];
			x.name = c.name;
			x.type = c.type;
			return x;
		})
		var updateIndex = columns[0].index();
		var v;
		tu.update = function(row, values){
			for(var i = 0; i < values.length; ++i){
				for(var c = 0; c < updateIndex; ++c){
					cols[c][newIndex] = table[c][row];
				}
				for(c = updateIndex+1; c < cols.length; ++c){
					cols[c][newIndex] = table[c][row];
				}
				cols[updateIndex][newIndex] = values[i]
				++newIndex;
			}
			return cols[0].length;
		}

		tu.finish = function(){
			while(table.cols()){
				table.removeColumn(0)
			}
			cols.forEach(function(c){
				table.addColumn(c.name, c, c.type)
			})
		}

		tu.stats = function(){
			return {}
		}
	}


	return tu;

}

dw.transform.SPLIT = 'split';
dw.transform.EXTRACT = 'extract';
dw.transform.CUT = 'cut';

dw.transform.MERGE = 'merge';
dw.transform.FOLD = 'fold';
dw.transform.UNFOLD = 'unfold';
dw.transform.FILL = 'fill';
dw.transform.FILTER = 'filter';
dw.transform.DROP = 'drop';
dw.transform.ROW = 'row';
dw.transform.COPY = 'copy';
dw.transform.LOOKUP = 'lookup';
dw.transform.TRANSLATE = 'translate';
dw.transform.EDIT = 'edit';
dw.transform.SORT = 'sort';
dw.transform.TRANSPOSE = 'transpose';
dw.transform.STRING = 'string';
dw.transform.INT = 'int';
dw.transform.NUMBER = 'number';
dw.transform.DATE = 'date';
dw.transform.SET_TYPE = 'set_type';
dw.transform.SET_ROLE = 'set_role';
dw.transform.SET_NAME = 'set_name';
dw.transform.WRAP = 'wrap';
dw.transform.MARKDOWN = 'markdown';
dw.unfold = function(column){
	var t = dw.transform(column);
	dv.ivar(t, [{name:'measure', initial:undefined}])
	t.description = function(){
		return [
			'Unfold',
			dw.column_clause(t, t._column, 'column', {editor_class:'unfold', single:true}),
			' on ',
			dw.column_clause(t, [t._measure], 'measure' ,{single:true})
		]
	}



	t.apply = function(tables, options){
		options = options || {};
		var table = t.getTable(tables),
			columns = t.columns(table),
			toHeaderNames = columns.map(function(c){return c.name}),
			keyColumns = table.filter(function(c){return toHeaderNames.indexOf(c.name)===-1 && t._measure != c.name}),
			rows = table.rows(), newIndex = 0,
			valueCol = table[t._measure],
			max_rows = options.max_rows || 1000,
			start_row = options.start_row || 0, end_row = options.end_row || rows;
		
		end_row = Math.min(rows, end_row);

		
		
		end_row = rows;

		var headerColumn = columns[0];

		var newColumnHeaders = [];
		headerColumn.forEach(function(e) {
			if (newColumnHeaders.indexOf(e) === -1) {
				newColumnHeaders.push(e);
			}
		});
	

		var new_table = [];
		keyColumns.forEach(function(e) {new_table.push([]);});

		newColumnHeaders.forEach(function(e, i) {
			var col = [];
			
			col.name = e;
			new_table.push(col);
		});

		var reduction = {};
		var reduction_index = 0;
		for (var r = start_row; r < end_row; r++) {
			var key = keyColumns.map(function(e){return e[r];}).join('*');
			if (reduction[key]===undefined) {
				reduction[key] = reduction_index;

				for (var i = 0; i < keyColumns.length; i++) {
					var col = keyColumns[i];
					new_table[i][reduction_index] = col[r];
				}
				reduction_index += 1;
			}

			index = reduction[key];
			header = headerColumn[r];
			measure = valueCol[r];

			new_table[keyColumns.length + newColumnHeaders.indexOf(header)][index] = measure;
		}

		var length = table.cols();
		for(var i = 0; i < length; ++i){
			table.removeColumn(0);
		}

		var name, valueCols = [];
		new_table.forEach(function(col, i) {
			if (i < keyColumns.length) {
				name = keyColumns[i].name;
			}
			else {
				name = col.name
			}
			table.addColumn(name, col, dv.type.nominal);
			
			if (i >= keyColumns.length) {
				valueCols.push(col.name);
			}
		});

		
		
		for (var i = 0; i < table.cols(); i++) {
			if (i >= keyColumns.length) {
				table[i].nameIsMeaningful = true;
			}
		}

		return { toKeyRows:[-1], toHeaderCols:columns, toValueCols:[valueCol], valueCols:valueCols.map(function(c){return table[c]}).filter(function(c){return c!=undefined})};
	}




	
	t.description_length = function(){
		return 0;
	}
	
	t.well_defined = function(table){
		if(t._column && t._column.length === 1 && t._measure && t._measure != t._column[0] && (!table || table.length >= 3)){
			
			var col = table[t._column[0]];
			
			
			
			

			return true;
		}
		
		return false;
	}
	
	t.check_validity = function(tables){
		var x = t.valid_columns(tables);
		if(x.valid) {
			var col = t.getTable(tables)[t._measure]
			if(col){
				return {valid:true}
			}
			else{
				return {valid:false, errors:['Invalid Measure']}
			}
		}
		else{
			return x;
		}
	}
	
	
	t.name = dw.transform.UNFOLD;	
	return t;
}

dw.transpose = function(column){
	var t = dw.transform(column);
	dv.ivara(t, [

	])

	t.description_length = function(){		
		return 0;
	}
	
	t.description = function(){
		var d = [
			'Transpose table'
		]
		return d;
	}

	t.apply = function(tables, options){
		options = options || {};
		var table = t.getTable(tables),
			columns = t.columns(table),
			rows = table.rows(),
			row = t._row || dw.row(),
			values,
			start_row =  0,
			end_row =  rows,
			method = t._method,
			direction = t._direction;

		var newcols = dv.range(0, table.rows()).map(function(r){
			var x = [];
			x.name = 'transpose';
			x.type = dv.type.nominal;
			return x;
		});
		
		for(var c = 0; c < table.cols(); ++c){
			var col = table[c];
			for(var r = 0; r < table.rows(); ++r){
				newcols[r][c] = col[r];
			}
		}
		
		while(table.cols()){
			table.removeColumn(0);
		}
		
		newcols.forEach(function(c){
			table.addColumn(c.name, c, c.type, dw.string(), undefined, {})
		})
		
		
		return {};
	}
	
	
	
	t.name = dw.transform.TRANSPOSE;
	return t;
}


dw.sort = function(column){
	var t = dw.transform(column);
	dv.ivara(t, [
		{name:'direction', initial:[]},
		{name:'as_type', initial:[]}
	])

	t.description_length = function(){
		
		return 0;
	}
	
	t.description = function(){
		var d = [
			'Sort by ',
			dw.column_clause(t, t._column, 'column')
		]
		if(t._direction && t._direction.length && t._direction[0]==='desc'){
			d.push(t._direction[0])
		}
		return d;
	}

	t.apply = function(tables, options){
		options = options || {};
		var table = t.getTable(tables),
			columns = t.columns(table),
			rows = table.rows(),
			row = t._row || dw.row(),
			values,
			start_row =  0,
			end_row =  rows,
			method = t._method,
			direction = t._direction;



		var comparisons = columns.map(function(c){
			var ct = t._as_type[0] || c.wrangler_type;
			if(!ct){
				return dw.stringCompare;
			}
			
			return ct.comparison();
			
		})

		var directions = [];
		for(var i = 0; i < columns.length;++i){
			if(t._direction[i]==='desc') directions.push(-1);
			else directions.push(1);
		}

		
		var sortFunction = function(a, b){
			for(var i = 0; i < comparisons.length;++i){
				var col = columns[i];

				var result = comparisons[i](col[a], col[b]);
				if(result != 0){
					return directions[i]*result;
				}
			}
			if(a < b) return -1;
			if(a==b) return 0;
			return 1;
		}
			
		var sortedRows = dw.merge_sort(dv.range(0, table.rows()), sortFunction);	
	
		var newTable = table.slice();
		for(var col = 0; col < table.length; ++col){
			var column = table[col];
			var newColumn = newTable[col]
			for(var row = 0; row < table.rows(); ++row){
				column[row] = newColumn[sortedRows[row]];
			}
		}
		
		
			
		return {updatedCols:columns}
		
	}
	
	
	
	t.name = dw.transform.SORT;
	return t;
}


dw.preview = function(vtable, table, transform, afterTable, tableSelection){


	var visible_rows = vtable.visible_rows(), start_row = Math.max(0, visible_rows[0]), end_row = Math.max(0, visible_rows[1]);

	var sample = table.slice(), wrangler = dw.wrangle(), tableNames = table.names(), inputColumn, original, updated, colIndex;
	
	
	
	
	
	
	
	

	vtable.container().removeClass('previewBeforeTable').removeClass('updatedTable')
	afterTable.container().removeClass('previewAfterTable').addClass('hidden').hide()
	


	
	if(transform){
		var oldDrop = transform.drop();
		transform.drop(false);
		
		if(transform.name===dw.transform.CUT) transform.update(false)
		
		var tstats = transform.apply([sample], {max_rows:1000, start_row:start_row, end_row:end_row}),
			newCols = tstats.newCols || [],
			updatedCols = tstats.updatedCols || [],
			droppedCols = tstats.droppedCols || [],
			toValueCols = tstats.toValueCols || [],
			toHeaderCols = tstats.toHeaderCols || [],
			toKeyRows = tstats.toKeyRows || [],
			keyRows = tstats.keyRows || [],
			valueCols = tstats.valueCols || [],
			keyCols = tstats.keyCols || [],
			newTables = tstats.newTables || [],
			valueStats = tstats.valueStats || [],
			promoteRows = tstats.promoteRows || [],
			splits = valueStats.map(function(v){if(v) return v[0] && v[0].splits})
			effectedRows = tstats.effectedRows || [], columnTable = sample;
			
		if(transform.name===dw.transform.CUT) transform.update(true)
			
			
			
			
		

		switch(transform.name){
			
			case dw.transform.FOLD:
			case dw.transform.UNFOLD:
			case dw.transform.WRAP:
			case dw.transform.TRANSPOSE:
				vtable.container().addClass('previewBeforeTable')			
				vtable.schema_table(table).table(table).draw()
				afterTable.table(sample).draw()
				afterTable.container().addClass('previewAfterTable').show()
				
				columnTable = table;
			break
			case dw.transform.FILTER:
				vtable.container().addClass('updatedTable')
				vtable.schema_table(table).table(table).draw()
			break
			
			case dw.transform.SPLIT:
			case dw.transform.CUT:
			case dw.transform.EXTRACT:
				highlightClass = transform.name+'Highlight'

			
			default:
				vtable.container().addClass('updatedTable')
				vtable.schema_table(table).table(sample).draw()

		}

	
		
		var colIndex, original, updated, children, node, val, split, rows;

		

		
		transform.column().forEach(function(col){
			original = columnTable[col];
			colIndex = original.index();
			children = jQuery('.updatedTable td:nth-child('+(colIndex+2)+')');

			for(var index = 0; index < splits.length; ++index){
				
				split = splits[index];
				
				if(split){
					node = children[index];
					
					if(node){
						node = node.firstChild
					}
					else{
						
						break;
					}
					val = original[index+start_row];
					

					
					if(node && val != undefined && split[0] && val.length >= split[0].end) {
						Highlight.highlight(node, split[0].start, split[0].end, {highlightClass:highlightClass})
					}
					
				}
				
				
			}
			
		})
		
		newCols.forEach(function(col){
			jQuery('.updatedTable td:nth-child('+(col.index()+2)+')').addClass( 'previewNew').addClass('unclickable');
		})
	
		droppedCols.forEach(function(col){
			jQuery('.updatedTable td:nth-child('+(col.index()+2)+')').addClass( 'previewDeleted');
		})
		
		toHeaderCols.forEach(function(col){
			colIndex = table[col.name].index()
			jQuery('.previewBeforeTable td:nth-child('+(colIndex+2)+')').addClass( 'previewSchema');
			
		})
		
		
		updatedCols.forEach(function(col){
			original = table[col.name];
			updated = sample[col.name];
			colIndex = original.index();
			var range = dv.range(0, table.rows());

			vtable.rows(function(r, i){
				if(updated[i] != undefined && original[i] !== updated[i]) r.find('td:nth-child('+(colIndex+2)+')').addClass('previewNew')
			}, range)
			
			
			
			
		})
		
		
		
		
		

		vtable.rows(function(r){
			r.addClass('previewDeleted')
		}, effectedRows)
		

		vtable.rows(function(r){
			r.addClass('previewKey')
		}, promoteRows)
		

		toValueCols.forEach(function(col){
			colIndex = table[col.name].index();
			jQuery('.previewBeforeTable td:nth-child('+(colIndex+2)+')').addClass( 'previewNew');
			
			vtable.rows(function(r, i){
				if(i===-1) jQuery('.previewBeforeTable tr:nth-child(2) th:nth-child('+(colIndex+2)+')').addClass( 'previewKey');
				else r.find('td:nth-child('+(colIndex+2)+')').addClass('previewKey')
			}, keyRows)
			
			
			
			
			
			
			
			
			
			

		})
		
		valueCols.forEach(function(col){
			colIndex = sample[col.name].index();
			jQuery('.previewAfterTable td:nth-child('+(colIndex+2)+')').addClass( 'previewNew');
			
			
			
			
			
			
				afterTable.rows(function(r, i){
					if(i===-1) jQuery('.previewAfterTable tr:nth-child(2) th:nth-child('+(colIndex+2)+')').addClass( 'previewKey');
					else r.find('td:nth-child('+(colIndex+2)+')').addClass('previewKey')
				}, toKeyRows)
			
			
			
			
			
			
			
		})
		
		keyCols.forEach(function(col){
			colIndex = sample[col.name].index();
			jQuery('.previewAfterTable td:nth-child('+(colIndex+2)+')').addClass( 'previewKey');
			
		})

	
	
	
		transform.drop(oldDrop);
	}
	else{
		

		vtable.schema_table(table).table(table).draw()
	}
	tableSelection.draw();
	
	jQuery('.unclickable').unbind('mouseup').unbind('mousedown')
}
dw.table_selection = function(vtable){
	var ts = {}, rowSelection = dw.selection(), colSelection = dw.selection();
	
	ts.add = function(selection){
		var keytype;
		if(selection.shift){
			keytype = dw.selection.shift;
		}
		else if(selection.ctrl){
			keytype = dw.selection.ctrl;
		}
		
		switch(selection.type){
			case dw.engine.row:
				rowSelection.add({type:keytype, selection:selection.position.row})
				colSelection.clear()
				break
			case dw.engine.col:
				colSelection.add({type:keytype, selection:selection.table[selection.position.col].index()})
				rowSelection.clear()
				break
			default:				
		}
		return ts;
	}
	
	ts.clear = function(){
		rowSelection.clear();
		colSelection.clear();
	}
	
	ts.draw = function(){
		vtable.rows(function(r){
			r.addClass('selected')
		}, rowSelection)
		vtable.cols(function(c){
			c.addClass('selected')
		}, colSelection)
		return ts;
	}
	
	ts.selection = function(){
		return {rows:rowSelection, cols:colSelection}
	}
	
	ts.rows = function(){
		return rowSelection.slice(0);
	}
	
	ts.cols = function(){
		return colSelection.slice(0);
	}
	
	
	
	return ts;
}

dw.table_selection.row = 'row';
dw.table_selection.col = 'col';
dw.selection = function(){
	var selection = []
	var history = [];
	selection.add = function(s){
		var e = s.selection, index, last, range;
		switch(s.type){
			case dw.selection.ctrl:
				index = selection.indexOf(e);
				if(index===-1){
					selection.push(e)
				}
				else{
					selection.splice(index, 1)
				}
				break
			case dw.selection.shift:
				if(selection.length){
					last = history[history.length-1];
					if(last.type!=dw.selection.clear){
						range = (last.selection < e) ? dv.range(last.selection, e+1) : dv.range(e, last.selection+1);
						range.forEach(function(r){
							if(selection.indexOf(r) === -1){
								selection.push(r)
							}
						})								
					}
				}
				else{
					selection.push(e)
				}
				break;
			case dw.selection.clear:
				selection.length = 0;
				break;
			default:
				selection.length = 0;
				selection.push(e)
		}
		history.push(s)
		return selection;
	}
	
	selection.clear = function(){
		selection.add({type:dw.selection.clear})
	}
	
	return selection;
}

dw.selection.ctrl = 'ctrl'
dw.selection.shift = 'shift'
dw.selection.clear = 'clear'
dw.vtable = function(container, options){
	container = jQuery(container);
	options = options || {}
	var vtable = {}, otable, table, data, columns, settings = {},
		textSelect = options.textSelect, interaction = options.interaction, ontablechange = options.ontablechange, onexecute = options.onexecute, onconfirm = options.onconfirm;




	var _schema_table;
	vtable.schema_table = function(val){
		if(arguments.length){
			_schema_table = val;


			return vtable;
		}
		else{
			if(_schema_table) return _schema_table;
			return table;
		}
	}

	var _row_offset = 0;
	vtable.row_offset = function(val){
		if(arguments.length){
			_row_offset = val;
			return vtable;
		}
		return _row_offset || 0;
	}



	dv.ivar(vtable, [{name:'page', initial:0}, {name:'page_length', initial:25}])

	vtable.table = function(t){

		var col, c, r, v;

		var max_cols = 40;
		table = t.slice_cols(0, max_cols);
		data = [];



		for(r = 0; r < t.rows();++r){
			data.push([r+1]);
		}



		for(c = 0; c < table.cols(); ++c){

			col = t[c];
			for(r = 0; r < t.rows(); ++r){
				v = col[r];
				data[r][c+1] = (v === undefined) ? '' : v;
			}
		}



		var name;
		columns = [{"sTitle":''}].concat(table.map(function(c){
			name = dw.display_name(c.name);

			return {"sTitle":name, "sWidth":"30px"}
		}))
		return vtable;
	}


	vtable.draw = function(){


		if(columns.length > 39){
			if(onconfirm)
				onconfirm({message:'You are creating a lot of columns.'})
		}

		var containerID = container.attr('id') + '_table';

		if(otable) otable.fnDestroy();







		console.log(data, columns)

		container.html( '<table cellpadding="0" cellspacing="0" border="0" class="display" id="'  + containerID + '"></table>' );
		otable = $('#'+containerID).dataTable( {
					"aaData": data,
					"aoColumns": columns,
					"aaSorting": [],
					"iDisplayStart": vtable.page()*vtable.page_length(),
					iDisplayLength: vtable.page_length(),
					aoColumnDefs: [
						{ "bSortable": false, "aTargets": ["_all"] }
						],
					"sDom": 'rt<"clear">'
															,"sScrollY": "85%"


				});





		var mousedownCell;
			var celldoubleclick = false;

			container.find('td').mouseup(function(e){



				var colIndex = jQuery(this).index();
				if(colIndex){

					if(this===mousedownCell){
						var selection = getSelection();
						var rowIndex = jQuery(this).parent();

						var position = (getPosition(this));
						var val = (table[position.col][position.row]);

						if(selection && val && val.length > selection.startCursor){
							if(selection){
								var position = getPosition(this);
								position.col = table[position.col].name
								var timeout = (selection.startCursor === selection.endCursor) ? 200 : 0;


								setTimeout(function(){
									if(!celldoubleclick){


										vtable.save_settings();
										interaction({type:dw.engine.highlight, position:position, selection:selection})


									}
								}, timeout)




							}

						}


					}

				}
				else{
					vtable.save_settings();
					interaction({type:dw.engine.row, position:getPosition(this), shift:e.shiftKey, ctrl:e.metaKey})
				}
			}).mousedown(function(e){



				Highlight.removeHighlight(e.currentTarget)
				mousedownCell = this;
			}).dblclick(function(e){
				celldoubleclick = true;
				var input = dw.jq('input'), me = this;
				jQuery(e.currentTarget).empty().append(input)
				var position = getPosition(me);
				input.css('width', '100%').css('height','16px').focus().val(table[position.col][position.row]);
				input.blur(function(){
					vtable.save_settings();
					interaction({type:dw.engine.edit, position:position, val:input.val(), table:table})
				}).keydown(function(e){
					e.stopImmediatePropagation();
					input.unbind('blur')
					switch(e.which){
						case 13:
							vtable.save_settings();
							interaction({type:dw.engine.edit, position:position, val:input.val(), table:table})
						break
					}
				})

			})




			var dblClick = false;


			container.find('.dataTables_scrollHead th').click(function(e){

				var me = this;
				setTimeout(function() {
					if(!dblClick){
						vtable.save_settings();
						interaction({type:dw.engine.col, position:{row:-1, col:table[jQuery(me).index()-1].name}, table:table, shift:e.shiftKey, ctrl:e.metaKey})
					}
				}, 200)
			}).dblclick(function(e){

				var input = dw.jq('input'), me = this;
				jQuery(e.currentTarget).empty().append(input)
				input.css('width', '100%').css('height','16px').focus().val(dw.display_name(table[jQuery(me).index()-1].name))
				input.blur(function(){
					if(dw.display_name(table[jQuery(me).index()-1].name) != dw.display_name(input.val())){
						vtable.save_settings();
						onexecute(dw.set_name(table[jQuery(me).index()-1].name, [input.val()]))
					}
				}).keydown(function(e){
					if(e.which===13){
						if(dw.display_name(table[jQuery(me).index()-1].name) != dw.display_name(input.val())){
							input.unbind('blur');
							vtable.save_settings();
							onexecute(dw.set_name(table[jQuery(me).index()-1].name, [input.val()]))
						}
					}



				})
				dblClick = true
			}).slice(1).append(dw.jq('span').addClass('column_sort_status'))

		vtable.add_sort_status();

		var summaryRow = dw.jq('tr').append(dw.jq('th')), summary;
		container.find('.dataTables_scrollHead thead').prepend(summaryRow)

		var summaries = []
		table.forEach(function(c){
			summary = dw.jq('th');
			summaryRow.append(summary);
			summaries.push(summary)
		})



		var tablewidth = vtable.width();

		container.find('table').width(tablewidth).css('overflow-x', 'hidden')
		container.width(tablewidth+30).css('max-width', tablewidth+30).css('margin','0px')


		container.find('table').parent().width(tablewidth+30).css('max-width', tablewidth+30).css('margin','0px')
		container.find('.dataTables_scrollHead').width(tablewidth+30).css('max-width', tablewidth+30).css('margin','0px')
		var colwidth = vtable.column_width();
		container.find('th,td').css('width', colwidth).css('max-width', colwidth).css('min-width', colwidth)
		container.find('th:nth-child(1),td:nth-child(1)').css('width', '30px').css('max-width', '30px').css('min-width', '30px').css('padding', '0px').addClass('rowHeader');
		container.find('.dataTables_scrollBody').css('overflow-x', 'hidden');



		table.forEach(function(c, i){
			draw_summary(summaries[i], c);
		})


		draw_type_icons()

		draw_controls()







		var scrollContainer = jQuery('.ui-layout-center');


		var left = settings['scrollLeft'];
		if(left!=undefined){
			scrollContainer.scrollLeft(left)
		}

		jQuery('.ui-layout-center').scroll(function(){
			vtable.save_settings();
		})


		return vtable;
	}

	vtable.save_settings = function(){

		var scrollContainer = jQuery('.ui-layout-center');
		settings['scrollLeft'] = jQuery(scrollContainer).scrollLeft();
	}

	vtable.column_width = function(column){
		if(table.length===1) return 800;
		else if(table.length===2) return 400;
		else if(table.length===3) return 266;
		else return 200;
	}

	vtable.width = function(){
		return table.length*vtable.column_width()+30
	}


	vtable.visible_rows = function(){

		var start_row = vtable.page()*vtable.page_length(), end_row = start_row+vtable.page_length();

		return [start_row, end_row];

	}

	vtable.add_sort_status = function(){
			container.find('.column_sort_status').addClass('column_sort_unsorted')
					if(options.wrangler){
						var sortColumn;
						var lastSort = options.wrangler.filter(function(t){return t.active() && t.name===dw.transform.SORT});
						lastSort = lastSort[lastSort.length-1];
						if(lastSort){
							sortColumn = lastSort.column();
							container.find('.column_sort_status').each(function(c, i){
								if(table[jQuery(this).parent().index()-1]===table[sortColumn]){
									if(lastSort.direction()[0]==='desc'){
										jQuery(this).removeClass('column_sort_unsorted').addClass('column_sort_desc').click(function(e){
											e.stopPropagation();
											e.stopImmediatePropagation();
											onexecute(dw.sort(table[jQuery(this).parent().index()-1].name).direction('asc').as_type([table[jQuery(this).parent().index()-1].wrangler_type]))
										})
									}
									else{
										jQuery(this).removeClass('column_sort_unsorted').addClass('column_sort_asc').click(function(e){
											e.stopPropagation();
											e.stopImmediatePropagation();
											onexecute(dw.sort(table[jQuery(this).parent().index()-1].name).direction('desc').as_type([table[jQuery(this).parent().index()-1].wrangler_type]))
										})
									}
								}
							})
						}
						container.find('.column_sort_unsorted').click(function(e){
							e.stopPropagation();
							e.stopImmediatePropagation();
							onexecute(dw.sort(table[jQuery(this).parent().index()-1].name).as_type([table[jQuery(this).parent().index()-1].wrangler_type]))
						});
					}
	}

	var draw_controls = function(){

		if(vtable.page_length() < vtable.schema_table().rows()){
			var next = dw.jq('span').addClass('next').addClass('paginate_button').append('next').click(function(){


				if (((vtable.page() + 1) * vtable.page_length()) <= vtable.schema_table().rows()) {
					vtable.page(vtable.page()+1)
					vtable.save_settings();
					ontablechange();
				}
			})
			var prev = dw.jq('span').addClass('next').addClass('paginate_button').append('prev').click(function(){

				if(vtable.page()){
					vtable.page(vtable.page()-1)

					vtable.save_settings();
					ontablechange();

				}

			})

				var first = dw.jq('span').addClass('first').addClass('paginate_button').append('First').click(function(){

					if(vtable.page()!=0){
						vtable.page(0)

						vtable.save_settings();
						ontablechange();

					}

				})
				var last = dw.jq('last').addClass('last').addClass('paginate_button').append('Last').click(function(){

					if(vtable.page()!=0){
						vtable.page(0)

						vtable.save_settings();
						ontablechange();

					}

				})
		}

		var tablesize = dw.jq('span').addClass('table_size_indicator').append('rows: ' + table.rows())

		var controls = dw.jq('div').attr('id','table_controls').width(vtable.width()-28);

		container.prepend(controls)
		controls.append(tablesize).append(prev).append(next)





	}



	var draw_type_icons = function(){
		var wt, wr, className;
		vtable.header_row().slice(1).each(function(x, i){
			wr = table[x].wrangler_role
			wt = table[x].wrangler_type || dw.string()
			className = (wr && wr.name) ? wr.name : wt.name;
			jQuery(i).addClass(className).addClass('icon')
		})
	}

	vtable.header_row = function(){
		return container.find('.dataTables_scrollHead tr:nth-child(2) th')
	}

	var draw_summary = function(container, col){
		var w = container.width(), good, empty, bparse, brole, summary = dw.summary(col);
		w = vtable.column_width()


		normalized_summary = normalize_summary(col, summary, 5, w)


		var data = normalized_summary;
		var types = ['valid', 'brole', 'bparse', 'empty']

		var h = 15,
		    x = d3.scale.linear().domain([0, 1]).range([0, w]),
		    y = d3.scale.ordinal().domain(d3.range(data.length)).rangeBands([0, h], .2);


		var vis = d3.select(container[0])
		  .append("svg:svg")
		    .attr("width", w)
		    .attr("height", h)




		var bars = vis.selectAll("g.bar")
		    .data(data)
		  .enter().append("svg:g")
		    .attr("class", "bar")
		    .attr("transform", function(d, i) { return "translate(" + normalized_summary.slice(0, i).reduce(function(s,x){return s+x.val}, 0) + ",0)"; });


		var barTitle = function(summary, d){
			var l = summary[d.type].length;
			switch(d.type){
				case 'missing':
				case 'valid':
					return l + " " + d.type + " " + (l === 1 ? 'value' : 'values');
				case 'bparse':
					return l + " " + (l === 1 ? 'value' : 'values') + " don't parse";
				case 'brole':
					return l + " " + (l === 1 ? 'value' : 'values') + " don't match role";
			}
		}


		var barmouseup = function(d, i){

			interaction({type:d.type, col:table[col.index()].name})

		}


		bars.append("svg:rect")
			.attr('class',  function(d){return d.type})
		    .attr("width", function(d, i) {return d.val})
		    .attr("height", h)
			.on('mouseup', barmouseup)
			.append("svg:title")
			.text(function(d, i){return barTitle(summary, d)})







	}

	var normalize_summary = function(col, summary, min, width){



		var nsummary = [], total = col.table.rows(), nonzero, remainder, w = 0, bw, keys;

		nonzero = dv.keys(summary).filter(function(k){return k!='unique'}).filter(function(key){
			return summary[key].length != 0;
		}).length;

		remainder = width - nonzero*min;
		keys = 		['valid', 'brole', 'bparse', 'missing'];
		keys.forEach(function(key){
			bw = Math.round((summary[key].length===0) ? 0 : (min + (remainder * (summary[key].length-1) / (total-nonzero))));

			nsummary.push({type:key, val:bw})

			w+=bw;


		})





		return nsummary;
	}

	var draw_summary_bar = function(container, col, nsummary, summary, type){
		var w = container.width();
		if(nsummary[type]){
			var bar = dw.jq('span').width(nsummary[type]).addClass('summaryBar').addClass(type)
			container.append(bar);
		}

	}

	var getPosition = function(e){
		var aPos = otable.fnGetPosition(e)
		if(typeOf(aPos==='array')) return {row:aPos[0], col:aPos[1]-1};
		return {row:aPos};
	}

	var getSelection = function(){
		var selection = window.getSelection()
		if(!selection) return
		try{
			var range = selection.getRangeAt(0)
			var startCursor = range.startOffset
			var endCursor = range.endOffset
			var split = {startCursor:startCursor, endCursor:endCursor};
			return split;
		}
		catch(e){
			return undefined;
		}
	}


	vtable.id = function(){
		return container.attr('id') + '_table';
	}

	vtable.rows = function(cb, rows){
		var containerID = vtable.id(), start_row = vtable.page()*vtable.page_length(), end_row = start_row+vtable.page_length(), index;

		rows.filter(function(r){
			return (r >= start_row && r < end_row) || r===-1;
		}).forEach(function(r){

			if(r===-1) return cb(jQuery('#'+containerID+' thead tr:nth-child('+(2)+')'), r);

			index = (r+1)%vtable.page_length();
			if(index === 0) index = vtable.page_length();

			return cb(jQuery('#'+containerID+' tr:nth-child('+(index)+')'), r);
		})
	}

	vtable.cols = function(cb, cols){
		var containerID = vtable.id();
		cols.forEach(function(c){
			cb(jQuery('#'+containerID+' td:nth-child('+(c+2)+')'), c)
		})
	}





	vtable.container = function(){
		return container;
	}

	return vtable;



}
dw.wrangler_export = function(table, options){
	
	options = options || {};
	var format = options.format || 'csv';
	
	switch(format){
		case 'csv':
			return dw.wrangler_export.csv(table)
		case 'tsv':
			return dw.wrangler_export.tsv(table)
		case 'rowjson':
			return dw.wrangler_export.rowjson(table)
		case 'columnjson':
			return dw.wrangler_export.coljson(table)
		case 'lookup_table':
			return dw.wrangler_export.lookup_table(table)
		case 'python':
			return dw.wrangler_export.python(options.wrangler)
		case 'javascript':
			return dw.wrangler_export.javascript(options.wrangler)
	}
	

}

dw.wrangler_export.csv = function(table){
	var x = '';
	
	x+= table.names().map(dw.display_name).join(',')+'\n';
	
	for(var r = 0; r < table.rows(); ++r){
		x+= table.row(r).join(',') + '\n'
	}
	return x;
	
}

dw.wrangler_export.tsv = function(table){
	var x = '';
	x+= table.names().map(dw.display_name).join('\t')+'\n';
	for(var r = 0; r < table.rows(); ++r){
		x+= table.row(r).join('\t') + '\n'
	}
	return x;
	
}





dw.wrangler_export.coljson = function(table){
	var x = '[';
	
	
	

	
	x += table.map(function(c){
		var type;
		switch(c.wrangler_type.name){
			case dw.transform.STRING:
				type = 'nominal'
				break
			case dw.transform.NUMBER:
				type = 'numeric'
				break
			case dw.transform.INT:
				type = 'numeric'
				break
			case dw.transform.DATE:
				type = 'ordinal'
				break
			default:
				type = 'nominal'
		}
		return '{"type": "' + type +  '", "name": "' + dw.display_name(c.name)+ '", "values": [' + c.map(function(r){
			if(dw.is_missing(r)||isNaN(r)) r = r.quote()
			return r
		}).join(',') + "]}" + '\n';
	}).join(',')
	
	
	x+= ']'
	
	
	return x;
	
}


dw.wrangler_export.rowjson = function(table){
	var x = '[', val;
	for(var r = 0; r < table.rows(); ++r){
	
		if(r) x += ',\n'
	
		x+= '{' + table.map(function(c){
			val = c[r];
			if(val===undefined) val = '';
			if(dw.is_missing(val)||isNaN(val)) val = val.quote()
			return '"'+dw.display_name(c.name) + '"' + ": " + val;
			
		}).join(',') + '}'
			
	
	}
	
	x+= ']';
	return x;
	
}

dw.wrangler_export.lookup_table = function(table){
	var x = '{', val;
	for(var r = 0; r < table.rows(); ++r){
	
		if(r) x += ',\n'
	
		x+= table.map(function(c, i){
			val = c[r];
			if(val===undefined) val = '';
			if(i!=0){
				if(dw.is_missing(val)||isNaN(val)) val = val.quote()	
			}
			else val = val.quote()
			return val;
			
		}).join(':')
			
	
	}
	
	x+= '}';
	return x;
	
}

dw.wrangler_export.javascript = function(w){
	var parse_javascript_parameter = function(x){
		if(x===true) return 'true'
		if(x===false) return 'false'
		if(x===undefined) return 'undefined'
		
		if(typeOf(x)==='object' ||typeOf(x)==='function'){
			if(x.is_transform){
				return parse_javascript_transform(x)
			}
			return dw.JSON.stringify(x.toString().replace(/^\/|\/$/g,''))
		}

		if(typeOf(x)==='array'){
			return '['+x.map(parse_javascript_parameter)+']'
		}		
		return dw.JSON.stringify(x)
	}

	var parse_javascript_transform = function(t){
		var constructor = 'dw.' + t.name + '()';
		
		var params = t.params().map(function(p){
			return '.' + p.substr(1) + '(' + parse_javascript_parameter(t[p]) + ')\n'
		}).join('\t')
		
		return constructor + params
	}
	
	var preamble = ""
	
	var exportComment = '\n\n\/* apply transforms to data.  data can be a string, datatable or array of datatables *\/\n';
	var exportCode = "w.apply(data)\n\
\n\
\n"
	
	var wrangler = 'w = dw.wrangle()\n'




	return preamble + wrangler + w.filter(function(t){return t.active()}).map(function(t){
		var comment = '\n\/* '+t.comment()+' *\/\n';
		return comment+'w.add('+parse_javascript_transform(t)+')'

		
	}).join('\n') + exportComment+ exportCode
	
}

dw.wrangler_export.python = function(w){

	var parse_python_parameter = function(x){
		if(x===true) return 'True'
		if(x===false) return 'False'
		if(x===undefined) return 'None'
		if(typeOf(x)==='object' ||typeOf(x)==='function'){
			if(x.is_transform){
				return parse_python_transform(x)
			}
			return dw.JSON.stringify(x.toString().replace(/^\/|\/$/g,''))
		}

		if(typeOf(x)==='array'){
			return '['+x.map(parse_python_parameter)+']'
		}		
		

		return dw.JSON.stringify(x)
	}

	var parse_python_transform = function(t){
		

		
		var n = t.name.split('_').map(function(c){return c.charAt(0).toUpperCase() + c.slice(1)}).join('');
		var constructor = 'dw.' + n + '(';
		
		var params = t.params().map(function(p){
			return  p.substr(1) + '=' + parse_python_parameter(t[p])
			
		}).join(',\n'+dv.range(0,10+n.length).map(function(){return ' '}).join(''))
		
		return constructor + params + ')'
	}
	
	var preamble = "from wrangler import dw\n\
import sys\n\
\n\
if(len(sys.argv) < 3):\n\
	sys.exit('Error: Please include an input and output file.  Example python script.py input.csv output.csv')\n\
\n"




	var exportCode = "\n\nw.apply_to_file(sys.argv[1]).print_csv(sys.argv[2])\n\
\n"
	
	var wrangler = 'w = dw.DataWrangler()\n'




	return preamble + wrangler + w.filter(function(t){return t.active()}).map(function(t){
		var comment = '\n# '+t.comment()+'\n';
		return comment + 'w.add('+parse_python_transform(t)+')'

		
	}).join('\n') + exportCode
	
}

if (!String.prototype.quote) {
    String.prototype.quote = function () {
        var c, i, l = this.length, o = '"';
        for (i = 0; i < l; i += 1) {
            c = this.charAt(i);
            if (c >= ' ') {
                if (c === '\\' || c === '"') {
                    o += '\\';
                }
                o += c;
            } else {
                switch (c) {
                case '\b':
                    o += '\\b';
                    break;
                case '\f':
                    o += '\\f';
                    break;
                case '\n':
                    o += '\\n';
                    break;
                case '\r':
                    o += '\\r';
                    break;
                case '\t':
                    o += '\\t';
                    break;
                default:
                    c = c.charCodeAt();
                    o += '\\u00' + Math.floor(c / 16).toString(16) +
                        (c % 16).toString(16);
                }
            }
        }
        return o + '"';
    };
}dw.corpus = function(){
	var corpus = {};
	
	corpus.frequency = function(transform, o){
		o = o || {};
		var inputs = o.inputs || [], input = inputs[inputs.length-1];
		
		if(input){
			switch(input.type){
				case dw.engine.highlight:
					if(input && input.params.start!=undefined){
						if(input.params.end - input.params.start > 1){
							switch(transform.name){
								case dw.transform.SPLIT:

									return 9;

								case dw.transform.EXTRACT:

									return 24;

								case dw.transform.CUT:

									return 10;

								case dw.transform.FILTER:

										return 8;
							}		
						}
					}	
					break;
				case dw.engine.row:
					var t = dw.row(dw.empty());

					for(var i = 0; i < input.params.rows.length; ++i){
						if(t.test(input.params.table, input.params.rows[i])){
							if(transform.name===dw.transform.FILL){
								return 0;
							}
						}
					}
					
					
					if(transform.name===dw.transform.SET_NAME){
						var rows = input.params.rows;
						if(rows.length === 1 && rows[0] < 4){
							if(!dw.row(dw.empty().percent_valid(80)).test(input.params.table, rows[0])){
								return 40	
							}
						}
						
					}
					
			}
		}
		
		



		switch(transform.name){
			case dw.transform.SPLIT:

				return 35;
			
			case dw.transform.EXTRACT:

				return 32;
			
			case dw.transform.CUT:

				return 28;
			
			case dw.transform.DROP:

				return 13;
				
			case dw.transform.FOLD:
	
				return 12;

			case dw.transform.UNFOLD:
				if(input && input.params.table.length > 3){
					return 0;
				}

				return 8;


				case dw.transform.SET_NAME:					

					return 4;

						case dw.transform.SHIFT:					

							return 5;
			
			case dw.transform.FILL:					
				
				return 21;
				
			case dw.transform.MERGE:
			
				return 3;
				
				case dw.transform.COPY:

					return 6;



			case dw.transform.FILTER:

				return 25;
		}
		
	}
	
	corpus.top_transforms = function(o, k){
		o = o || {};
		var transform = o.transform, given_params = o.given_params, needed_params = o.needed_params, table = o.table;
		
		switch(transform.name){
			case dw.transform.FILL:
				if(given_params.indexOf('column')===-1&&needed_params.indexOf('direction')!=-1){
					var column = transform.column(), row = transform.row();
					if(column && column.length!=1 && row){
						return [transform.clone().direction(dw.RIGHT), transform.clone().direction(dw.DOWN), transform.clone().direction(dw.UP)]
					}
					else{
						return [transform.clone().direction(dw.DOWN),transform.clone().direction(dw.UP), transform.clone().direction(dw.RIGHT)]
					}
				}
				else{
					return [transform.clone().direction(dw.DOWN),transform.clone().direction(dw.UP), transform.clone().direction(dw.RIGHT)]
				}
				return [transform.clone()]
			case dw.transform.FOLD:
				if(given_params.indexOf('keys')===-1)
					return [transform.clone().keys([-1]),transform.clone().keys([0]),transform.clone().keys([0,1]),transform.clone().keys([0,1,2])]

				if(given_params.indexOf('column')===-1){
					return [transform.clone()].concat(get_columns(table, [dw.integer, dw.string]).map(function(c){return transform.clone().column(c)}))
				}

				return [transform.clone()]

			case dw.transform.UNFOLD:
				if(given_params.indexOf('measure')===-1){
					
					return get_columns(table, [dw.integer, dw.number, dw.string]).map(function(c){return transform.clone().measure(c)})
					
					
				}					
				return [transform.clone()]

			default:
				return [transform.clone()]
		}
	}
	
	get_columns = function(table, type_hierarchy, count){
		var cols = table.slice(0);
		cols.map(function(c){})
		cols.sort(function(a, b){
			var aindex = type_hierarchy.indexOf(a.wrangler_type.name)
			var bindex = type_hierarchy.indexOf(b.wrangler_type.name)
			
			if(aindex===-1) aindex = 1000000;
			if(bindex===-1) bindex = 1000000;
			
			if(aindex < bindex) return -1;
			if(bindex < aindex) return -1;
			
			return 0;
		})
		return cols.slice(0, count).map(function(c){return c.name})
	}
	
	return corpus;
}
enable_proactive = true;
enable_hyperactive = false;

dw.engine = function(options){
	var engine = {}, options = options || {}, transform_set = options.transform_set || dw.engine.transform_set, corpus = options.corpus || dw.corpus(), workingTransform;

	dv.ivar(engine, [{name:'table', initial:undefined}]);
	dv.ivara(engine, [{name:'inputs', initial:[]}]);

	engine.run = function(k){
		var params = inferSelection().concat(inferRow()).concat(inferCol()).concat(inferEdit());

		var inferredTransforms = inferTransforms(params);
		var promotes = filterInputs([dw.engine.promote, dw.engine.param]);





		if (enable_proactive &&
				inferredTransforms.length==1 &&
				((promotes.length==0 && inferredTransforms[0]===undefined) ||
				 (promotes.length > 0))) {
			var nRows = engine._table.rows();
			var nCols = engine._table.cols();

			var stateScore = dw.calc_state_score(engine._table);
			var numUniques = dw.num_unique_elts(engine._table);
			var potentialSuggestions = [];









			var colsToDelSet = {};

			var emptyColNames = [];

			for (var c = 0; c < nCols; c++) {
				var col = engine._table[c];









				var onlyEmptyRowsMissing = true;

				var numMissing = 0;
				for (var r = 0; r < nRows; r++) {
					var elt = col[r];
					if (dw.is_missing(elt)) {
						numMissing++;

						if (onlyEmptyRowsMissing) {
							var rowContents = engine._table.row(r);
							var rowIsEmpty = (rowContents.filter(dw.is_missing).length == rowContents.length);
							if (!rowIsEmpty) {
								onlyEmptyRowsMissing = false;
							}
						}
					}
				}


				if (numMissing == nRows) {

					emptyColNames.push(col.name);



				}
				else {

					if (numMissing >= (nRows / 2)) {

					}

					var hasCommas = false;
					var hasColons = false;
					var hasPipes = false;
					var hasTabs = false;

					col.forEach(function(elt) {
						if (elt) {

							var commas = elt.match(/,/g);
							var colons = elt.match(/\:/g);
							var pipes = elt.match(/\|/g);
							var tabs = elt.match(/\t/g);






							if (commas) hasCommas = true;
							if (colons) hasColons = true;
							if (pipes) hasPipes = true;
							if (tabs) hasTabs = true;
						}
					});







				}
			}















			for (var r = 0; r < nRows; r++) {
				var rowElts = engine._table.row(r);
				var numMissing = rowElts.filter(dw.is_missing).length;
				var pctMissing = numMissing / rowElts.length;


				if (pctMissing > 0.5 && pctMissing < 1) {





				}
			}





			var foldColNames = engine._table.slice_cols(1, engine._table.cols()).map(function(e) {return e.name});





			var numFoldedColumns = foldColNames.length;




			if (nRows > 1 && numFoldedColumns > 1) {



				var allHeaderNamesMeaningful = true;

				for (var i = 1; i < nCols; i++) {
					if (!engine._table[i].nameIsMeaningful) {
						allHeaderNamesMeaningful = false;
						break;
					}
				}

				if (allHeaderNamesMeaningful && dw.is_slice_all_valid_and_unique(engine._table[0], 0)) {
					potentialSuggestions.push(dw.fold(foldColNames).keys([-1]));
				}

				if (dw.is_slice_all_valid_and_unique(engine._table[0], 1)) {
					potentialSuggestions.push(dw.fold(foldColNames).keys([0]));
				}
			}
			if (nRows > 2 && numFoldedColumns > 2 &&
					dw.is_slice_all_valid_and_unique(engine._table[0], 2)) {
				potentialSuggestions.push(dw.fold(foldColNames).keys([0, 1]));
			}
			if (nRows > 3 && numFoldedColumns > 3 &&
					dw.is_slice_all_valid_and_unique(engine._table[0], 3)) {
				potentialSuggestions.push(dw.fold(foldColNames).keys([0, 1, 2]));
			}



			if (nCols >= 3 && nCols <= 5) {


				for (var c1 = 0; c1 < nCols; c1++) {
					for (var c2 = 0; c2 < nCols; c2++) {

						if (c1 == c2) {
							continue;
						}

						otherColsIndices = [];
						for (var i = 0; i < nCols; i++) {
							otherColsIndices.push(i);
						}
						otherColsIndices.splice(otherColsIndices.indexOf(c1), 1);
						otherColsIndices.splice(otherColsIndices.indexOf(c2), 1);

						var c1Col = engine._table[c1];
						var c2Col = engine._table[c2];
						var otherCols = otherColsIndices.map(function(c) {return engine._table[c]});

						var c1Summary = dw.summary(c1Col);
						var c2Summary = dw.summary(c2Col);
						var otherColsSummaries = otherCols.map(dw.summary);



						if (c1Summary.missing.length > 0) {
							continue;
						}
						if (otherColsSummaries.filter(function(e) {return (e.missing.length > 0)}).length > 0) {
							continue;
						}



						if (c1Summary.bparse.length > 0) {
							continue;
						}
						if (otherColsSummaries.filter(function(e) {return (e.bparse.length > 0)}).length > 0) {
							continue;
						}

						potentialSuggestions.push(dw.unfold(c1Col.name).measure(c2Col.name));
					}
				}
			}



			var scoredCandidates = [];

			for (var i = 0; i < potentialSuggestions.length; i++) {
				var tableCopy = engine._table.slice();
				var curSuggestion = potentialSuggestions[i];
				curSuggestion.apply([tableCopy]);

				var transformType = curSuggestion.description()[0];




				if (transformType == "Unfold") {
					var measureCol = engine._table[curSuggestion._measure];






					var endIdx = tableCopy.cols();

					var flattenedSubmatrix = [];
					for (var c = nCols - 2; c < endIdx; c++) {
						var col = tableCopy[c];


						for (var r = 0; r < tableCopy.rows(); r++) {
							flattenedSubmatrix.push(col[r]);
						}
					}

					var measureColStats = dw.get_column_stats(measureCol, nRows);


					var flattenedSubmatrixStats = dw.get_column_stats(flattenedSubmatrix, flattenedSubmatrix.length);


					var measureColScore = (1 - measureColStats.colHomogeneity) +
																(measureColStats.numMissing / nRows);
					var flattenedSubmatrixScore = (1 - flattenedSubmatrixStats.colHomogeneity) +
																				(flattenedSubmatrixStats.numMissing / flattenedSubmatrix.length);



					if (flattenedSubmatrixScore <= measureColScore) {
						scoredCandidates.push([flattenedSubmatrixScore - measureColScore, curSuggestion]);
					}
				}
				else {
					var newScore = dw.calc_state_score(tableCopy);
					var newNumUniques = dw.num_unique_elts(tableCopy);


					if (newScore < stateScore) {
						scoredCandidates.push([stateScore - newScore, curSuggestion]);
					}
				}
			}


			scoredCandidates.sort(function(a, b) {return b[0] - a[0];});

			params = scoredCandidates.map(function(e) {return e[1];});




			var promotes = filterInputs([dw.engine.promote, dw.engine.param]);
			var promote = promotes[promotes.length-1];
			var workingTransform = (promote && promote.transform);

			if (workingTransform) {

				params.unshift(workingTransform);



				params = params.filter(function(t, i){
					if(i===0 || t===undefined) return true;
					if(params[0] && t.equals(params[0])){
						return false;
					}
					return true;
				});
			}
			else {

				params.unshift(undefined);
			}

			return params;
		}


		return inferredTransforms.slice(0, k);
	}

	engine.input = function(input){
		engine._inputs.push(input)
		return engine;
	}

	engine.restart = function(){

	}

	engine.promoted_transform = function(){
		var promotes = filterInputs([dw.engine.promote, dw.engine.param, dw.engine.filter]), promote = promotes[promotes.length-1];
		return promote && promote.transform;
	}

	var inferTransforms = function(params){

		var promotes = filterInputs([dw.engine.promote, dw.engine.param]), promote = promotes[promotes.length-1], transforms = [], tset=transform_set.slice(0);

		workingTransform = (promote && promote.transform)

		if(workingTransform){
			tset = tset.filter(function(t){return t.name!=workingTransform.name})
			tset.unshift(workingTransform)
		}

		transforms = transforms.concat(tset.reduce(function(tforms, t){
			return tforms.concat(params.filter(function(p){return !p.is_transform}).reduce(function(acc, p){
					return acc.concat(inferTransformParameterSet(t, p));
			}, []))
		}, []))

		transforms = params.filter(function(p){return p.is_transform}).concat(transforms)



		transforms = transforms.concat(inferMissing()).concat(inferBadType()).concat(inferBadRole()).concat(inferValid())




		transforms = sortTransforms(transforms)

		
		transforms.unshift(workingTransform)





		transforms = varyTransforms(transforms)





		transforms = transforms.slice(0, 8).filter(function(t, i){
			if(i===0 || t===undefined) return true;

			if(transforms[0] && t.equals(transforms[0])){
				return false;
			}

			return true;
		})



		if(transforms.length === 1 && transforms[0]===undefined){
			var type = getFilterType();
			if(type)
				transforms = [dw[type]()];
		}

		return transforms;


	}

	var varyTransforms = function(transforms){
		var counts = {}, all_counts = {}, remaining_counts = {}, current, currentCount, filterType = getFilterType(), variedTransforms = [],
			exemptName = filterType || (workingTransform && workingTransform.name), maxCount = Math.max(Math.ceil(Math.min(6, transforms.length)*.33), 6);



		var total_count = 0;

		for(var i = 0; i < transforms.length; ++i){
			current = transforms[i];
			if(current===undefined){

			}
			else{
				currentCount = all_counts[current.name] || 0;
				if(current.name === exemptName){
					total_count++;
				}
				else{
					all_counts[current.name] = ++currentCount;
					if(currentCount <= maxCount){
						total_count++;
					}
				}
			}
		}

		
		var remaining_count = 6 - total_count;


		for(var i = 0; i < transforms.length; ++i){
			current = transforms[i];
			if(current === undefined){
				variedTransforms.push(current)
			}
			else{
				currentCount = counts[current.name] || 0;
				if(current.name === exemptName){
					variedTransforms.push(current);
				}
				else{
					counts[current.name] = ++currentCount;
					if(currentCount <= maxCount){
						variedTransforms.push(current)
					}
					else{
						if(remaining_count > 0){
							remaining_count--;
							variedTransforms.push(current)
						}
					}
				}
			}
		}
		return variedTransforms;
	}

	var getFilterType = function(){
		var filters = filterLatestInputs(dw.engine.filter), filter = filters[filters.length-1], filterType = (filter ? filter.transform : undefined);
		return filterType;
	}

	var sortTransforms = function(transforms){
		var inputs = getSelectionRecords();
		transforms.forEach(function(t){
			t.weights = {};
			t.weights.tf = corpus.frequency(t, {inputs:inputs});
			t.weights.td = transformDifficulty(t)
			t.weights.tdl = transformDescriptionLength(t)
			if(workingTransform) t.weights.wts = workingTransform.similarity(t)
		})

		var aw, bw, filterType = getFilterType();


		transforms.sort(function(a, b){
			aw = a.weights; bw = b.weights;
			if(workingTransform){
				if(a.name === workingTransform.name && b.name != workingTransform.name) return -1;
				if(b.name === workingTransform.name && a.name != workingTransform.name) return 1;
				if(a.name === workingTransform.name && b.name === workingTransform.name){
					var as = aw.wts, bs = bw.wts;
					if(as > bs) return -1;
					if(bs > as) return 1;
				}

			}

			if(filterType){
				if(a.name === filterType && b.name != filterType) return -1;
				else if(b.name === filterType && a.name != filterType) return 1;
			}



			if(aw.td > bw.td) return -1; if(bw.td > aw.td) return 1;
			if(aw.tf > bw.tf) return -1; if(bw.tf > aw.tf) return 1;
			if(aw.tdl < bw.tdl) return -1; if(bw.tdl < aw.tdl) return 1;
			return 0;
		})

		return transforms

	}


	var transformDescriptionLength = function(t){
		return t.description_length();
	}

	var transformDifficulty = function(t){
		switch(t.name){
			case dw.transform.SPLIT:
			case dw.transform.CUT:
			case dw.transform.EXTRACT:
			case dw.transform.FILTER:
				return 1;
			default:
				return 0;
		}
	}




	var inferTransformParameterSet = function(transform, param){



		if(param.is_transform) return [param]

		var t = transform.clone();

		var keys = dv.keys(param), p, neededParams;




		for(var i = 0; i < keys.length; ++i){
			p = keys[i];

			if(!t.has_parameter(p)) return [];
			try{
				t[p](param[p]);
			}
			catch(e){
				console.error(e)
			}

		}






		neededParams = t.enums().filter(function(x){return keys.indexOf(x)===-1})
		var top = corpus.top_transforms({transform:t, given_params:keys, needed_params:neededParams, table:engine._table})

		var promoted = engine.promoted_transform();
		if(promoted && promoted === t.name){
			return top.slice(0, 30);
		}
		else{
			return top.slice(0, 30).filter(function(x){return x.well_defined(engine._table)})
		}



	}

	var inferMissing = function(){
		var inputs = filterLatestInputs(dw.engine.missing_bar), col, candidates = [];


		if(inputs.length){
			col = inputs[inputs.length-1].col;

			candidates.push(dw.fill(engine._table[col].name))
			candidates.push(dw.fill(engine._table[col].name).direction(dw.UP))
			candidates.push(dw.filter(dw.is_null(engine._table[col].name)))
		}

		return candidates;
	}

	var inferValid = function(){
		var inputs = filterLatestInputs(dw.engine.valid_bar), col, candidates = [];

		if(inputs.length){
			col = inputs[inputs.length-1].col;
			candidates.push(dw.filter(dw.is_valid(engine._table[col].name)))
		}

		return candidates;
	}

	var inferBadRole = function(){
		var inputs = filterLatestInputs(dw.engine.bad_role_bar), col, candidates = [];

		if(inputs.length){
			col = inputs[inputs.length-1].col;
			candidates.push(dw.filter(dw.matches_role(engine._table[col].name)))
		}

		return candidates;
	}

	var inferBadType = function(){
		var inputs = filterLatestInputs(dw.engine.bad_type_bar), col, candidates = [];

		if(inputs.length){
			col = inputs[inputs.length-1].col;
			candidates.push(dw.filter(dw.matches_type(engine._table[col].name, engine._table[col].wrangler_type)))
		}

		return candidates;
	}

	var inferRow = function(){
		var inputs = filterLatestInputs(dw.engine.row), parameters = [], rows, candidates;


		if(inputs.length){
			rows = inputs[inputs.length-1].rows
		 	candidates = dw.row_inference().candidates(engine.table(), inputs[inputs.length-1].rows);
			parameters = candidates;

		}

		return parameters;
	}

	var inferEdit = function(){
		var inputs = filterLatestInputs(dw.engine.edit), parameters = [], rows, candidates;


		if(inputs.length){
			candidates = dw.edit_inference().candidates(engine.table(), inputs);
			parameters = candidates;
		}



		return parameters;
	}

	var inferCol = function(){
		var inputs = filterLatestInputs(dw.engine.col), parameters = [], names;
		if(inputs.length){

			names = inputs[inputs.length-1].cols.map(function(c){
				return engine._table[c].name
			});

			if(names.length	 > 0){
				parameters.push({column:names})
				if(names.length === 2){
					parameters.push({column:[names[0]], measure:names[1]})
					parameters.push({column:[names[1]], measure:names[0]})
				}
			}
		}

		return parameters;
	}

	var getSelectionRecords = function(inputs){
		inputs = inputs || filterLatestInputs(dw.engine.highlight);

		if(inputs && inputs.length){
			var selection, row, col, text, start, end, position, records, table = engine.table();
			return inputs.map(function(input, i){
				row = input.position.row, col = input.position.col, text = engine._table[col][row], start = input.selection.startCursor, end = input.selection.endCursor;
				return {type:dw.engine.highlight, params:dw.regex.record(text, start, end, table[col].name, row, table)};
			})
		}

		inputs =  filterLatestInputs(dw.engine.row);
		if(inputs && inputs.length){
			return inputs.map(function(i){
				return {type:dw.engine.row, params:{rows:i.rows, table: engine.table()}}
			})
		}

		inputs =  filterLatestInputs(dw.engine.col);
		if(inputs && inputs.length){
			return inputs.map(function(i){
				return {type:dw.engine.col, params:{cols:i.cols, table: engine.table()}}
			})
		}
	}



	var inferSelection = function(){

		var inputs = filterLatestInputs(dw.engine.highlight),
			selection, row, col, text, start, end, position, records, table = engine.table();

		if(!inputs.length) return []




		records = inputs.map(function(input, i){
			row = input.position.row, col = input.position.col, text = engine._table[col][row], start = input.selection.startCursor, end = input.selection.endCursor;
			return dw.regex.record(text, start, end, table[col].name, row, table);
		})



		var candidates = dw.regex().candidates(records);


		if(inputs.length === 1 || candidates.length < 2){
			candidates.unshift({positions:[inputs[inputs.length-1].selection.startCursor, inputs[inputs.length-1].selection.endCursor]})
		}

		var startRecord = records.length-1;

		while(candidates.length<3 && startRecord > 0){
			candidates = dw.regex().candidates(records.slice(records.length-startRecord))
			startRecord-=1;
		}




		var column = inputs[inputs.length-1].position.col
		column = engine._table[column].name;
		candidates.forEach(function(c){
			c.column = column
		})

		candidates = candidates.concat(dw.row_inference().candidates(engine.table(), records, {type:dw.engine.highlight}))



		return candidates;
	}

	var filterLatestInputs = function(type, o){
		var o = o || {}, inputs = engine._inputs, clear_index = inputs.length-1, clearTypes = o.clear_types || [type, dw.engine.filter, dw.engine.promote, dw.engine.param];
		while(clear_index >= 0){
			if(clearTypes.indexOf(inputs[clear_index].type)===-1){
				break;
			}
			clear_index--;
		}

		return engine._inputs.slice(clear_index+1).filter(function(i){
			return i.type === type;
		})
	}




	var filterInputs = function(type, o){

		var o = o || {}, inputs = engine._inputs, clear_index = inputs.length-1, clearTypes = o.clear_types || [dw.engine.execute, dw.engine.clear];

		if(typeOf(type)!='array') type = [type];

		while(clear_index >= 0){
			if(clearTypes.indexOf(inputs[clear_index].type)!=-1){
				break;
			}
			clear_index--;
		}

		return engine._inputs.slice(clear_index+1).filter(function(i){
			return type.indexOf(i.type)!=-1;
		})
	}


	return engine;
}

dw.engine.transform_set = [
	dw.split(),
	dw.edit(),
	dw.extract(),
	dw.cut(),
	dw.fill(),
	dw.fold(),
	dw.merge(),
	dw.filter(),
	dw.drop(),
	dw.unfold(),
	dw.set_name(),
	dw.translate(),
	dw.wrap(),
	dw.copy()
]

dw.engine.highlight = 'text_select';
dw.engine.edit = 'text_edit';
dw.engine.row = 'row_select';
dw.engine.col = 'col_select';
dw.engine.filter = 'type_select';
dw.engine.transform = 'transform_select';
dw.engine.execute = 'execute_transform';
dw.engine.promote = 'promote_transform';
dw.engine.clear = 'clear_transform';
dw.engine.missing_bar = 'missing';
dw.engine.bad_type_bar = 'bparse';
dw.engine.bad_role_bar = 'brole';
dw.engine.valid_bar = 'bvalid';

dw.engine.param = 'param_edit';




dw.calc_state_score = function(table) {
	var sumHomo = 0;
	var totalDelims = 0;
	var totalMissing = 0;
	var totalElts = 0;
	var nCols = table.cols();

	for (var c = 0; c < nCols; c++) {
		var col = table[c];
		var colStats = dw.get_column_stats(col, table.rows());

		sumHomo += colStats.colHomogeneity;




		totalElts += table.rows();

		totalMissing += colStats.numMissing;
		totalDelims += colStats.numDelims;
	}



	var avgHomo = sumHomo / nCols;
	var pctMissing = totalMissing / totalElts;


	var avgDelims = 0;
	if (totalMissing < totalElts) {
		avgDelims = totalDelims / (totalElts - totalMissing);
	}





	var stateScore = (1-avgHomo) + pctMissing + avgDelims;



	return stateScore;
}





dw.get_column_stats = function(col, nRows) {
	var numMissing = 0;
	var numDates = 0;
	var numNumbers = 0;
	var numStrings = 0;

	var numCommas = 0;
	var numColons = 0;
	var numPipes = 0;
	var numTabs = 0;


	for (var r = 0; r < nRows; r++) {
		var elt = col[r];







		if (dw.is_missing(elt)) {
			numMissing++;
		}
		else if (dw.date_parse(elt)) {
			numDates++;
		}
		else if (!isNaN(Number(elt))) {
			numNumbers++;
		}










		if (elt) {
			var commas = elt.match(/,/g);
			var colons = elt.match(/\:/g);
			var pipes = elt.match(/\|/g);
			var tabs = elt.match(/\t/g);






			if (commas) numCommas += commas.length;
			if (colons) numColons += colons.length;
			if (pipes) numPipes += pipes.length;
			if (tabs) numTabs += tabs.length;
		}
	}
	numStrings = nRows - numMissing - numNumbers - numDates;

	var numRealElts = nRows - numMissing;


	var colHomogeneity = 0;


	var pctDates = numDates / nRows;
	var pctNumbers = numNumbers / nRows;
	var pctStrings = numStrings / nRows;


	

	colHomogeneity = pctDates*pctDates + pctNumbers*pctNumbers + pctStrings*pctStrings;

	return {colHomogeneity: colHomogeneity,
					numMissing:     numMissing,
					numDelims:      numCommas+numColons+numPipes+numTabs};
}



dw.num_unique_elts = function(table) {
	var numUniques = 0;
	var uniques = {};
	for (var c = 0; c < table.cols(); c++) {
		var col = table[c];
		for (var r = 0; r < table.rows(); r++) {
			var elt = col[r];

			if (!dw.is_missing(elt) && uniques[elt] != 1) {
				numUniques++;
				uniques[elt] = 1;
			}
		}
	}

	return numUniques;
}



dw.is_slice_all_valid_and_unique = function(lst, i) {
	lst = lst.slice(i);
	var uniques = {};
	lst.forEach(function(e) {

		if (dw.is_missing(e)) {
			return false;
		}
		uniques[e] = 1;
	});
	var numUniques = 0;
	for (e in uniques) numUniques++;
	return (numUniques == lst.length);
}

dw.raw_inference = function(raw_text){
	
	var subsample = raw_text.substr(0, 10000)
	var total = subsample.length;
	var newlines = 0,
		commas = 0,
		quotes = 0,
		singleQuotes = 0,
		tabs = 0,
		spaces = 0,
		pipes = 0;
	
	for(var x = 0; x < total; ++x){
		var ch = subsample[x]
		switch(ch){
			case '\n':
				newlines++
			break
			case ',':
				commas++
			break
			case '"':
				quotes++
			break
			case '|':
				pipes++
			break
			case "'":
				singleQuotes++
			break
			case '\t':
				tabs++
			break
			case ' ':
				spaces++
			break
			
		}
	}
	
	var inference = {}
	var delimiterStrength = Math.max(commas, tabs, pipes);
	var cutoff = .8*newlines
	if(delimiterStrength > cutoff){
		if(commas >= cutoff){
			inference.type = 'csv'
			inference.delimiter = ','
		}
		else if(tabs >= cutoff){
			inference.type = 'tsv'
			inference.delimiter = '\t'
		}
		else if(pipes >= cutoff){
			inference.type = 'pipes'
			inference.delimiter = '\\|'
		}
		
		
		
		
		
		
		
		
		
		if(quotes >= 2){
			inference.ignore = 'double quotes'
			inference.quote_character = '"';
		}
		else if(singleQuotes >= 2){
			inference.ignore = 'single quotes'
			inference.quote_character = "'";
		}
		
	}
	
	
	
	var transforms = [dw.split('data').on('\n').max(0).result(dw.ROW)]
	
	if(inference.type){
		var split = dw.split('data').max(0).on(inference.delimiter);
				if(inference.quote_character){
					split.quote_character(inference.quote_character)
				}
				transforms.push(split)
			
				if(inference.quote_character){
					transforms.push(dw.cut().on(inference.quote_character).max(0))
				}
		
	}
	

	
	return {inference:inference, transforms:transforms}
	
	
}
var dw = dw || {};
dw.row_inference = function(){
	var r = {};
	
	r.candidates = function(table, records, o){
		
		o = o || {}
		var type = o.type || dw.engine.row, candidates = [];

		if(records.length){		
			switch(type){
				case dw.engine.row:

					var index = dw.row(dw.rowIndex(records));
					candidates.push({row:index})
					candidates.push({keys:records})
					if(records.length===1){
						candidates.push({header_row:records[0]})						
					}
					candidates = candidates.concat(enumerateEmpty(table, records))
					candidates = candidates.concat(enumerateRowEquals(table, records))
					candidates = candidates.concat(enumerateRowCycle(table, records))
					candidates = candidates.concat(enumeratePromote(table, records))
					return candidates;
			
				case dw.engine.highlight:

					records = records.filter(function(r){return r.text.length > 0});
					
					candidates = candidates.concat(enumerateEquals(table, records))
					candidates = candidates.concat(enumerateStartsWith(table, records))
					candidates = candidates.concat(enumerateContains(table, records))

					return candidates;
			}
		}
		
		
		return []
		
	}
	
	
	var enumeratePromote = function(table, records, o){
		var candidates = [];
		if(records.length === 1){
			var r = records[0];
			if(r < 5){
				
			}
		}
		return candidates.map(function(c){return {row:c}});
	}
	
	
	var enumerateRowEquals = function(table, records, o){
		var candidates = [];
		if(records.length){
			table.forEach(function(col){
				var val = col[records[records.length-1]];
				if(val)
					candidates = candidates.concat([dw.row(dw.eq(col.name, val, true))])
				else{
					candidates = candidates.concat([dw.row(dw.is_null(col.name))])
				}
			})
		}

		candidates = candidates.filter(function(c){
			for(var i = 0; i < records.length; ++i){
				if(c.test(table, records[i])===0){
					return false;
				}
			}
			return true;
		})

		return candidates.map(function(c){return {row:c}});
	}
	
	var enumerateRowCycle = function(table, records, o){
		
		if(records.length >= 2){
			var sortedRecords = records.slice().sort(function(a,b){return a-b > 0});
			var difference = sortedRecords[1]-sortedRecords[0];
			
			if(difference===1) return [];
			
			for(var i = 1; i < sortedRecords.length - 1; ++i){
				if(sortedRecords[i+1]-sortedRecords[i]!=difference){
					return []
				}
			}

			var all = dw.row(dw.rowCycle(difference, sortedRecords[0]%difference))
			
			
			
			var t = [all].reverse()
			
			return t.map(function(x){return {row:x}})
			
		}
		
		return [];
		
		
		
		
	}
	
	
	var enumerateEquals = function(table, records, o){
		if(records.length > 0){
			var record = records[records.length-1];
			if(record.start === 0 && record.end === record.text.length){
				var t = dw.row(dw.eq(record.col, record.text.substring(record.start, record.end), true));
				return [{row:t}]
				
			}
		}
		return []
	}
	
	
	
	var enumerateStartsWith = function(table, records, o){

		if(records.length > 0){
			var record = records[records.length-1];

			if(record.start === 0){
				var t = dw.row(dw.starts_with(record.col, record.text.substring(record.start, record.end), true));
				return [{row:t}]
			}
		}
		return []
		
	}
	
	var enumerateContains = function(table, records, o){

		if(records.length > 0){
			var record = records[records.length-1];
			var t = dw.row(dw.contains(record.col, record.text.substring(record.start, record.end), true));
			return [{row:t}]
			
		}
		return []
		
	}
	
	
	var enumerateIsNull = function(table, records, o){
		
	}
	


	var enumerateEmpty = function(table, records, o){
		var t = dw.row(dw.empty());
		for(var i = 0; i < records.length; ++i){
			if(!t.test(table, records[i])){
				return []
			}
		}
		return [{row:dw.row(dw.empty())}]
	}
	
	
	return r;
}
dw.edit_inference = function(){
	var r = {};
	
	r.candidates = function(table, records, o){
		
		o = o || {}
		var type = o.type || dw.engine.edit, candidates = [];

		if(records.length){		
				
			var record = records[records.length-1];
			var oldVal = table[record.position.col][record.position.row];
			var val = record.val;
			
			candidates.push({column:table[record.position.col].name, to:record.val, row:dw.row(dw.rowIndex([record.position.row]))});
			var colname = table[record.position.col].name;
			
			if(oldVal != undefined) oldVal = ''+oldVal;
			
			if(oldVal){
				candidates.push({column:colname, to:record.val, row:dw.row(dw.eq(colname, oldVal, true))});				
			}
			else{
				candidates.push({column:colname, to:record.val, row:dw.row(dw.is_null(colname))});			
			}
			

			
			if(oldVal && val == oldVal.toUpperCase()){
				candidates.push({column:colname, update_method:dw.edit.upper});			
			}
			if(oldVal && val == oldVal.toLowerCase()){
				candidates.push({column:colname, update_method:dw.edit.lower});			
			}
			if(oldVal && val.substr(1) == oldVal.substr(1)){
				if(val[0]!=oldVal[0]){
					if(val[0]===oldVal[0].toUpperCase()){
						candidates.push({column:colname, update_method:dw.edit.capitalize});					
					}					
					else if(val[0]===oldVal[0].toLowerCase()){
						candidates.push({column:colname, update_method:dw.edit.uncapitalize});
					}					
				}
			}
			
			if(oldVal===undefined || oldVal.replace(/[ \t\n]/g, '').length===0){
				var max_dist = 15;
				var col = record.position.col;
				var column = table[col], row = record.position.row;
				for(var i = row+1; i < Math.min(row+max_dist, table.rows()); ++i){
					var v = column[i];
					if(!dw.is_missing(v)){
						if(v===val){
							candidates.push(dw.fill(colname).direction('up'))
							break;
						}
						else{
							break
						}
						
					}
				}
				for(var i = row-1; i >= Math.max(row-max_dist, 0); --i){
					var v = column[i];
					if(!dw.is_missing(v)){
						if(v===val){
							candidates.push(dw.fill(colname).direction('down'))
							break;
						}
						else{
							break
						}
						
					}
				}

				for(var i = col-1; i >= Math.max(col-max_dist, 0); --i){
					var v = table[i][row];
					if(!dw.is_missing(v)){
						if(v===val){
							candidates.push(dw.fill().direction('right').row(dw.row(dw.rowIndex([row]))))
							break;
						}
						else{
							break
						}

					}
				}
				for(var i = col+1; i < Math.min(col+max_dist, table.cols()); ++i){
					var v = table[i][row];
					if(!dw.is_missing(v)){
						if(v===val){
							candidates.push(dw.fill(colname).direction('left').row(dw.row(dw.rowIndex([row]))))
							break;
						}
						else{
							break
						}

					}
				}
			}
			



			return candidates;
			

		}
		
		
		return []
		
	}
	
	
	
	return r;
}
dw.infer_type_transforms = function(table){
	var cols = table, types;

	var infer_col_type = function(col, sampleSize){
		var validNumbers = 0, validInts = 0, validDates = 0, blanks = 0, uniqueValues = [], v, sampleSize = sampleSize || col.length;

		for(var i = 0; i < sampleSize; ++i){
			v = col[i]


			if(dw.is_missing(v)) {
				blanks++
			}
			else{
				v = ""+v


				if (dw.date_parse(v)) {
					validDates++;
				}
				else if(!isNaN(Number(v))){
					validNumbers++;

					if(Number(v)===parseInt(Number(v))){
						validInts++;
					}
				}
			}
		}

		var nonEmpty = sampleSize - blanks;

		if((validDates && validDates * 2 > nonEmpty)){
			return {type:dw.date(), role:dw.role()}
		}



		if(validInts && validInts * 2 > nonEmpty){
			return {type:dw.integer(), role:dw.role()}
		}
		if(validNumbers && validNumbers * 2 > nonEmpty){
			return {type:dw.number(), role:dw.role()}
		}

		var role = dw.role()
		var candidate_roles = [dw.state(), dw.country()];

		for (var i = 0; i < candidate_roles.length; ++i) {
		  var	validRole = 0, candidate_role = candidate_roles[i];
  		col.forEach(function(v){
  		  if(candidate_role.parse(v) !== false){
  			  validRole++;
  		  }
  		})
  		if(validRole * 2 > nonEmpty){
					role = candidate_role;
			}
		}





		return {type:dw.string(), role:role}
	}

	types_and_roles = cols.map(function(col){
		return infer_col_type(col);
	})

	types = types_and_roles.map(function(t){
		return t.type
	})

	roles = types_and_roles.map(function(t){
		return t.role
	})






	return [dw.set_type(cols.map(function(c){return c.name}), types),dw.set_role(cols.map(function(c){return c.name}), roles)]

}
dw.clause = function(options){

	options = options || {};
	var editor_class = options.editor_class;

	
	var clause = {}, element = jQuery(document.createElement('span'));
	dv.ivar(clause, [{name:'onedit', initial:undefined}])	
	
	clause.draw = function(container, draw_options) {
		draw_options = draw_options || {}, editable = draw_options.editable || false;
		
 		var description = dw.jq('span').append(' ' + clause.description() + ' ')
		
		element.append(description);
		
		if(!clause.plain){
			element.addClass('editableClause').addClass(editor_class)

			if(editable){
				description.click(function(){
					element.empty();
					var editor = clause.editor();
					element.append(editor)
				})
				
				
			}
		}
		
		clause.done_editing = function(params){
			draw_options.onedit(params)
		}
		
		container.append(element);
	}
	
	
		
	return clause;
		
}

dw.clause.get = function(transform, param){
	
	switch(param){
		
		case 'row':
			return dw.row_clause(transform, transform[param](), param)
		case 'keys':
			return dw.key_clause(transform, transform[param](), param)
		case 'column':
			return dw.column_clause(transform, transform[param](), param)		
		case 'max':
			return dw.select_clause(transform, {select_options:{'0':'repeatedly','1':'once'}, param:'max'})
		case 'drop':
			return dw.select_clause(transform, {select_options:{'true':'true','false':'false'}, param:'drop'})
		case 'result':
			return dw.select_clause(transform, {select_options:{'row':'row','column':'column'}, param:'result'})
		case 'direction':
			return 	dw.select_clause(transform, {select_options:{'right':'the left', 'left':'the right', 'up':'below', 'down':'above'}, param:'direction'})

		case 'on':
		case 'before':
		case 'after':
		case 'ignore_between':		
			return dw.regex_clause(transform, param);
		case 'quote_character':		
			return dw.select_other(transform, param, {});
		default:
			return dw.input_clause(transform, param)
	}
	
}





dw.combo_with_other = function(select_options, options){

	var selected_val = options.val, editor;
	var input =	dw.jq('input');
	input.val(options.val || '').focus();
	input.blur(function(){
		options.ondone(input.val())
	})
	editor = dw.jq('select');
	if(!select_options.filter(function(c){return c.value===selected_val}).length){
		dw.add_select_option(editor, selected_val, selected_val)
	}
	select_options.forEach(function(c){
		dw.add_select_option(editor, c.name, c.value);
	})

	editor.change(function(){
		var val = editor.attr('value');

		if(val===dw.combo_with_other.other){
			editor.replaceWith(input)
			input.focus()
		}
		else{
			options.ondone(editor.attr('value'))
		}
	}).blur(function(){
		if(editor.attr('value')!=dw.combo_with_other.other){
			options.ondone(editor.attr('value'))
		}
	})
	dw.add_select_option(editor, dw.combo_with_other.other)
	editor.val(options.val)
	return editor;
	
}

dw.combo_with_other.other = 'other...'
dw.column_clause = function(t, columns, param, options){
	
	options = options || {};
	options.editor_class = options.editor_class || 'updatedColumn';
	var extra_text = options.extra_text || '';
	
	var clause = dw.clause(options), editor;
	
	clause.description = function(){
		var x = columns.slice(0,Math.min(4, columns.length)).map(dw.display_name).join(', ')

		if(columns.length > 4){
			x += '...';
		}
		
		return x + extra_text;
	}
	
	clause.editor = function(draw_options){
		draw_options = draw_options || {};
		if(draw_options.table){
			editor = dw.jq('select')
			if(!options.single){
				
			}
			else{
				editor.change(function(){
					onEdit();
				})
			}
			if(options.all_columns){
				dw.add_select_option(editor, 'all columns');
			}
			draw_options.table.forEach(function(c){
				dw.add_select_option(editor, dw.display_name(c.name), c.name);
			})
			var v = (columns.length) ? columns : ['all columns'];
			
			var onEdit = function(){
				var v = editor.val() || [];
				if(v.indexOf('all columns')!=-1) v = []
				t = t[param](v)
				clause.done_editing({type:dw.engine.param, value:v, param:options.param, transform:t});
			}
			
			editor.val(v);

			editor.blur(function(){
				onEdit();
			})
			
			

		}
		else{
			editor = jQuery(document.createElement('input'));
			editor.val(clause.description())

			editor.blur(function(){
				t = t[param](clause.value())
				clause.done_editing({type:dw.engine.param, value:editor.val(), param:param, transform:t});
			})
		}
		
		return editor;
		
	}
	
	clause.value = function(){
		var vals = editor.val().replace(/ /g, '').split(/,/g);
		if(options.clean_val){
			return vals.map(options.clean_val)
		}
		return vals;
	}
	
	return clause;
	
}
dw.input_clause = function(t, param){
	
	var clause = dw.clause(), editor, input = t[param]();
	

	
	clause.description = function(){
		return input;
	}
	
	clause.editor = function(){
		editor = jQuery(document.createElement('input'));
		editor.val(clause.description())
		
		editor.blur(function(){

			t = t[param](clause.value())
			clause.done_editing({type:dw.engine.param, value:editor.val(), param:param, transform:t});
		})
		
		
		return editor;
	}
	
	clause.value = function(){
		return editor.val();
	}
	
	return clause;
	
}
dw.key_clause = function(t, keys, param, options){


	
	options = options || {};
	options.editor_class = options.editor_class || 'updatedColumn';
	var extra_text = options.extra_text || '';
	
	var clause = dw.clause(options), editor;
	
	clause.description = function(){
		return keys.map(function(k){return k==='header' ? k : Number(k)+1}).join(', ')+extra_text;
	}
	
	clause.editor = function(draw_options){
		draw_options = draw_options || {};
		editor = jQuery(document.createElement('input'));
		editor.val(clause.description())

		editor.blur(function(){
			t = t[param](clause.value())
			clause.done_editing({type:dw.engine.param, value:clause.value(), param:param, transform:t});
		})
				
		return editor;
		
	}
	
	clause.value = function(){
		var vals = editor.val().replace(/ /g, '').replace('header', '-1').split(/,/g).map(function(c){return c==='header' ? c : Number(c)-1});
		if(options.clean_val){
			return vals.map(options.clean_val)
		}
		return vals;
	}
	
	return clause;
	
}
dw.regex_clause = function(t, param, options){
	

	
	options = options || {};
	options.editor_class = options.editor_class || 'updatedColumn';
	


	var clause = dw.clause(options), editor, regex = options.value || t[param]();
	
	clause.description = function(){
		return dw.regex.friendly_description(regex).replace(/^\/|\/$/g,'');
	}
	
	clause.editor = function(){
		editor = jQuery(document.createElement('input'));
		
		var val = regex ? regex.toString().replace(/^\/|\/$/g,'').replace(/\n/g, '\\n').replace(/\t/g, '\\t') : '';

		editor.val(val)
		
		editor.blur(function(){
			
			var newval = clause.val();
			
			
			t = t[param](clause.val())
			clause.done_editing({type:dw.engine.param, value:clause.val(), param:param, transform:t});


		})
		
		return editor;
	}
	
	clause.editor = function(){
		var val = regex ? regex.toString().replace(/^\/|\/$/g,'').replace(/\n/g, '\\n').replace(/\t/g, '\\t') : '';
		var regex_options = [{name:'comma', value:','},{name:'tab', value:'\\t'},{name:'whitespace', value:' '},{name:'newline', value:'\\n'},{name:'pipe', value:'\\|'},{name:'period', value:'\\.'}]
		editor = dw.combo_with_other(regex_options, {val:val, ondone:function(val){
			var newval = clause.val(val);	
			t = t[param](newval)
			clause.done_editing({type:dw.engine.param, value:newval, param:param, transform:t});
			
		}});
		return editor;
	}
	
	clause.val = function(val){
		
		var val = val || editor.val();
		
		if(val.length===0) return undefined;
		
		return new RegExp(val.replace(/\\n/g, '\n').replace(/\\t/g, '\t'))
	}
	
	return clause;
	
}
dw.row_clause = function(t, row, param, options){
	
	
	
	options = options || {};
	options.editor_class = options.editor_class || 'droppedRow';
	param = param || 'row'

	var clause = dw.clause(options), editor;
	
	clause.description = function(){
		if(row != undefined){
			var d = row.description();
			if(typeOf(d)==='array') d = d[0];
			
			if(d.length > 50){
				d = d.substr(0, 50);
				d+='...'
			}
			
			return d;
		}
		else return ''
	}
	
	clause.editor = function(){
		editor = jQuery(document.createElement('input'));
		editor.val(row ? row.formula() : '')
		
		editor.blur(function(){
			t = t[param](clause.val())
			clause.done_editing({type:dw.engine.param, value:editor.val(), param:param, transform:t});
		}).keydown(function(e){
			editor.unbind('blur')
			if(e.which===13){
				t = t[param](clause.val())
				clause.done_editing({type:dw.engine.param, value:editor.val(), param:param, transform:t});
			}
		})
		return editor;
	}
	
	clause.val = function(){
		var val = editor.val();
		if(val)	return (dw.row.fromFormula(val))
		else return undefined
	}
	
	return clause;
	
}
dw.select_clause = function(t, options){
	
	var clause = dw.clause(), editor, options = options || {};
	var param = options.param;

	var value = options.value || t[param]();
	
	
	
	
	clause.description = function(container){
		
		editor = jQuery(document.createElement('select'))
				
		return options.select_options[value];
	}
	
	
	clause.editor = function(){
		editor = dw.jq('select')
		
		dv.keys(options.select_options).forEach(function(o){

			
			var option = document.createElement("option");
			option.value = o;
			option.text = options.select_options[o];
			editor[0].options.add(option)
			
			
			
			
		})
		
		editor.val(value)
		
		
		editor.change(function(){
				t = t[param](editor.attr('value'))
				clause.done_editing({type:dw.engine.param, value:editor.attr('value'), param:options.param, transform:t});
			})
				
			editor.blur(function(){
				t = t[param](editor.attr('value'))
				clause.done_editing({type:dw.engine.param, value:editor.attr('value'), param:options.param, transform:t});
			})

		



		return editor;
	}
	
	
	clause.on_edit = function(){
		
		
	}
	
	
	clause.value = function(){
		return editor.attr('value');
	}
	
	
	return clause;
	
}
dw.select_other = function(t, param, options){
	

	
	options = options || {};
	options.editor_class = options.editor_class || 'updatedColumn';
	


	var clause = dw.clause(options), editor, val = options.value || t[param]() || '';
	
	clause.description = function(){
		return val;
	}
	
	clause.editor = function(){
		editor = jQuery(document.createElement('input'));
		
		editor.val(val)
		
		editor.blur(function(){
			
			var newval = clause.val();
				
			t = t[param](clause.val())
			clause.done_editing({type:dw.engine.param, value:clause.val(), param:param, transform:t});


		})
		
		return editor;
	}
	
	clause.editor = function(){
		
		var val_options = [{name:'Quotes', value:'"'},{name:'Single Quotes', value:"'"}]
		editor = dw.combo_with_other(val_options, {val:val, ondone:function(val){
			var newval = clause.val(val);	
			t = t[param](newval)
			clause.done_editing({type:dw.engine.param, value:newval, param:param, transform:t});
			
		}});
		return editor;
	}
	
	clause.val = function(val){
		
		var val = val || editor.val();
		
		if(val.length===0) return undefined;
		
		return val;
	}
	
	return clause;
	
}
dw.text_clause = function(text){
	var clause = dw.clause();
	
	clause.plain = true;
	
	clause.description = function(){
		return text;
	}
	
	return clause;
}
dw.descriptionEditor = function(container, transform, options){
	var editor = {};
	
	
	
	editor.draw = function(){
		


		var clauses = transform.description();
		
		clauses.forEach(function(clause){
			if(typeOf(clause)==='string')
				clause = dw.text_clause(clause)

			clause.draw(container, options);
		})
		
		
	}
	
	return editor;
}
dw.detail_editor = function(container, transform, options){
	
	var editor = {};

	ignore = ['status', 'table', 'which', 'drop', 'insert_position', 'ignore_between', 'update', 'method']
	if(transform.name === 'filter') ignore.push('column')
	if(transform.name != 'split') ignore.push('result')
	editor.draw = function(){
		var clause, detail;
		transform.params().map(function(p){return p.substr(1)})
			.filter(function(param){
				return ignore.indexOf(param)===-1
			}).sort()
			.forEach(function(param){


				detail = dw.jq('div').addClass('detail')
				var label = dw.jq('div').append(param).addClass('label');
				detail.append(label)
			
				clause = dw.clause.get(transform, param)
				clause.done_editing = function(params){
					options.onedit(params);
				};
				detail.append(clause.editor(options).addClass('detail_editor_input'))
			
				container.append(detail)
				detail.append(dw.jq('div').addClass('clear'))
			})
		
		return container;
		
	}
	
	
	return editor;
}
dw.editor = function(container, suggestions, options){

	var editor = {}, onpromote = options.onpromote, onhighlight = options.onhighlight, onselect = options.onselect, onedit = options.onedit, onclear = options.onclear, header, title;
	
	editor.draw = function(){
		var descriptionWrapper, addButton,  editorWraper;
		editorWrapper = jQuery(document.createElement('div')).attr('id', 'editorTransformContainer')

	
			header = dw.jq('div').addClass('menu').attr('id', 'suggestionsBanner')
			title = dw.jq('span').append('Suggestions').attr('id', 'suggestionsTitle')

			header.append(title)
		container.append(header)
	

		container.append(editorWrapper)
	
		
		
			

	
		
		
		
		
		

		suggestions.forEach(function(s, i){
			
			
			
			
			var sWrapper = jQuery(document.createElement('div')).addClass('suggestion')			
			if(!i) sWrapper.addClass('firstSuggestion')
			else sWrapper.css('opacity', 0);


			editorWrapper.append(sWrapper)

			if(s){
				descriptionWrapper = jQuery(document.createElement('div')).addClass('descriptionEditor')			
				addButton = jQuery(document.createElement('div')).addClass('editorButton addButton')

				addButton.click(function(){
					onselect(editor.transform())
				})
				
				
				sWrapper.click(function(){
					
					if(i!=0) onselect(editor.transform())
					
				})
				
				
				dw.descriptionEditor(descriptionWrapper, s, {onedit:onedit, editable:(i===0), table:options.table}).draw()


				if(i===0&&s!=null){
					
					
					
					
					
					
					
					
					
					
					
					
					
					
					
					
					
					
					
					
					
					
				}


				sWrapper.append(descriptionWrapper)
				sWrapper.append(addButton)

				sWrapper.append(jQuery(document.createElement('div')).addClass('clear'));
				
				var hoverConfig = {
				     over: function(e){
						var old = editor.selected();
						if(old.length){
							deselect(old);
						}
						select(jQuery(e.currentTarget));
					},
				     timeout: 500,
				     out: function(){}
				};

				sWrapper.hoverIntent(hoverConfig)

				
				
				

				
				
				
				
				
				
				
			}
			else{
				sWrapper.addClass('emptySuggestion')
			}
			
		})
		jQuery('.suggestion').filter(':not(.suggestion.selected)').fadeTo(500, 1);			
		return editor;
	}
	
	editor.promote = function(){
		var fadeTime = 800, selected = editor.selected(), offset = selected.offset(),
			primary = jQuery('.suggestion:first'), primaryOffset = primary.offset(), top = primaryOffset.top-primary.height()-85, width = selected.width();
			
			
		jQuery('.suggestion').filter(':not(.suggestion.selected)').fadeTo(fadeTime, 0);			
		setTimeout(function(){
			selected.css('position', 'absolute').css('top', offset.top-selected.height()-100).width(width).animate({
							top:top,
							width:'+=0'
				}, 500,
				function(){
					onpromote(editor.transform())
				}
			)
		}, fadeTime)


		

	}
	
	
	editor.prev = function(){
		var old = editor.selected();
		if(old.length){
			deselect(old);	
			select(old.prev())
		}		
		else{
			select(jQuery('.suggestion:last'))
		}
	}
	
	editor.next = function(){
		var old = editor.selected();
		if(old.length){
			deselect(old);	
			select(old.next())
		}		
		else{
			select(jQuery('.suggestion:first'))
		}
		
	}

	editor.first_suggestion = function(){
		select(jQuery('.suggestion:nth-child(2)'))
	}
	editor.working = function(){
		select(jQuery('.suggestion:first'))
	}

	editor.first = function(){
		if(suggestions[0]){
			select(jQuery('.suggestion:first'))
		}
		else{
			select(jQuery('.suggestion:nth-child(2)'))
		}
	}
	
	editor.transform = function(){
		
		return suggestions[editor.selected().index()];
	}
	
	function select(e){
		e.addClass('selected')
		onhighlight({transform:editor.transform()})
	}
	
	function deselect(e){
		e.removeClass('selected')
	}
	
	editor.selected = function(){
		return jQuery('.suggestion.selected')
	}


	return editor;
	
}
dw.script = function(container, wrangler, options){
	var script = {};
	
	script.draw = function(){

		var descriptionWrapper, toggleButton, otherButton,  editorWraper, toggleClass, importScript, exportScript, title, header, initial = options.initial || [], onedit = options.onedit;
		
		header = dw.jq('div').addClass('menu').attr('id', 'scriptBanner')
		title = dw.jq('span').append('Script').attr('id', 'scriptTitle')
		importScript = dw.jq('span').append('Import').attr('id', 'scriptImport').addClass('scriptIO')
		exportScript = dw.jq('span').append('Export').attr('id', 'scriptExport').addClass('scriptIO')
		
		exportScript.click(options.onexport)
		
		container.append(header)
		header.append(title).append(exportScript);
		
		editorWrapper = jQuery(document.createElement('div')).attr('id','scriptTransformContainer')
		container.append(editorWrapper)

		var transforms = initial.concat(wrangler)

		transforms.forEach(function(t, i){

			if(!t.deleted()){

				var tWrapper = jQuery(document.createElement('div')).addClass('scriptTransform')

				
				tWrapper.addClass(t.status())
				
				descriptionWrapper = jQuery(document.createElement('div')).addClass('descriptionEditor')
				editorWrapper.append(tWrapper)
				
				otherButton = jQuery(document.createElement('div'));
				
				if(t.active()){
					otherButton.addClass('editorButton annotateButton')
				}
				else{
					otherButton.addClass('editorButton deleteButton').attr('title', 'Delete Transform')
					otherButton.click(function(){
						t.delete_transform();
						options.edit({transform:t, type:'delete'});
					})					
				}

				
				
				
				toggleButton = jQuery(document.createElement('div')).addClass('editorButton').addClass('primaryButton')
				
				
				if(t.invalid()){
					toggleButton.attr('title', t.errorMessage())
				}
				else{
					toggleButton.click(function(){
						
						t.toggle();
						options.edit({transform:t, type:(t.active()?'redo':'undo')});
					})
					
					if(t.inactive()){
						toggleButton.attr('title', 'Redo Transform')
					}
					else{
						toggleButton.attr('title', 'Undo Transform')
					}
				}
				
				
				

				dw.descriptionEditor(descriptionWrapper, t, {onedit:onedit, editable:true}).draw()

				var detail_toggler = dw.jq('div').addClass('detail_toggler')
				tWrapper.append(detail_toggler)

				tWrapper.append(descriptionWrapper)
				tWrapper.append(toggleButton)
				tWrapper.append(otherButton)
				tWrapper.append(jQuery(document.createElement('div')).addClass('clear'))				
				
				
				detail_toggler.click(function(event){
					if(t.show_details){
						hide_details();
						t.show_details = false;
					}
					else{
						show_details();
						t.show_details = true;
					}
				})
				var hide_details = function(){
					
					tWrapper.find('.detail_editor').remove();
					detail_toggler.removeClass('detail_expanded')
				}
				var show_details = function(){
					var detail_container = dw.jq('div').addClass('detail_editor');
					tWrapper.append(detail_container)
					dw.detail_editor(detail_container, t, {onedit:onedit}).draw()
					detail_toggler.addClass('detail_expanded')
				}
				if(t.show_details){
					show_details();
				}

			}
			


			
		})
		
		jQuery('.scriptTransform').mouseenter(function(){
			jQuery(this).addClass('selected')
		}).mouseleave(function(){
			jQuery(this).removeClass('selected')
		})
		
		return script;
	}
		
	return script;
}
dw.tmenu = function(container, options){
	
	var menu = {};
	
	var options = options || {}, interaction = options.interaction, transforms = options.transforms || [
		{name:'Text', sub:[{name:'Split', transform:dw.transform.SPLIT},{name:'Cut', transform:dw.transform.CUT},{name:'Extract', transform:dw.transform.EXTRACT},{name:'Edit', transform:dw.transform.CONVERT}]},
		{name:'Columns', sub:[{name:'Fill', transform:dw.transform.FILL},{name:'Translate', transform:dw.transform.TRANSLATE},{name:'Drop', transform:dw.transform.DROP},{name:'Merge', transform:dw.transform.MERGE}]},
		{name:'Rows', sub:[{name:'Delete', transform:dw.transform.FILTER},{name:'Fill', transform:dw.transform.FILL},{name:'Promote', transform:dw.transform.SET_NAME}]},
		{name:'Table', sub:[{name:'Fold', transform:dw.transform.FOLD},{name:'Unfold', transform:dw.transform.UNFOLD},{name:'Transpose', transform:dw.transform.TRANSPOSE}]},
		{name:'Clear', sub:[]}		
	];
	
	var ul = dw.jq('div').addClass('tmenu');
	container.attr('id', 'transformMenu');
	container.append(ul)
	menu.draw = function(){
		var list, title;
		transforms.forEach(function(m){
			list = dw.jq('span');
			ul.append(list);
			title = dw.jq('p').append(dw.jq('a').append(m.name));
			list.append(title)

			var subheight = 18 + 18*(m.sub.length);
			var sub = dw.jq('div').addClass('tsubmenu').height(subheight);
			list.append(sub);
			sub.hide();
			m.sub.forEach(function(s){
				title = dw.jq('p').addClass('submenu').append(dw.jq('a').append(s.name))
				
				sub.append(title)
				jQuery(title).click(function(e){
					interaction({type:dw.engine.filter, transform:s.transform})
				})
			})


			if(m.name!='Clear'){
				jQuery(list).click(function(){
					var position = jQuery(this).position();
					
					
					sub.css('left', position.left).css('top', position.top+20);
					sub.show();
					
					jQuery(this).height(subheight)
					jQuery(this).addClass('openMenu')
					

					
					
				}).mouseleave(function(){
					sub.hide();
					jQuery(this).height(19)
					jQuery(this).removeClass('openMenu')
					
				});
			}
			else{
				list.addClass('clearMenu').click(function(){
					options.onclear()
				})
			}
			
			
		})
		
		
		
		
		
		container.append(dw.jq('div').height(20))
		
	}
	
	
	
	
	return menu;
	
}
dw.transform_menu = function(container, options){
	
	var menu = {};
	
	var options = options || {}, interaction = options.interaction, transforms = options.transforms || [
		{name:'Text', sub:[{name:'Split', transform:dw.transform.SPLIT},{name:'Cut', transform:dw.transform.CUT},{name:'Extract', transform:dw.transform.EXTRACT},{name:'Edit', transform:dw.transform.CONVERT}]},
		{name:'Columns', sub:[{name:'Fill', transform:dw.transform.FILL},{name:'Translate', transform:dw.transform.TRANSLATE},{name:'Drop', transform:dw.transform.DROP},{name:'Merge', transform:dw.transform.MERGE}]},
		{name:'Rows', sub:[{name:'Wrap', transform:dw.transform.WRAP}, {name:'Delete', transform:dw.transform.FILTER},{name:'Promote', transform:dw.transform.SET_NAME}]},
		{name:'Table', sub:[{name:'Fold', transform:dw.transform.FOLD},{name:'Unfold', transform:dw.transform.UNFOLD},{name:'Transpose', transform:dw.transform.TRANSPOSE}]},
		{name:'Clear', sub:[]}		
	];
	
	var vis = d3.select('#'+container.attr('id')), editor = dw.jq('div').addClass('detail_editor_container')
	

		
	menu.draw = function(){
		
		
		var idx = d3.range(transforms.length)

		var sub = vis.append('div').attr('id', 'menu').selectAll('div.menu_group')
		  .data(idx)
		  .enter().append('div')
			.attr('class', 'menu_group')

	  sub.selectAll('div.menu_option')
		  .data(function(d, i){return d3.range(transforms[d].sub.length).map(function(){return d})})
		  .enter().append('div')
			.attr('class', function(d, i){return 'menu_option ' + transforms[d].sub[i].name} )
		  	.text(function(d, i) { return transforms[d].sub[i].name})
		    .on('mousedown', function(d, i){
				interaction({type:dw.engine.filter, transform:transforms[d].sub[i].transform})
		      })
	
	  jQuery(vis[0]).append(editor)
		
	}
	

	
	menu.update = function(e){
		editor.empty();
		jQuery('.selected_menu_option').removeClass('selected_menu_option')
		if(e.transform){
			var name = e.transform.name;
			if(name==='filter') name = 'delete';
			dw.transform_editor(editor, e.transform, {onedit:options.onedit, table:options.table}).draw()	
			jQuery('.menu_option.'+ name).addClass('selected_menu_option')
		}

	}
	
	
	menu.draw();
	
	menu.update({transform:dw.split()})


	
	
	
	
	
	
	
	


	return menu;
	
}
dw.transform_editor = function(container, transform, options){
	
	var editor = {};

	ignore = ['status', 'table', 'which', 'drop', 'insert_position', 'ignore_between', 'update', 'method']
	if(transform.name === 'filter') ignore.push('column')
	if(transform.name != 'split') ignore.push('result')
	editor.draw = function(){
		var clause, detail;
		transform.params().map(function(p){return p.substr(1)})
			.filter(function(param){
				return ignore.indexOf(param)===-1
			}).sort()
			.forEach(function(param){


				detail = dw.jq('div').addClass('editor_wrapper')
				var label = dw.jq('div').append(param.replace(/_/g, ' ')).addClass('label');
				detail.append(label)
			
				clause = dw.clause.get(transform, param)
				clause.done_editing = function(params){
					options.onedit(params);
				};
				detail.append(clause.editor(options).addClass('editor_input'))
			
				container.append(detail)
				detail.append(dw.jq('div').addClass('clear'))
			})
		
		return container;
		
	}
	
	
	return editor;
}
dw.wrangler = function(options){
	var tContainer = options.tableContainer, previewContainer = options.previewContainer, transformContainer = options.transformContainer, table = options.table, originalTable = table.slice(), temporaryTable, vtable, afterTable, transform,
		engine, suggestions, editor, wrangler = {}, script, w = dw.wrangle(), tableSelection, scriptContainer = jQuery(document.createElement('div')).attr('id','scriptContainer'), editorContainer = jQuery(document.createElement('div')).attr('id','editorContainer'), dashboardContainer = options.dashboardContainer;



	if(options.initial_transforms){
		options.initial_transforms.forEach(function(t){
			w.add(t);
		})
		w.apply([table]);
	}

console.log("HELLO "+dw.wrangler_export(table,{}));


	transformContainer.append(editorContainer).append(scriptContainer)

	engine = dw.engine().table(table);

	var transform_menu = dw.transform_menu(dashboardContainer, {interaction:interaction, onclear:clear_editor, onedit:interaction, table:undefined})


	function interaction(params){

		dw.log(params)
		var selection = tableSelection.add(params);

		params.rows = selection.rows();
		params.cols = selection.cols();
		suggestions = engine.table(table).input(params).run(13);
		transform = suggestions[0];

		drawEditor();



		if (enable_proactive) {

			if(params.type==dw.engine.clear ||
				 params.type==dw.engine.execute) {


				drawTable();
			}
			else if(params.type === dw.engine.promote){
				editor.working();
			}
			else{

				if(params.type===dw.engine.param){
					editor.working()
				}
				else{
					editor.first_suggestion();
				}
			}

		}
		else {

			if(params.type === dw.engine.promote){
				editor.working();
			}
			else{

				if(params.type===dw.engine.param){
					editor.working()
				}
				else{
					editor.first_suggestion();
				}
			}

		}

		transform_menu.update({transform:editor.transform() || transform})

	}

	function infer_schema(){
		var typeTransforms = dw.infer_type_transforms(table);
		typeTransforms.forEach(function(t){
			t.sample_apply([table])
		})
	}

	function table_change(){
		transform = editor.transform()
		drawTable();
	}

	var warned = false;
	function confirmation(options){
		if(!warned){
			warned = true;
			alert('Wrangler only supports up to 40 columns and 1000 rows.  We will preview only the first 40 columns and 1000 rows of data.')

		}
	}

	vtable = dw.vtable(tContainer, {
		interaction:interaction,
		ontablechange:table_change,
		onexecute:execute_transform,
		onconfirm:confirmation,
		wrangler:w
	})


	afterTable = dw.vtable(previewContainer, {
		interaction:function(params){}
	})

	function highlight_suggestion(params){
		preview(params.transform)

		transform_menu.update({transform:params.transform})
		dw.log({type:'highlight_suggestion', suggestion:params.transform})
	}

	function execute_transform(transform, params){
		transform.sample_apply([table]);
		dw.summary.clear_cache();
		infer_schema()
		w.add(transform)
		tableSelection.clear()
		interaction({type:dw.engine.execute, transform:transform})
		drawScript()

		var x = jQuery('#scriptTransformContainer')
		x.scrollTop(100000);


	}

	function clear_editor(){
		tableSelection.clear()
		interaction({type:dw.engine.clear})
	}


	function promote_transform(transform, params){
		tableSelection.clear()
		interaction({type:dw.engine.promote, transform:transform})
	}



	tableSelection = dw.table_selection(vtable);

	wrangler.draw = function(){
		suggestions = engine.table(table).run();
		drawTable();
		drawEditor()
		drawScript()
	}



	function drawTable(){
		preview(transform);
	}

	function drawEditor(){
		editorContainer.empty();

		editor = dw.editor(editorContainer, suggestions, {onpromote:promote_transform, onhighlight:highlight_suggestion, onselect:execute_transform, onedit:interaction, table:table}).draw()
	}

	function exportTable(){
		var select = dw.jq('select').addClass('exportOptions');

		var buttons = jQuery('<form>\
		<input type="radio" name="exportType" value="data" checked="checked"/> Data<br />\
		<input type="radio" name="exportType" value="script" /> Script\
		</form>')

		buttons.find(':radio').height(15).width(15).click(function(e){
			select.empty()
			add_export_options()
		})

		var add_export_options = function(){
			if(buttons.find(':radio:checked').val()=='data'){
				add_export_option('csv', 'Comma-Separated Values (CSV)');
				add_export_option('tsv', 'Tab-Separated Values (TSV)');
				add_export_option('rowjson', 'Row-Oriented JSON (One object per row)');
				add_export_option('columnjson', 'Column-Oriented JSON (One array per column)');
				add_export_option('lookup_table', 'Lookup Table (currently supports 2 column table)');
				inputArea.attr('value', dw.wrangler_export(table, {}))
				inputArea.focus();
				inputArea.select();
				dw.log({type:'export', params:{type:'csv'}})
			}
			else{
				add_export_option('python', 'Python');
				add_export_option('javascript', 'JavaScript');
				inputArea.attr('value', dw.wrangler_export(table, {format:'python', wrangler:w}))
				inputArea.focus();
				inputArea.select();
				dw.log({type:'export', params:{type:'python'}})
				instructions.empty();

								instructions.append(python)


			}
		}

		var add_export_option = function(type, name){


			dw.add_select_option(select, name, type);


		}
		var python = 'To run python code, run <span class=\'terminal\'>easy_install datawrangler</span> or download the <a class=\'runtimeLink\' href=\'http://vis.stanford.edu/wrangler/files/python/DataWrangler-0.1.tar.gz\' target=\'_blank\'> python runtime.</a>'
		var javascript = 'To run javascript code, download the <a class=\'runtimeLink\' href=\'http://vis.stanford.edu/wrangler/files/javascript/dwrt-r0.1.js\' target=\'_blank\'> javascript runtime</a>.'
		select.change(function(){
			inputArea.attr('value', dw.wrangler_export(table, {format:select.val(), wrangler:w}))
			jQuery('.exportHeader').removeClass('selectedExportHeader');
			jQuery(this).addClass('selectedExportHeader')
			inputArea.focus();
			inputArea.select();
			dw.log({type:'export', params:{type:select.val()}})
			instructions.empty();
			if(select.val()==='python'){
				instructions.append(python)
			}
			if(select.val()==='javascript'){
				instructions.append(javascript)
			}
		})


		jQuery("#table").hide();
		var upload = dw.jq('div').attr('id', 'uploadContainer')


		upload.append(buttons)


		upload.append(select);

		var instructions = dw.jq('div').attr('id', 'scriptInstructions')



		jQuery('#profilerCenterPanel').prepend(upload)

		add_export_option('csv', 'Comma-Separated Values (CSV)');
		add_export_option('tsv', 'Tab-Separated Values (TSV)');
		add_export_option('rowjson', 'Row-Oriented JSON (One object per row)');
		add_export_option('columnjson', 'Column-Oriented JSON (One array per column)');
		add_export_option('lookup_table', 'Lookup Table (currently supports 2 column table)');






		upload.append(dw.jq('button').attr('id','wranglerInputSubmit').append('Back to Wrangling')
						.click(function(){
							upload.remove();
							jQuery("#table").show()
						})

		)



		var inputArea = dw.jq('textArea').attr('id','wranglerInput');
		upload.append(inputArea)
		inputArea.attr('value', dw.wrangler_export(table, {}))

		jQuery('.exportHeader:first').addClass('selectedExportHeader');

		inputArea.focus();
		inputArea.select();

		upload.append(instructions)

		clear_editor();
		dw.log({type:'export', params:{type:'csv'}})
	}

	function script_interaction(params){
		temporaryTable = originalTable.slice();




		dw.progress_call(w.apply, w, [temporaryTable])
		dw.log({type:'edit_script', params:params})
		table = temporaryTable;
		clear_editor();
		wrangler.draw();

	}

	function drawScript(){
		var scrollTop = jQuery('#scriptTransformContainer').scrollTop();
		scriptContainer.empty();
		script = dw.script(scriptContainer, w, {
			edit:function(params){
				script_interaction(params)




			},
			onexport:exportTable,
			onedit:script_interaction,
			table:table





		}).draw()
		jQuery('#scriptTransformContainer').scrollTop(scrollTop);
	}

	function preview(transform){
		dw.preview(vtable, table, transform, afterTable, tableSelection);
	}


	jQuery(document).bind('keydown', function(event){

		var type = event && event.srcElement && event.srcElement.type

		if(type!='text'){
			switch(event.which){
		          	case 8:

		           	break
		        case 9:
					editor.promote()


					if(type!='textarea'){
		                event.preventDefault()
		            }
		            break
		        case 38:

					editor.prev()
					event.preventDefault()
		            break
		        case 40:

					editor.next()
		            event.preventDefault()
		            break
		        case 13:

					transform = editor.transform();
					execute_transform(transform)
					if(type!='textarea'){
		                event.preventDefault()
		            }
		            break
				case 27:
					clear_editor();
					break
		    }

		}
	    if(type!='textarea'){

	    }
	})

	infer_schema()




	wrangler.draw();



	return wrangler;
}
