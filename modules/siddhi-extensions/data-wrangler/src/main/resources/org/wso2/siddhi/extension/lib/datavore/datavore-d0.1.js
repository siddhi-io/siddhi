/**
 * @class The built-in Array class.
 * @name Array
 */

/**
 * Creates a new array with the results of calling a provided function on every
 * element in this array. Implemented in Javascript 1.6.
 *
 * @function
 * @name Array.prototype.map
 * @see <a
 * href="https://developer.mozilla.org/En/Core_JavaScript_1.5_Reference/Objects/Array/Map">map</a>
 * documentation.
 * @param {function} f function that produces an element of the new Array from
 * an element of the current one.
 * @param [o] object to use as <tt>this</tt> when executing <tt>f</tt>.
 */
if (!Array.prototype.map) Array.prototype.map = function(f, o) {
  var n = this.length;
  var result = new Array(n);
  for (var i = 0; i < n; i++) {
    if (i in this) {
      result[i] = f.call(o, this[i], i, this);
    }
  }
  return result;
};

/**
 * Creates a new array with all elements that pass the test implemented by the
 * provided function. Implemented in Javascript 1.6.
 *
 * @function
 * @name Array.prototype.filter
 * @see <a
 * href="https://developer.mozilla.org/En/Core_JavaScript_1.5_Reference/Objects/Array/filter">filter</a>
 * documentation.
 * @param {function} f function to test each element of the array.
 * @param [o] object to use as <tt>this</tt> when executing <tt>f</tt>.
 */
if (!Array.prototype.filter) Array.prototype.filter = function(f, o) {
  var n = this.length;
  var result = new Array();
  for (var i = 0; i < n; i++) {
    if (i in this) {
      var v = this[i];
      if (f.call(o, v, i, this)) result.push(v);
    }
  }
  return result;
};

/**
 * Executes a provided function once per array element. Implemented in
 * Javascript 1.6.
 *
 * @function
 * @name Array.prototype.forEach
 * @see <a
 * href="https://developer.mozilla.org/En/Core_JavaScript_1.5_Reference/Objects/Array/ForEach">forEach</a>
 * documentation.
 * @param {function} f function to execute for each element.
 * @param [o] object to use as <tt>this</tt> when executing <tt>f</tt>.
 */
if (!Array.prototype.forEach) Array.prototype.forEach = function(f, o) {
  var n = this.length >>> 0;
  for (var i = 0; i < n; i++) {
    if (i in this) f.call(o, this[i], i, this);
  }
};

/**
 * Apply a function against an accumulator and each value of the array (from
 * left-to-right) as to reduce it to a single value. Implemented in Javascript
 * 1.8.
 *
 * @function
 * @name Array.prototype.reduce
 * @see <a
 * href="https://developer.mozilla.org/En/Core_JavaScript_1.5_Reference/Objects/Array/Reduce">reduce</a>
 * documentation.
 * @param {function} f function to execute on each value in the array.
 * @param [v] object to use as the first argument to the first call of
 * <tt>t</tt>.
 */
if (!Array.prototype.reduce) Array.prototype.reduce = function(f, v) {
  var len = this.length;
  if (!len && (arguments.length == 1)) {
    throw new Error("reduce: empty array, no initial value");
  }

  var i = 0;
  if (arguments.length < 2) {
    while (true) {
      if (i in this) {
        v = this[i++];
        break;
      }
      if (++i >= len) {
        throw new Error("reduce: no values, no initial value");
      }
    }
  }

  for (; i < len; i++) {
    if (i in this) {
      v = f(v, this[i], i, this);
    }
  }
  return v;
};
// datavore!

/**
 * The top-level Datavore namespace. All public methods and fields should be
 * registered on this object. Note that core Datavore source is surrounded by an
 * anonymous function, so any other declared globals will not be visible outside
 * of core methods. This also allows multiple versions of Datavore to coexist,
 * since each version will see their own <tt>dv</tt> namespace.
 *
 * @namespace The top-level Datavore namespace, <tt>dv</tt>.
 */
var dv = {};

/**
 * Datavore major and minor version numbers.
 *
 * @namespace Datavore major and minor version numbers.
 */
dv.version = {
  /**
   * The major version number.
   *
   * @type number
   * @constant
   */
  major: 0,

  /**
   * The minor version number.
   *
   * @type number
   * @constant
   */
  minor: 1
};

/**
 * @private Reports the specified error to the JavaScript console. Mozilla only
 * allows logging to the console for privileged code; if the console is
 * unavailable, the alert dialog box is used instead.
 *
 * @param e the exception that triggered the error.
 */
dv.error = function(e) {
  (typeof console == "undefined") ? alert(e) : console.error(e);
};

/**
 * @private Registers the specified listener for events of the specified type on
 * the specified target. For standards-compliant browsers, this method uses
 * <tt>addEventListener</tt>; for Internet Explorer, <tt>attachEvent</tt>.
 *
 * @param target a DOM element.
 * @param {string} type the type of event, such as "click".
 * @param {function} the event handler callback.
 */
dv.listen = function(target, type, listener) {
  listener = dv.listener(listener);
  return target.addEventListener
      ? target.addEventListener(type, listener, false)
      : target.attachEvent("on" + type, listener);
};

/**
 * @private Returns a wrapper for the specified listener function such that the
 * {@link dv.event} is set for the duration of the listener's invocation. The
 * wrapper is cached on the returned function, such that duplicate registrations
 * of the wrapped event handler are ignored.
 *
 * @param {function} f an event handler.
 * @returns {function} the wrapped event handler.
 */
dv.listener = function(f) {
  return f.$listener || (f.$listener = function(e) {
      try {
        dv.event = e;
        return f.call(this, e);
      } finally {
        delete dv.event;
      }
    });
};



dv.array = function(n) {
	var a = [];
	for (var i=0; i<n; ++i) { a.push(0); }
	return a;
}

dv.keys = function(map) {
  var array = [];
  for (var key in map) {
    array.push(key);
  }
  return array;
};

/**
 * @param {number} start
 * @param {number=} stop
 * @param {number=} step
 */
dv.range = function(start, stop, step) {
  if (arguments.length == 1) { stop = start; start = 0; }
  if (step == null) step = 1;
  if ((stop - start) / step == Infinity) throw new Error("infinite range");
  var range = [],
       i = -1,
       j;
  if (step < 0) while ((j = start + step * ++i) > stop) range.push(j);
  else while ((j = start + step * ++i) < stop) range.push(j);
  return range;
};

dv.time = function(callback){
	var s = Date.now();
	callback();
	return Date.now()-s;
}

dv.compare_time = function(l, r){
	return dv.time(l) / dv.time(r);
}
// -- RANDOM NUMBER GENERATORS ------------------------------------------------

dv.rand = {};

dv.rand.uniform = function(min, max) {
	min = min || 0;
	max = max || 1;
	var delta = max - min;
	return function() {
		return min + delta * Math.random();
	}
};

dv.rand.integer = function(a, b) {
	if (b === undefined) {
		b = a;
		a = 0;
	}
	return function() {
		return a + Math.max(0, Math.floor(b*(Math.random()-0.001)));
	}
}

dv.rand.normal = function(mean, stdev) {
	mean = mean || 0;
	stdev = stdev || 1;
	var next = undefined;
	return function() {
		var x = 0, y = 0, rds, c;
		if (next !== undefined) {
			x = next;
			next = undefined;
			return x;
		}
		do {
			x = Math.random()*2-1;
			y = Math.random()*2-1;
			rds = x*x + y*y;
		} while (rds == 0 || rds > 1);
		c = Math.sqrt(-2*Math.log(rds)/rds); // Box-Muller transform
		next = mean + y*c*stdev;
		return mean + x*c*stdev;
	}
}

function typeOf(value) {
    var s = typeof value;
    if (s === 'object') {
        if (value) {
            if (typeof value.length === 'number' &&
                    !(value.propertyIsEnumerable('length')) &&
                    typeof value.splice === 'function') {
                s = 'array';
            }
        } else {
            s = 'null';
        }
    }
    return s;
}






dv.ivar = function(obj, ivars){
	if(typeOf(ivars) != 'array') ivars = [ivars]
	ivars.forEach(function(ivar){
		var initial;
		if(typeOf(ivar) === 'object'){
			initial = ivar.initial;
			ivar = ivar.name;
		}
		var name = "_"+ivar;
		obj[name] = initial;
		obj[ivar] = function(v){
			if(arguments.length){
				obj[name] = v
				return obj;
			}
			return obj[name]
		}
	})
}

dv.ivara = function(obj, ivars){
	if(typeOf(ivars) != 'array') ivars = [ivars]
	ivars.forEach(function(ivar){
		var initial;
		if(typeOf(ivar) === 'object'){
			initial = ivar.initial;
			if(initial!=undefined && typeOf(initial)!='array') initial = [initial];
			ivar = ivar.name;
		}
		var name = "_"+ivar;
		obj[name] = initial;
		obj[ivar] = function(v){
			if(arguments.length){
				obj[name] = (typeOf(v) === 'array' ? v : [v])
				return obj;
			}
			return obj[name]
		}
	})
}


dv.dom = function(tree){
	
	if(tree instanceof jQuery){
		return tree
	}
	if(typeof tree === 'string'){
		tree = {
			type:'textNode',
			value:tree
		}
	}
	
	var type = tree.type || 'div';
	var element;
	switch(type){
		case 'textNode':
			element = jQuery(document.createTextNode(tree.value))
			break
		case 'input':
		case 'div':
		case 'label':
		case 'select':
		case 'span':
		case 'button':
		case 'li':
		case 'textArea':
		case 'ul':
		case 'p':
		case 'a':
			element = jQuery(document.createElement(type))
			break
		default:
			throw new Error('invalid element type in build tree of dom utility')
	}

	(tree.classes || []).forEach(function(c){
		element.addClass(c)
	});

	(tree.selectOptions || []).forEach(function(o){
		var option = document.createElement("option");
		option.text = o.label;
		option.value = o.value;
		element.attr('options').add(option)
	});

	var attrs = (tree.extraAttrs || tree.attrs || {})
	pv.keys(attrs).forEach(function(a){
		element.attr(a, attrs[a])
	});

	(tree.children || []).forEach(function(child){
		element.append(dv.dom(child))
	});
	
	var events = tree.events || {};
	pv.keys(events).forEach(function(k){
		element[k].call(element, events[k])
	});
	return element
}

dv.log = function(x, b){
	return (b) ? Math.log(x) / Math.log(b) : Math.log(x)

}


// -- DATA TABLE --------------------------------------------------------------

dv.type = {
	nominal: "nominal",
	ordinal: "ordinal",
	numeric: "numeric",
	unknown: "unknown"
};



dv.table = function(input, o)
{
	var table = []; // the data table
	var metadata = {};
	table.addColumn = function(name, values, type, wranglerType, wranglerRole, o) {
		o = o || {};
		type = type || dv.type.unknown;
		wranglerType = wranglerType;
		wranglerRole = wranglerRole;


		
		name = clean_name(name)
		var compress = o.compressAll || (o.compress && (type===dv.type.nominal || type===dv.type.ordinal));
		var vals = values;
		
		if (compress) {
			vals = [];
			vals.lut = code(values);
			for (var i=0, map=dict(vals.lut); i<values.length; ++i) {
				vals.push(map[values[i]]);
			}
		}
		
		vals.name = name;
		vals.wrangler_type = wranglerType;
		vals.wrangler_role = wranglerRole;
		//vals.index = table.length;
		vals.index = function(){
			for(var i = 0; i < table.length; ++i){
				if(table[i].name===this.name){
					return i
				}
			}
			return -1;
		}
		vals.type = type;
		vals.table = table;
		vals.setName = function(n){
			var oldName = vals.name;
			if(oldName===n) return;
			n = clean_name(n)
			vals.name = n;
			delete table[oldName];
			table[n] = vals;

		}
		if(o.index === undefined){
			table.push(vals);	
		}
		else{
			table.splice(o.index, 0, vals)
		}
		
		
		
		table[name] = vals;
	}
	
	table.metadata = function(key, value){
		if(arguments.length > 1){
			metadata[key] = value;
			return table;
		}
		else{
			return metadata[key];
		}
	}
	
	table.removeColumn = function(col) {
		col = table[col] || null;
		if (col != null) {
			delete table[col.name];
			table.splice(col.index(), 1);
		}
		return col;
	}
	
	table.slice = function(start, end, o) {
		start = start || 0;
		if(end===undefined) end = table.rows()
	    var input = table.map(function(col){
			return {name:col.name, type:col.type, values:col.slice(start, end), wrangler_type:col.wrangler_type, wrangler_role:col.wrangler_role}
		})
		return dv.table(input, o)
	}
	
	table.slice_cols = function(start, end, o){
		start = start || 0, end = end || table.cols();
		var t = table.slice();
		
		for(var i = end; i < table.cols(); ++i){
			t.removeColumn(end);
		}
		
		for(var i = 0; i < start; ++i){
			t.removeColumn(0);
		}
		
		return t;
		
		// start = start || 0;
		// 		var cols = table.map(function(col){return col});
		// 		return cols.slice(start, end);
	}
	
	table.rows = function() { return table[0] ? table[0].length : 0; }


	table.row = function(r){
		return table.map(function(c){
			return c[r]
		});
	}

	table.cols = function() { return table.length; }
	
	table.names = function() {return table.map(function(col){return col.name})}
	table.types = function() {return table.map(function(col){return col.wrangler_type})}
	
	table.query = function(q) {
		var dims = [], sz = [1], hasDims = q.dims, narm=q.narm, filter=q.filter ? q.filter.table(table) : undefined, result = q.result, limit = q.limit || 10000, warn = q.warn || false;
		if (hasDims) {
			sz = [];
			for (i=0; i<q.dims.length; ++i) {
				var dim = q.dims[i], type = typeof dim;
				if (type === "string" || type === "number") {
					col = table[dim];
				} else if (dim.array) {
					col = dim.array(table[dim.value]);
				} else if (dim.values) {
					col = dim.values;
					col.lut = dim.lut;
				}
				dims.push(col);
				sz.push(col.lut.length);
			}
		}
		
		var vals = q.vals,                                       // aggregates
		    C = sz.reduce(function(a,b) { return a * b; }, 1),   // cube cardinality
		    N = table[0].length, p, col, v, name, expr,          // temp variables
			cnt, sum, ssq, min, max, first, _cnt, _sum, _ssq, _min, _max, _first,// aggregate columns
		    ctx = {}, emap = {}, exp = [], lut, // aggregate state variables
		    i=0, j=0, k=0, idx=0; // indices		


		if(limit && C > limit && warn) 	alert('This unfold operation requires computing over 10,000 values.  We currently do not support operations this large and will preview only a subset of values created.');

		C = (limit) ? Math.min(limit, C) : C;

		// Identify Requested Aggregates
		var star = false;
		for (i=0; i<vals.length; ++i) {
			var req = vals[i].init();
			for (expr in req) {
				if (expr == "*") {
					req[expr].map(function(func) {
						ctx[func] = dv.array(C);
					});
					star = true;
				} else {
					idx = table[expr].index();
					name = table[expr].name;
					req[expr].map(function(func) {
						ctx[func+"_"+name] = (ctx[func+"_"+idx] = dv.array(C));
					});
					if (!emap[idx]) {
						emap[idx] = true;
						exp.push(idx);
					}
				}
			}
		}
		if (exp.length==0 && star) exp.push(-1);

		// Compute Cube Index Coefficients
		for (i=0, p=[1]; i<sz.length; ++i) {
			p.push(p[i]*sz[i]);
		}
		
		// Compute Aggregates


		for (j=0; j<exp.length; ++j) {
			expr = exp[j];
			cnt = ctx["cnt"]; _cnt = (cnt && j==0);
			sum = ctx["sum_"+expr]; _sum = (sum !== undefined);
			ssq = ctx["ssq_"+expr]; _ssq = (ssq !== undefined);
			min = ctx["min_"+expr]; _min = (min !== undefined);
			max = ctx["max_"+expr]; _max = (max !== undefined);
			first = ctx["first_"+expr]; _first = (first !== undefined);
			col = table[expr];

			for (i=0; i<N; ++i) {
				if (col) v = col[i];
				for (idx=0, k=0; k<sz.length; ++k) {
					// compute cube index
					idx += p[k] * (hasDims ? dims[k][i] : 0);
				}
				if(narm&&isNaN(v)){
					continue;
				}				
				if(filter&&!filter.test(i)){
					continue;
				}
				if (_cnt) cnt[idx] += 1;
				if (_sum) sum[idx] += v;
				if (_ssq) ssq[idx] += v*v;
				if (_first && !first[idx]) first[idx] = col.lut[v];
				if (_min && v < min[idx]) min[idx] = v;
				if (_max && v > max[idx]) max[idx] = v;
				

			}
		}
		
		// Generate Results
		var result = [], stride = 1, s, val;
		

		

		for (i=0; i<dims.length; ++i) {
			col = [];
			lut = dims[i].lut;
			s = sz[i];
			val = 0;
			for (j=0, k=0, c=-1; j<C; ++j, ++k) {
				if (k == stride) { k = 0; val = (val + 1) % s; }
				col[j] = lut[val];
			}
			stride *= s;
			col.unique = lut.length;
			result.push(col);
		}
				
		vals.map(function(op) { result.push(op.done(ctx)); });
		
		
		// if(result === 'table'){
		// 	result.map(function(r){
		// 		return {}
		// 	})
		// }
		
		
		return result;
	}
	
	/** @private */
	function code(a) {
		var c = [], d = {}, v;
		for (var i=0; i<a.length; ++i) {
			if (d[v=a[i]] === undefined) { d[v] = 1; c.push(v); }
		}
		return c.sort();
	};
	
	/** @private */
	var counts = {};
	
	var parseInteger = function(v){
		var n = Number(v)
		if(isNaN(n) || (!n && (v+'').indexOf('0')===-1) || parseInt(n)!=n) return undefined
		return n;
	}
	
	function clean_name(name){

		if(parseInteger(name) != undefined){
			name = '_'+name;
		}
		if(name===undefined) name = '_'
		name = name.replace(/ /g, '_')
		var clean = name, count;
		while(table[clean]!=undefined){
			
			var count = counts[name];
			if(count){
				counts[name]++;
			}
			else{
				count = 0;
				counts[name]=1;
			}
			
			clean = name + counts[name];
			
			
		}
		return clean;
			
	}
	
	/** @private */
	function dict(lut) {
		return lut.reduce(function(a,b,i) { a[b] = i; return a; }, {});
	};

	if(typeOf(input)==='string'){
		input  = [{name:'data', values:[input], type:dv.type.nominal}]
	}

	// populate data table
	input.forEach(function(d) {
		table.addColumn(d.name, d.values, d.type, d.wrangler_type, d.wrangler_role, o);	
	});
	return table;
};
// -- QUERY OPERATORS ---------------------------------------------------------

dv.noop = function() {};

// -- aggregation (value) operators ---

dv.first = function(expr) {
	var op = {};
	op.init = function() {
		var o = {}; o[expr] = ["first"]; return o;
	}
	op.done = function(ctx) { return ctx["first_"+expr]; };
	op.value = expr;
	return op;
}



dv.count = function(expr) {
	var op = {};
	op.init = function() {
		return {"*":["cnt"]};
	}
	op.done = function(ctx) { return ctx["cnt"]; };
	op.value = expr;
	return op;
}

dv.min = function(expr) {
	var op = {};
	op.init = function() {
		var o = {}; o[expr] = ["min"]; return o;
	}
	op.done = function(ctx) { return ctx["min_"+expr]; };
	op.value = expr;
	return op;
}

dv.max = function(expr) {
	var op = {};
	op.init = function() {
		var o = {}; o[expr] = ["max"]; return o;
	}
	op.done = function(ctx) { return ctx["max_"+expr]; };
	op.value = expr;
	return op;
}

dv.sum = function(expr) {	
	var op = {};
	op.init = function() {
		var o = {}; o[expr] = ["sum"]; return o;
	}
	op.done = function(ctx) { return ctx["sum_"+expr]; };
	op.value = expr;
	return op;
}

dv.avg = function(expr) {	
	var op = {};
	op.init = function() {
		var o = {"*":["cnt"]}; o[expr] = ["sum"]; return o;
	};
	op.done = function(ctx) {
		var akey = "avg_"+expr, avg = ctx[akey];
		if (!avg) {
			var sum = ctx["sum_"+expr], cnt = ctx["cnt"];			
			ctx[akey] = (avg = sum.map(function(v,i) { return v/cnt[i]; }));
		}
		return avg;
	};
	op.value = expr;
	return op;
}

dv.variance = function(expr, sample) {
    var op = {}, adj = sample ? 1 : 0;
	op.init = function() {
		var o = {"*":["cnt"]}; o[expr] = ["sum","ssq"]; return o;
	};
	op.done = function(ctx) {
		var cnt = ctx["cnt"], sum = ctx["sum_"+expr], ssq = ctx["ssq_"+expr];
		var akey = "avg_"+expr, avg = ctx[akey];
		if (!avg) {
			ctx[akey] = (avg = sum.map(function(v,i) { return v/cnt[i]; }));
		}
		return ssq.map(function(v,i) { return (v - avg[i]/cnt[i]) / (cnt[i]-adj); });
	};
	op.value = expr;
	return op;
}

dv.stdev = function(expr, sample) {
	var op = dv.variance(expr, sample), end = op.done;
	op.done = function(ctx) {
		var dev = end(ctx);
		for (var i=0; i<dev.length; ++i) { dev[i] = Math.sqrt(dev[i]); }
		return dev;
	}
	return op;
}

// -- dimension operators ---

dv.bin = function(expr, step, min, max) {	
	var op = {};
	op.array = function(values) {
		var N = values.length, val, idx, i,
		    minv = min, maxv = max, minb = false, maxb = false;
		if (minv === undefined) { minv = Infinity; minb = true; }
		if (maxv === undefined) { maxv = -Infinity; maxb = true; }
		if (minb || maxb) {
			for (i=0; i<N; ++i) {
				val = values[i];
				if (minb && val < minv) minv = val;
				if (maxb && val > maxv) maxv = val;
			}
			if (minb) minv = Math.floor(minv / step) * step;
			if (maxb) maxv = Math.ceil(maxv / step) * step;
		}
		// compute index array
		var a = [], lut = (a.lut = []),
		    range = (maxv - minv), unique = Math.ceil(range/step);
		for (i=0; i<N; ++i) {
			val = values[i];
			if (val >= maxv) a.push(unique-1);
			else a.push(Math.floor(unique*(values[i]-minv)/range));
		}
		for (i=0; i<unique; ++i) {
			// multiply b/c adding garners round-off error
			lut.push(minv + i*step);
		}
		return a;
	}
	op.value = expr;
	return op;
}

// dv.prettyBin = function(expr) {	
// 	var op = {};
// 	op.array = function(values) {
// 		var N = values.length, val, idx, i,
// 		    minv = min, maxv = max, minb = false, maxb = false;
// 		minv = Infinity; minb = true;
// 		maxv = -Infinity; maxb = true;
//
// 		for (i=0; i<N; ++i) {
// 			val = values[i];
// 			if (val < minv) minv = val;
// 			if (val > maxv) maxv = val;
// 		}
// 		if (minb) minv = Math.floor(minv / step) * step;
// 		if (maxb) maxv = Math.ceil(maxv / step) * step;
// 		
// 		// compute index array
// 		var a = [], lut = (a.lut = []),
// 		    range = (maxv - minv), unique = Math.ceil(range/step);
// 		for (i=0; i<N; ++i) {
// 			val = values[i];
// 			if (val >= maxv) a.push(unique-1);
// 			else a.push(Math.floor(unique*(values[i]-minv)/range));
// 		}
// 		for (i=0; i<unique; ++i) {
// 			// multiply b/c adding garners round-off error
// 			lut.push(minv + i*step);
// 		}
// 		return a;
// 	}
// 	op.value = expr;
// 	return op;
// }

dv.quantile = function(expr, n) {	
	function search(array, value) {
	    var low = 0, high = array.length - 1;
		while (low <= high) {
		    var mid = (low + high) >> 1, midValue = array[mid];
		    if (midValue < value) low = mid + 1;
		    else if (midValue > value) high = mid - 1;
		    else return mid;
		}
		var i = -low - 1;
		return (i < 0) ? (-i - 1) : i;
	}

	var op = {};
	op.array = function(values) {
		// get sorted data values
		var i, d = values.sorted;
		if (!d) {
			var cmp;
			if (values.type && values.type === "numeric") {
				cmp = function(a,b) { return a-b; }
			} else {
				cmp = function(a,b) { return a<b ? -1 : a>b ? 1 : 0; }
			}
			values.sorted = (d = values.slice().sort(cmp));
		}
		// compute quantile boundaries
	    var q = [d[0]], a = [], lut = (a.lut = []);
        for (i=1; i<=n; ++i) {
			q[i] = d[~~(i * (d.length - 1) / n)];
			lut.push(i-1);
	    }
		// iterate through data and label quantiles
		for (i=0; i<values.length; ++i) {
			a.push(Math.max(0, search(q, values[i])-1));
		}
		return a;
	}
	op.value = expr;
	return op;
}

// -- filter operators

dv.compare = {
	eq:'eq',
	lt:'lt',
	le:'le',
	gt:'gt',
	ge:'ge',
	ne:'ne'
}

dv.eq = function(expr, compOp){
	if(typeof compOp != 'object') compOp = {val:compOp}
	return dv.compare(expr, dv.compare.eq, compOp)
}

dv.compare = function(expr, comp, compOp){
	var f = {};
	var col;
	f.table = function(t){
		col = t[expr]
		return this;
	}
	var val = compOp.val;
	switch(comp){
		case dv.compare.eq:
			f.test = function(i){
				return col[i] == val;
			}
			break;
		case dv.compare.ne:
			f.test = function(i){
				return col[i] != val;
			}
			break;
		case dv.compare.lt:
			f.test = function(i){
				return col[i] < val;
			}
			break;
		case dv.compare.le:
			f.test = function(i){
				return col[i] <= val;
			}
			break;
		case dv.compare.gt:
			f.test = function(i){
				return col[i] > val;
			}
			break;
		case dv.compare.ge:
			f.test = function(i){
				return col[i] >= val;
			}
			break;
	}
	return f;
}
dv.graph = function(N, src, trg) {
	var G = [], _links;
	G.nodes = N;
	G.edges = src.length;
	G.source = src;
	G.target = trg;
	
	G.init = function() {
		var i, u, v, links = [];
		for (i=0; i<N; ++i) {
			links.push([]);
		}
		for (i=0; i<src.length; ++i) {
			u = src[i];
			v = trg[i];
			links[u].push(v);
			links[v].push(u);
		}
		_links = links;
	}
	
	G.neighbors = function(n) {
		return _links[n];
	}
	
	G.init();
	return G;
}

// -- Node Statistics ---------------------------------------------------------

dv.graph.indegree = function(g)
{
	var i, N=g.nodes, E=g.edges, trg=g.target, deg = dv.array(N);
	for (i=0; i<E; ++i) deg[trg[i]] += 1;
	return deg;
}

dv.graph.outdegree = function(g)
{
	var i, N=g.nodes, E=g.edges, src=g.source, deg = dv.array(N);
	for (i=0; i<E; ++i) deg[src[i]] += 1;
	return deg;
}

dv.graph.degree = function(g)
{
	var i, N=g.nodes, E=g.edges, src=g.source, trg=g.target, deg = dv.array(N);
	for (i=0; i<E; ++i) {
		deg[src[i]] += 1;
		deg[trg[i]] += 1;
	}
	return deg;
}

/**
 * Calculates betweenness centrality measures for nodes in an unweighted graph.
 * The running time is O(|V|*|E|).
 * The algorithm used is due to Ulrik Brandes, as published in the
 * <a href="http://www.inf.uni-konstanz.de/algo/publications/b-fabc-01.pdf">
 * Journal of Mathematical Sociology, 25(2):163-177, 2001</a>.
 */
dv.graph.bc = function(g)
{
	var N = g.nodes, links, stack, queue,
	    i, j, n, v, w, s, sn, sv, sw;

	// Score objects track centrality statistics
	function score() {
		var s = {};
		s.centrality = 0;
		s.reset = function() {
			s.predecessors = [];
			s.dependency = 0;
			s.distance = -1;
			s.paths = 0;
			return s;
		}
		return s.reset();
	}
	
	// init 1 score per node
	for (n=0, s=[]; n<N; ++n) {
		s.push(score());
	}
	
	// compute centrality
	for (n=0; n<N; ++n) {
		for (i=0; i<N; ++i) { s[i].reset(); }
		sn = s[n];
		sn.paths = 1;
		sn.distance = 0;
		
		stack = [];
		queue = [n];
		
		while (queue.length > 0) {
			v = queue.shift();
			stack.push(v);
			sv = s[v];
			
			links = g.neighbors(v);
			for (i=0; i<links.length; ++i) {
				w = links[i];
				sw = s[w];
				if (sw.distance < 0) {
					queue.push(w);
					sw.distance = sv.distance + 1;
				}
				if (sw.distance == sv.distance + 1) {
					sw.paths += sv.paths;
					sw.predecessors.push(sv);
				}
			}
		}
		while (stack.length > 0) {
			sw = stack.pop();
			for (i=0; i<sw.predecessors.length; ++i) {
				sv = sw.predecessors[i];
				sv.dependency += (sv.paths/sw.paths) * (1+sw.dependency);
			}
			if (sw !== sn) sw.centrality += sw.dependency;
		}
	}
	return s.map(function(sc) { return sc.centrality; });
}

// -- Clustering --------------------------------------------------------------

dv.cluster = {};

dv.cluster.merge = function(a, b, p, n) {
	var m = {i:(+a),j:(+b),prev:p,next:n};
	if (p) p.next = m;
	if (n) n.prev = m;
	return m;
}

dv.cluster.community = function(matrix)
{
	var edge = dv.cluster.merge;
	function pass1(i,j,v) {
		if (i==j) {
			return 0;     // clear diagonal cells
		} else {
			zsum += v;    // sum other cells
			return v;
		}
	}
	function pass2(i,j,v) {
		v *= zsum;        // scale by matrix sum
		a[i] += v;        // sum columns
		e = edge(i,j,e);  // collect edges
		return v;
	}
	
	var dQ, maxDQ=0, Q=0, zsum=0, N=matrix.rows,
	    a = dv.array(N), Z, z, x, y, v, na, tmp, i, j, k,
	    xy, yx, xk, kx, yk, ky;
		scores=[], merges=edge(-1,-1), merge=merges,
		E = edge(-1,-1), e = E, maxEdge = edge(0,0);

    // initialize weighted matrix, column sums and edges
    Z = matrix.clone();
	Z.visitNonZero(pass1); zsum = 1/zsum;
	Z.visitNonZero(pass2);
	z = Z.values();

    // compute clustering
    for (i=0; i<N-1 && E.next; ++i) {
        maxDQ = -Infinity;
        maxEdge.i = 0;
		maxEdge.j = 0;

		for (e=E.next; e; e=e.next) {
			x = e.i; y = e.j;
			if (x == y) continue;
			// compute delta Q
			xy = x*N+y; yx = y*N+x;
			dQ = z[xy] + z[yx] - 2*a[x]*a[y];
			// check against max so far
			if (dQ > maxDQ) {
				maxDQ = dQ;
				maxEdge.i = x;
				maxEdge.j = y;
			}
		}

        // update the graph
        x = maxEdge.i; y = maxEdge.j;
        if (y < x) { tmp = y; y = x; x = tmp; } // lower idx first

		xy = x*N; yx = y*N;
        for (k=0, na=0; k<N; ++k) {
			xk = xy+k; yk = yx+k;
            v = z[xk] + z[yk];
            if (v != 0) {
                na += v;
				z[xk] = v;
				z[yk] = 0; // sparse?
            }
        }

        for (k=0; k<N; ++k) {
			kx = k*N+x; ky = k*N+y;
            v = z[kx] + z[ky];
			if (v != 0) {
				z[kx] = v;
				z[ky] = 0; // sparse?
            }
        }

        a[x] = na;
        a[y] = 0;

        // update edge list
		for (e=E.next; e; e=e.next) {
			if ( (e.i==x && e.j==y) || (e.i==y && e.j==x) ) {
				e.prev.next = e.next;
				if (e.next) e.next.prev = e.prev;
			} else if (e.i == y) {
				e.i = x;
			} else if (e.j == y) {
				e.j = x;
			}
		}
		
        Q += maxDQ;
        scores.push(Q);
		merge = edge(x, y, merge);
    }
	return {"merges":merges, "scores":scores};
}

dv.cluster.groups = function(mergelist, idx) {
	var merges = mergelist.merges,
	    scores = mergelist.scores,
	    map = {}, groups, gid=1,
	    max, i, j, e, k1, k2, l1, l2;
	
	if (idx === undefined || idx < 0) {
		for (i=0,idx=-1,max=-Infinity; i<scores.length; ++i) {
			if (scores[i] > max) { max = scores[idx=i]; }
		}
	}
	
	for (i=0, e=merges.next; i <= idx; ++i, e=e.next) {
		k1 = e.i; k2 = e.j;
		if ((l1 = map[k1]) === undefined) {
			l1 = [k1];
			map[k1] = l1;
		}
		if ((l2 = map[k2]) === undefined) {
			l1.push(k2);
		} else {
			for (j=0; j<l2.length; ++j) l1.push(l2[j]);
			delete map[k2];
		}
	}
	
	groups = dv.array(merges.length+1);
	for (k1 in map) {
		l1 = map[k1];
		for (i=0; i<l1.length; ++i) {
			groups[l1[i]] = gid;
		}
		++gid;
	}
	
	return groups;
}
/* Matrix API
 rows
 cols
 nnz
 sum (?)
 sumsq (?)
 clone
 like(rows, cols)
 init(rows, cols)
 get(i, j)
 set(i, j)
 scale(s)
 multiply(mat)
 visitNonZero(func)
 visit(func)
*/
dv.matrix = {};

dv.matrix.dense = function(rows, cols, vals) {
	var A = {}, _v = [], _c = cols, _r = rows;
	
	A.values = function() { return _v; }
	
	A.init = function(rows, cols, vals) {
		A.rows = (_r = rows);
		A.cols = (_c = cols);
		_v = [];
		if (vals) {
			for (var i=0; i<(rows*cols); ++i) {
				_v.push(vals[i]);
			}
		} else {
			for (var i=0; i<(rows*cols); ++i) { _v.push(0); }
		}
	}
	A.clone = function() { return dv.matrix.dense(_r, _c, _v); }
	A.like = function(rows, cols) { return dv.matrix.dense(_r, _c); };
	
	A.get = function(i,j) { return _v[i*_c + j]; }
	A.set = function(i,j,v) { _v[i*_c + j] = v; }
	
	A.scale = function(s) {
		for (var i=0; i<_v.length; ++i) { _v[idx] *= s; }
	}
	A.multiply = function(B) {
		if (_c !== B.rows) {
			dv.error("Incompatible matrix dimensions");
			return null;
		}
		var rows = _r, cols = B.cols, i, j, k, v, z;
		z = A.like(rows, cols);
		for (i=0; i<rows; ++i) {
			for (j=0, v=0; j<cols; ++j) {
				for (k=0; k<_c; ++k) {
					v += A.get(i,k) * B.get(k,j);
				}
				if (v) z.set(i,j,v);
			}
		}
		return z;
	}
	
	A.visitNonZero = function(f) {
		var k0, k, i, j;
		for (i=0; i<_r; ++i) {
			k0 = i*_c;
			for (j=0; j<_c; ++j) {
				k = k0 + j;
				u = _v[k];
				if (u) {
					v = f(i,j,u);
					_v[k] = v;
				}
			}
		}
	}
	
	A.visit = function(f) {
		var k0, k, i, j;
		for (i=0; i<_r; ++i) {
			k0 = i*_c;
			for (j=0; j<_c; ++j) {
				k = k0 + j;
				u = _v[k];
				v = f(i,j,u);
				_v[k] = v;
			}
		}
	}
			
	A.init(rows, cols, vals);
	return A;
}

/**
Sparse matrix definition.
*/
dv.matrix.sparse = function(rows, cols, vals) {
	var A = {}, _v = {}, _c = cols, _r = rows;
	
	A.values = function() { return _v; }
	
	A.init = function(rows, cols, vals) {
		A.rows = (_r = rows);
		A.cols = (_c = cols);
		_v = [];
		if (vals) {
			for (idx in vals) {
				_v[idx] = vals[idx];
			}
		}
	}
	A.clone = function() { return dv.matrix.sparse(_r, _c, _v); }
	A.like = function(rows, cols) { return dv.matrix.sparse(rows,cols); };
	
	A.get = function(i,j) {
		var v = _v[i*_c + j];
		return (v === undefined) ? 0 : v;
	}
	A.set = function(i,j,v) {
		var k = i*_c + j;
		if (v == 0) {
			delete _v[k];
		} else {
			_v[k] = v;
		}
	}
	
	A.scale = function(s) {
		for (var idx in _v) { _v[idx] *= s; }
	}
	A.multiply = function(B) {
		if (_c !== B.rows) {
			dv.error("Incompatible matrix dimensions");
			return null;
		}
		var rows = _r, cols = B.cols, i, j, k, v, z;
		z = A.like(rows, cols);
		for (i=0; i<rows; ++i) {
			for (j=0, v=0; j<cols; ++j) {
				for (k=0; k<_c; ++k) {
					v += A.get(i,k) * B.get(k,j);
				}
				if (v) z.set(i,j,v);
			}
		}
		return z;
	}
	
	A.visitNonZero = function(f) {
		var i, j, k, u, v, idx;
		for (idx in _v) {
			k = (+idx);
			j = k % _c;
			i = (k-j) / _c; // sub remainder for integer division
			u = _v[k];
			v = f(i,j,u);
			if (v==0) {
				delete _v[k];
			} else {
				_v[k] = v;
			}
		}
	}
	
	A.visit = function(f) {
		var k0, k, u, v, i, j;
		for (i=0; i<_r; ++i) {
			k0 = i*_c;
			for (j=0; j<_c; ++j) {
				k = k0 + j;
				u = _v[k];
				u = u ? (+u) : 0;
				v = f(i,j,u);
				if (v==0) {
					delete _v[k];
				} else {
					_v[k] = v;
				}
			}
		}
	}
	
	A.init(rows, cols, vals);
	return A;
};