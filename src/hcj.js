var apply = function (v) {
	return function (f) {
		return f(v);
	};
};
var constant = function (b) {
	return function (a) {
		return b;
	};
};
var curry = function (arr) {
	throw('the venerable curry, we shall define at a later date');
};
var id = function (a) {
	return a;
};
var add = function (a, b) {
	return a + b;
};
var subtract = function (a, b) {
	return a - b;
};
var mathMax = function (a, b) {
	return Math.max(a, b);
};
var mathMin = function (a, b) {
	return Math.min(a, b);
};
var px = function (n) {
	return n + 'px';
};

var pushStreams = [];
var pushValues = [];
var pushing = false;
var pushFlush = function () {
	var ps = pushStreams;
	var pv = pushValues;
	pushStreams = [];
	pushValues = [];
	pushing = false;
	for (var j = 0; j < ps.length; j++) {
		var s = ps[j];
		var v = pv[j];
	}
};
var pushNow = function (s, v) {
	pushStreams.push(s);
	pushValues.push(v);
	if (pushing === false) {
		pushing = true;
		setTimeout(function () {
			setTimeout(pushFlush);
		});
	}
};

var stream = {
	create: function () {
		return {
			listeners: [],
			lastValue: undefined,
		};
	},
	isStream: function (v) {
		return v &&
			v.hasOwnProperty('listeners') &&
			v.hasOwnProperty('lastValue');
	},
	once: function (v) {
		var s = stream.create();
		stream.push(s, v);
		return s;
	},
	push: function (s, v) {
		if (s.lastValue !== v) {
			s.lastValue = v;
			for (var i = 0; i < s.listeners.length; i++) {
				if (s.listeners[i]) {
					s.listeners[i](v);
				}
			}
		}
	},
	map: function (s, f) {
		var out = stream.create();
		if (s.lastValue !== undefined) {
			stream.push(out, f(s.lastValue));
		}
		s.listeners.push(function (v) {
			stream.push(out, f(v));
		});
		return out;
	},
	reduce: function (s, f, v1) {
		var out = stream.once(v1);
		if (s.lastValue !== undefined) {
			stream.push(out, f(out.lastValue, s.lastValue));
		}
		s.listeners.push(function (v) {
			stream.push(out, f(out.lastValue, v));
		});
		return out;
	},
	filter: function (s, f) {
		var out = stream.create();
		s.listeners.push(function (v) {
			if (f(v)) {
				stream.push(out, v);
			}
		});
		return stream;
	},
	onValue: function (s, f) {
		stream.map(s, function (v) {
			f(v);
			return true;
		});
		var index = s.listeners.length - 1;
		return function () {
			delete s.listeners[index];
		};
	},
	promise: function (s) {
		var d = $.Deferred();
		stream.map(s, function (v) {
			d.resolve(v);
		});
		return d.promise;
	},
	prop: function (s, str) {
		return stream.map(s, function (v) {
			return v[str];
		});
	},
	delay: function (s, amount) {
		var out = stream.create();
		stream.map(s, function (v) {
			setTimeout(function () {
				stream.push(out, v);
			}, amount);
		});
		return out;
	},
	debounce: function (s, amount) {
		var out = stream.create();
		var lastPushed = 0;
		var running = false;
		stream.map(s, function (v) {
			if (!running) {
				running = true;
				var d = new Date().getTime();
				setTimeout(function () {
					running = false;
					stream.push(out, s.lastValue);
					lastPushed = Math.max(lastPushed + amount, d);
				}, Math.max(0, (lastPushed + amount) - d));
			}
		});
		return out;
	},
	pushAll: function (source, target) {
		if (source.lastValue !== undefined) {
			stream.push(target, source.lastValue);
		}
		return stream.onValue(source, function (v) {
			stream.push(target, v);
		});
	},
	combine: function (streams, f) {
		var arr = [];
		var out = stream.create();

		var running = false;
		var tryRunF = function () {
			if (!running) {
				running = true;
				setTimeout(function () {
					running = false;
					for (var i = 0; i < streams.length; i++) {
						if (arr[i] === undefined) {
							return;
						}
					}
					stream.push(out, f.apply(null, arr));
				});
			}
		};

		streams.reduce(function (i, s) {
			if (s.lastValue !== undefined) {
				arr[i] = s.lastValue;
				tryRunF();
			}
			stream.onValue(s, function (v) {
				arr[i] = v;
				tryRunF();
			});
			return i + 1;
		}, 0);

		return out;
	},
	combineInto: function (streams, f, out) {
		var arr = [];

		var running = false;
		var tryRunF = function () {
			if (!running) {
				running = true;
				setTimeout(function () {
					running = false;
					for (var i = 0; i < streams.length; i++) {
						if (arr[i] === undefined) {
							return;
						}
					}
					stream.push(out, f.apply(null, arr));
				});
			}
		};

		streams.reduce(function (i, s) {
			if (s.lastValue !== undefined) {
				arr[i] = s.lastValue;
				tryRunF();
			}
			stream.onValue(s, function (v) {
				arr[i] = v;
				tryRunF();
			});
			return i + 1;
		}, 0);
	},
	all: function (streams, f) {
		return stream.combine(streams, function () {
			f.apply(null, arguments);
			return true;
		});
	},
	combineObject: function (streamsObject) {
		var keys = Object.keys(streamsObject);
		var obj = {};
		var out = stream.create();

		var running = false;
		var tryRunF = function () {
			if (!running) {
				running = true;
				setTimeout(function () {
					running = false;
					for (var i = 0; i < keys.length; i++) {
						var key = keys[i];
						if (obj[key] === undefined) {
							return;
						}
					}
					stream.push(out, $.extend({}, obj));
				});
			}
		};

		keys.map(function (key, i) {
			stream.onValue(streamsObject[key], function (v) {
				obj[key] = v;
				tryRunF();
			});
		});

		return out;
	},
	splitObject: function (obj) {
		var keys = Object.keys(obj);
		var streams = {};
		keys.map(function (key) {
			streams[key] = stream.once(obj[key]);
		});
		return streams;
	},
	fromEvent: function ($el, event) {
		var s = stream.create();
		$el.on('event', function (ev) {
			stream.push(s, ev);
		});
		return s;
	},
	fromPromise: function (p, initialValue) {
		var out = stream.create();
		if (initialValue) {
			stream.push(out, initialValue);
		}
		p.then(function (v) {
			stream.push(out, v);
		});
		return out;
	},
	cases: function (streams, indexS) {
		streams.push(indexS);
		return stream.combine(streams, function () {
			var args = Array.prototype.slice.call(arguments);
			var index = args.pop();
			return args[index];
		});
	},
};

var onceZeroS = stream.once(0);

var updateDomEls = [];
var updateDomProps = [];
var updateDomValues = [];
var runDomFuncs = function () {
	for (var i = 0; i < updateDomEls.length; i++) {
		updateDomEls[i].css(updateDomProps[i], updateDomValues[i]);
	}
	updateDomEls = [];
	updateDomProps = [];
	updateDomValues = [];
};
var updateDomFunc = function ($el, prop, value) {
	if (updateDomEls.length === 0) {
		setTimeout(runDomFuncs);
	}
	updateDomEls.push($el);
	updateDomProps.push(prop);
	updateDomValues.push(value);
};

var measureWidth = function ($el, w) {
	var $sandbox = $('.sandbox');
	var $clone = $el.clone();
	$clone.css('width', w ? px(w) : '')
		.css('height', '')
		.css('display', 'inline-block')
		.appendTo($sandbox);

	var width = parseFloat($clone.css('width'));
	$clone.remove();

	return width;
};

var measureHeight = function ($el) {
	return function (w) {
		var $sandbox = $('.sandbox');
		var $clone = $el.clone();
		$clone.css('width', px(w))
			.css('height', '')
			.appendTo($sandbox);

		var height = parseInt($clone.css('height'));

		$clone.remove();

		return height;
	};
};

var el = function (name, build, context) {
	var $el = $(document.createElement(name));
	$el.css('pointer-events', 'initial');

	if (name === 'textarea') {
		// give textareas a resize event
	}

	var unbuild = [];
	context.child = function (ctx) {
		ctx = ctx || {};
		['width', 'height', 'top', 'left', 'widthCss', 'heightCss', 'topCss', 'leftCss'].map(function (prop) {
			if (ctx[prop] === true) {
				ctx[prop] = stream.create();
			}
		});
		try {
			return {
				$el: $el,
				width: ctx.width || context.width,
				height: ctx.height || context.height,
				top: ctx.top || onceZeroS,
				left: ctx.left || onceZeroS,
				widthCss: ctx.widthCss,
				heightCss: ctx.heightCss,
				topCss: ctx.topCss,
				leftCss: ctx.leftCss,
				topAccum: stream.combine([context.topAccum, context.top], add),
				leftAccum: stream.combine([context.leftAccum, context.left], add),
				unbuild: unbuild.push,
			};
		}
		catch (e) {
			debugger;
		}
	};

	var instance = {
		$el: $el,
	};
	var streams = build($el, context, function (wd) {
		var w = measureWidth($el, wd);
		if (instance.minWidth) {
			stream.push(instance.minWidth, w);
		}
		else {
			instance.initialMinWidth = w;
		}
	}, function () {
		var h = measureHeight($el);
		if (instance.minHeight) {
			stream.push(instance.minHeight, h);
		}
		else {
			instance.initialMinHeight = h;
		}
	}) || {};
	instance.minWidth = streams.minWidth ? streams.minWidth : stream.create();
	instance.minHeight = streams.minHeight ? streams.minHeight : stream.create();
	if (instance.hasOwnProperty('initialMinWidth')) {
		stream.push(instance.minWidth, instance.initialMinWidth);
	}
	if (instance.hasOwnProperty('initialMinHeight')) {
		stream.push(instance.minHeight, instance.initialMinHeight);
	}

	$el.appendTo(context.$el);
	$el.before(' ');
	$el.after(' ');

	instance.destroy = function () {
		unbuild.map(apply());
		$el.remove();
	};

	return instance;
};

var callEl = function (name) {
	return function (build) {
		return function (context) {
			return el(name, build, context);
		};
	};
};

var a = callEl('a');
var button = callEl('button');
var div = callEl('div');
var form = callEl('form');
var iframe = callEl('iframe');
var img = callEl('img');
var input = callEl('input');
var label = callEl('label');
var li = callEl('li');
var option = callEl('option');
var p = callEl('p');
var select = callEl('select');
var textarea = callEl('textarea');
var ul = callEl('ul');
