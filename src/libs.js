var add = function (a, b) {
	return a + b;
};
var mathMax = function (a, b) {
	return Math.max(a, b);
};
var mathMin = function (a, b) {
	return Math.min(a, b);
};
var passThroughToFirst = function (instance, context, i) {
	i.minHeight.pushAll(instance.minHeight);
	i.minWidth.pushAll(instance.minWidth);
	return [{
		width: context.width,
		height: context.height,
		top: Stream.once(0),
		left: Stream.once(0),
	}];
};

var unit = function (unit) {
	return function (number) {
		return number + unit;
	};
};
var px = unit('px');

var url = function (str) {
	return 'url("' + str + '")';
};



var debugPrintHeight = function (instance) {
	instance.minHeight.map(function (mh) {
		console.log(mh);
		console.log(new Error().stack);
	});
};
var debugPrintWidth = function (instance) {
	instance.minWidth.map(function (mw) {
		console.log(mw);
		console.log(new Error().stack);
	});
};


// $$ :: String -> [*] -> Component -> Component
// applies a jquery function to the component instance after creation
var $$ = function (func) {
	return function () {
		var args = Array.prototype.slice.call(arguments);
		return function (i) {
			i.$el[func].apply(i.$el, args);
			if (i.$el.hasClass('text') ||
				i.$el.hasClass('paragraph') ||
				i.$el.hasClass('image') ||
				'BUTTON' === i.$el.prop('tagName') ||
				'INPUT' === i.$el.prop('tagName') ||
				'TEXTAREA' === i.$el.prop('tagName')) {
				i.updateDimensions(true);
				if (!i.$el.hasClass('waiting-for-width')) {
					i.$el.addClass('waiting-for-width');
					// record old min width
					var mw = findMinWidth(i.$el);
					var initialWait = 1000;
					var exp = 1.1;
					// see if min width has changed
					var tryNewWidth = function (waitTime) {
						return function () {
							var mw2 = findMinWidth(i.$el);
							if (mw2 !== mw) {
								i.updateDimensions(true);
								i.$el.removeClass('waiting-for-width');
							}
							else {
								// exponential backoff
								setTimeout(tryNewWidth(waitTime * exp), waitTime);
							}
						};
					};
					// if width hasn't immediately changed, then wait
					setTimeout(tryNewWidth(initialWait));
				}
			}
		};
	};
};

var $addClass = $$('addClass');
var $css = $$('css');
var $attr = $$('attr');
var $prop = $$('prop');

var $cssC = liftCF($css);

var chooseHeightFromWidth = function (instance, context) {
	var choosingHeight = false;
	context.width.onValue(function (w) {
		if (!choosingHeight) {
			choosingHeight = true;
			setTimeout(function () {
				var optimalHeight = findOptimalHeight(instance.$el, w);
				instance.minHeight.push(optimalHeight);
				choosingHeight = false;
			});
		}
	});
};
var $html = function (html, setWidth) {
	return function (instance, context) {
		instance.$el.html(html);
		chooseHeightFromWidth(instance, context);
		if (setWidth) {
			instance.updateDimensions();
		}
	};
};


var windowWidth = Stream.never();
var windowHeight = Stream.never();
var updateWindowWidth = function () {
	windowWidth.push(document.documentElement.clientWidth);
};
var updateWindowHeight = function () {
	windowHeight.push(window.innerHeight);
};
$(updateWindowWidth);
$(updateWindowHeight);
$(window).on('resize', function () {
	updateWindowWidth();
	updateWindowHeight();
});

var windowResize = Stream.once(null);
$(window).on('resize', function (e) {
	windowResize.push(e);
});

var windowScroll = Stream.never();
$(window).on('scroll', function () {
	windowScroll.push(window.scrollY);
});
windowScroll.push(window.scrollY);

var windowHash = Stream.never();
$(window).on('hashchange', function () {
	windowHash.push(location.hash);
});
windowHash.push(location.hash);


var withMinWidth = function (mw, end) {
	return function (i) {
		i.minWidth.push(mw);
		if (end) {
			i.minWidth.end();
		}
	};
};
var withMinHeight = function (mh, end) {
	return function (i) {
		i.minHeight.push(mh);
		if (end) {
			i.minHeight.end();
		}
	};
};
var adjustMinSize = function (config) {
	return function (c) {
		return div.all([
			child(c),
			wireChildren(function (instance, context, i) {
				i.minWidth.map(function (mw) {
					return config.mw(mw);
				}).pushAll(instance.minWidth);
				i.minHeight.map(function (mh) {
					return config.mh(mh);
				}).pushAll(instance.minHeight);
				return [{
					top: Stream.once(0),
					left: Stream.once(0),
					width: context.width,
					height: context.height,
				}];
			}),
		]);
	};
};
var link = function (i) {
	i.$el.css('cursor', 'pointer')
		.css('pointer-events', 'all');
};

var componentName = function (name) {
	return function (i) {
		i.$el.addClass(name);
	};
};

// var on = function (event) {
// 	return function ($el, handler) {
// 		return function (i) {
// 		};
// 	};
// };

// var click = onEvent('click');

var onThis = function (event) {
	return function (handler) {
		return function (i) {
			var disabled = false;
			i.$el.on(event, function (ev) {
				if (!disabled) {
					return handler(ev, function () {
						disabled = true;
						return function () {
							disabled = false;
						};
					}, i);
				}
			});
		};
	};
};
var changeThis = onThis('change');
var clickThis = onThis('click');
var inputPropertychangeThis = onThis('input propertychange');
var keydownThis = onThis('keydown');
var keyupThis = onThis('keyup');
var mousedownThis = onThis('mousedown');
var mousemoveThis = onThis('mousemove');
var mouseoverThis = onThis('mouseover');
var mouseoutThis = onThis('mouseout');
var mouseupThis = onThis('mouseup');
var submitThis = onThis('submit');

var hoverThis = function (cb) {
	return function (instance) {
		cb(false, instance);
		instance.$el.on('mouseover', function () {
			cb(true, instance);
		});
		instance.$el.on('mouseout', function () {
			cb(false, instance);
		});
	};
};

var hoverStream = function (stream, f) {
	f = f || function (v) {
		return v;
	};
	return function (instance) {
		instance.$el.css('pointer-events', 'initial');
		instance.$el.on('mouseover', function (ev) {
			stream.push(f(ev));
			ev.stopPropagation();
		});
		$('body').on('mouseover', function (ev) {
			stream.push(f(false));
		});
	};
};

var cssStream = function (style, valueS) {
	return function (instance) {
		valueS.map(function (value) {
			instance.$el.css(style, value);
		});
	};
};

var withBackgroundColor = function (stream, arg2) {
	// stream is an object
	if (!stream.map) {
		stream = Stream.once({
			backgroundColor: stream,
			fontColor: arg2,
		});
	}
	return function (i, context) {
		stream.map(function (colors) {
			var bc = colors.backgroundColor;
			var fc = colors.fontColor;
			context.backgroundColor.push(bc);
			setTimeout(function () {
				setTimeout(function () {
					var brightness = bc.a * colorBrightness(bc) +
						(1 - bc.a) * context.brightness.lastValue();
					context.fontColor.push(fc || (brightness > 0.5 ? black : white));
				});
			});
		});
	};
};
var withFontColor = function (fc) {
	return function (i, context) {
		context.fontColor.push(fc);
	};
};
var hoverColor = function (backgroundColor, hoverBackgroundColor, fontColor, hoverFontColor) {
	backgroundColor = colorString(backgroundColor || transparent);
	hoverBackgroundColor = colorString(hoverBackgroundColor || backgroundColor);
	fontColor = colorString(fontColor || black);
	hoverFontColor = colorString(hoverFontColor || fontColor);
	return hoverThis(function (h, instance) {
		instance.$el.css('background-color', h ? hoverBackgroundColor : backgroundColor);
		instance.$el.css('color', h ? hoverFontColor : fontColor);
	});
};

var keepAspectRatioCorner = function (config) {
	config = config || {};
	return function (c) {
		return div.all([
			$css('overflow', 'hidden'),
			child(c),
			wireChildren(function (instance, context, i) {
				i.minWidth.pushAll(instance.minWidth);
				i.minHeight.pushAll(instance.minHeight);

				var ctx = {
					top: Stream.create(),
					left: Stream.create(),
					width: Stream.create(),
					height: Stream.create(),
				};

				Stream.combine([
					i.minWidth,
					i.minHeight,
					context.width,
					context.height,
				], function (mw, mh, w, h) {
					var ar = mw / mh;
					var AR = w / h;

					// container is wider
					if ((!config.fillSpace && AR > ar) ||
						(config.fillSpace && AR < ar)) {
						var usedWidth = h * ar;

						var left;
						if (config.left) {
							left = 0;
						}
						else if (config.right) {
							left = w - usedWidth;
						}
						else {
							left = (w - usedWidth) / 2;
						}

						ctx.top.push(0);
						ctx.left.push(left);
						ctx.width.push(usedWidth);
						ctx.height.push(h);
					}
					// container is taller
					else {
						var usedHeight = w / ar;

						var top;
						if (config.top) {
							top = 0;
						}
						else if (config.bottom) {
							top = h - usedHeight;
						}
						else {
							top = (h - usedHeight) / 2;
						}

						ctx.top.push(top);
						ctx.left.push(0);
						ctx.width.push(w);
						ctx.height.push(usedHeight);
					}
				});

				return [ctx];
			}),
		]);
	};
};

var keepAspectRatio = keepAspectRatioCorner();
var keepAspectRatioFill = keepAspectRatioCorner({
	fillSpace: true,
});

var image = function (config) {
	var srcStream = (config.src && config.src.map) ? config.src : Stream.once(config.src);
	return img.all([
		componentName('image'),
		$css('pointer-events', 'all'),
		function (i, context) {
			srcStream.map(function (src) {
				i.$el.prop('src', src);
			});
			i.$el.css('display', 'none');

			i.$el.on('load', function () {
				i.$el.css('display', '');
				var nativeWidth = i.$el[0].naturalWidth;
				var nativeHeight = i.$el[0].naturalHeight;
				var aspectRatio = nativeWidth / nativeHeight;

				var initialMinWidth =
					config.minWidth ||
					config.chooseWidth ||
					nativeWidth;
				var initialMinHeight =
					config.minHeight ||
					config.chooseHeight ||
					(initialMinWidth / aspectRatio);
				i.minWidth.push(initialMinWidth);
				i.minHeight.push(initialMinHeight);

				var minWidth, minHeight;

				if (config.minWidth !== undefined && config.minWidth !== null) {
					minWidth = config.minWidth;
					minHeight = minWidth / aspectRatio;
					i.minWidth.push(minWidth);
					i.minHeight.push(minHeight);
				}
				else if (config.minHeight !== undefined && config.minHeight !== null) {
					minHeight = config.minHeight;
					minWidth = minHeight * aspectRatio;
					i.minWidth.push(minWidth);
					i.minHeight.push(minHeight);
				}
				else if (config.useNativeWidth) {
					i.minWidth.push(nativeWidth);
					i.minHeight.push(nativeHeight);
				}
				else {
					i.minWidth.push(nativeWidth);
				}
				if (!config.useNativeWidth) {
					context.width.map(function (width) {
						return width / aspectRatio;
					}).pushAll(i.minHeight);
				}
			});
		},
	]);
};

var linkTo = function (href, c) {
	return a.all([
		$prop('href', href),
		child(c.all([
			$css('pointer-events', 'initial'),
		])),
		wireChildren(passThroughToFirst),
	]);
};

var nothing = div.all([
	componentName('nothing'),
	withMinHeight(0),
	withMinWidth(0),
]);

var text = function (text) {
	if (!(text.map && text.push)) {
		text = Stream.once(text);
	}
	
	return div.all([
		componentName('text'),
		$css('pointer-events', 'all'),
		$css('white-space', 'nowrap'),
		function (instance, context) {
			chooseHeightFromWidth(instance, context);
			text.onValue(function (t) {
				if (t) {
					while (t.indexOf(' ') !== -1) {
						t = t.replace(' ', '&nbsp;');
					}
				}
				instance.$el.html(t);
				instance.minWidth.push(findMinWidth(instance.$el));
			});
		},
	]);
};
var faIcon = function (str) {
	return text('<i class="fa fa-' + str + '"></i>');
};
var paragraph = function (text, minWidth) {
	if (!(text.map && text.push)) {
		text = Stream.once(text);
	}

	minWidth = minWidth || 0;
	
	return div.all([
		componentName('paragraph'),
		$css('pointer-events', 'all'),
		function (instance, context) {
			chooseHeightFromWidth(instance, context);
			instance.updateDimensions = function () {
				instance.minWidth.push(findScrollWidth(instance.$el, minWidth));
			};
			text.onValue(function (t) {
				instance.$el.html(t);
				instance.updateDimensions();
			});
			Stream.combine([
				text,
				context.width,
			], function (text, w) {
				var optimalHeight = findOptimalHeight(instance.$el, w);
				instance.minHeight.push(optimalHeight);
			});
		},
	]);
};

var ignoreSurplusWidth = function (_, cols) {
	return cols;
};
var ignoreSurplusHeight = function (_, rows) {
	return rows;
};
var centerSurplusWidth = function (gridWidth, positions) {
	var lastPosition = positions[positions.length - 1];
	var surplusWidth = gridWidth - (lastPosition.left + lastPosition.width);
	var widthPerCol = surplusWidth / positions.length;
	positions.map(function (position, i) {
		position.left += surplusWidth / 2;
	});
	return positions;
};
var evenSplitSurplusWidth = function (gridWidth, positions) {
	var lastPosition = positions[positions.length - 1];
	var surplusWidth = gridWidth - (lastPosition.left + lastPosition.width);
	var widthPerCol = surplusWidth / positions.length;
	positions.map(function (position, i) {
		position.width += widthPerCol;
		position.left += i * widthPerCol;
	});
	return positions;
};
var justifySurplusWidth = function (gridWidth, positions) {
	var lastPosition = positions[positions.length - 1];
	var surplusWidth = gridWidth - (lastPosition.left + lastPosition.width);
	positions.map(function (position, i) {
		for (var index = 0; index < i; index++) {
			position.left += surplusWidth / (positions.length - 1);
		}
	});
	return positions;
};
var justifyAndCenterSurplusWidth = function (gridWidth, positions) {
	var lastPosition = positions[positions.length - 1];
	var surplusWidth = gridWidth - (lastPosition.left + lastPosition.width);
	positions.map(function (position, i) {
		position.left += i * surplusWidth / (positions.length) +
			surplusWidth / (2 * positions.length);
	});
	return positions;
};
var surplusWidthAlign = function (t) {
	return function (gridWidth, positions) {
		var lastPosition = positions[positions.length - 1];
		var surplusWidth = gridWidth - (lastPosition.left + lastPosition.width);
		positions.map(function (position, i) {
			position.left += t * surplusWidth;
		});
		return positions;
	};
};
var surplusWidthAlignLeft = surplusWidthAlign(0);
var surplusWidthAlignCenter = surplusWidthAlign(0.5);
var surplusWidthAlignRight = surplusWidthAlign(1);
var superSurplusWidth = function (gridWidth, positions) {
	var lastPosition = positions[positions.length - 1];
	var surplusWidth = gridWidth - (lastPosition.left + lastPosition.width);
	if (positions.length === 1) {
		// if we're the only thing on the row, stretch up to roughly
		// double our min width
		if (surplusWidth < positions[0].width) {
			return evenSplitSurplusWidth(gridWidth, positions);
		}
		else {
			return positions;
		}
	}
	if (positions.length === 2) {
		// if there are two things in the row, make two columns each
		// with centered content
		return justifyAndCenterSurplusWidth(gridWidth, positions);
	}
	// if there are 3+ things in the row, then justify
	return justifySurplusWidth(gridWidth, positions);
};

var giveToNth = function (n) {
	return function (gridWidth, positions) {
		var lastPosition = positions[positions.length - 1];
		var surplusWidth = gridWidth - (lastPosition.left + lastPosition.width);
		positions.map(function (position, i) {
			if (i === n || (i === positions.length - 1 && n >= positions.length)) {
				position.width += surplusWidth;
			}
			else if (i > n) {
				position.left += surplusWidth;
			}
		});
		return positions;
	};
};
var giveToFirst = giveToNth(0);
var giveToSecond = giveToNth(1);
var giveToThird = giveToNth(2);

var giveHeightToNth = function (n) {
	return function (totalHeight, positions) {
		var lastPosition = positions[positions.length - 1];
		var surplusHeight = totalHeight - (lastPosition.top + lastPosition.height);
		positions.map(function (position, i) {
			if (i === n || (i === positions.length - 1 && n >= positions.length)) {
				position.height += surplusHeight;
			}
			else if (i > n) {
				position.top += surplusHeight;
			}
		});
		return positions;
	};
};
var slideshow = function (config, cs) {
	config.gutterSize = config.gutterSize || 0;
	config.leftTransition = config.leftTransition || 'none';
	config.alwaysFullWidth = config.alwaysFullWidth || false;
	return div.all([
		$css('overflow', 'hidden'),
		componentName('slideshow'),
		children(cs.map(function (c) {
			return c.all([
				$css('transition', 'left ' + config.leftTransition),
			]);
		})),
		wireChildren(function (instance, context, is) {
			var allMinWidths = Stream.combine(is.map(function (i) {
				return i.minWidth;
			}), function () {
				var args = Array.prototype.slice.call(arguments);
				return args;
			});

			allMinWidths.onValue(function (mws) {
				instance.minWidth.push(mws.reduce(add, config.gutterSize * (is.length - 1)));
			});

			var contexts = is.map(function () {
				return {
					top: Stream.once(0),
					left: Stream.never(),
					width: Stream.never(),
					height: context.height,};});

			Stream.all([config.selectedS, context.width, allMinWidths], function (selected, width, mws) {
				var selectedLeft = 0;
				var selectedWidth = 0;
				var left = 0;
				var positions = mws.map(function (mw, index) {
					mw = config.alwaysFullWidth ? width : mw;
					if (selected === index) {
						selectedLeft = left + config.gutterSize * index;
						selectedWidth = mw;
					}
					var position = {
						left: left + config.gutterSize * index,
						width: mw };
					left += mw;
					return position;
				});
				var dLeft = (width - selectedWidth) / 2 - selectedLeft;
				positions.map(function (position) {
					position.left += dLeft;
				});

				positions.map(function (position, index) {
					var ctx = contexts[index];
					ctx.left.push(position.left);
					ctx.width.push(position.width);
				});
			});

			Stream.combine(is.map(function (i) {
				return i.minHeight;
			}), function () {
				var args = Array.prototype.slice.call(arguments);
				var height = args.reduce(mathMax, 0);
				
				contexts.map(function (ctx) {
					ctx.height.push(height);
				});
				
				return height;
			}).pushAll(instance.minHeight);

			return [contexts];
		}),
	]);
};
var slideshowVertical = function (config, cs) {
	config.gutterSize = config.gutterSize || 0;
	config.topTransition = config.topTransition || 'none';
	config.alwaysFullHeight = config.alwaysFullHeight || false;
	return div.all([
		$css('overflow', 'hidden'),
		componentName('slideshow'),
		children(cs.map(function (c) {
			return c.all([
				$css('transition', 'top ' + config.topTransition),
			]);
		})),
		wireChildren(function (instance, context, is) {
			var allMinHeights = Stream.combine(is.map(function (i) {
				return i.minHeight;
			}), function () {
				var args = Array.prototype.slice.call(arguments);
				return args;
			});

			allMinHeights.onValue(function (mhs) {
				instance.minHeight.push(mhs.reduce(mathMax, 0));
			});

			var contexts = is.map(function (i) {
				return {
					top: Stream.never(),
					left: Stream.once(0),
					width: context.width,
					height: i.minHeight,
				};
			});

			Stream.all([
				config.selected,
				context.height,
				allMinHeights
			], function (selected, height, mhs) {
				var selectedTop = 0;
				var selectedHeight = 0;
				var top = 0;
				var positions = mhs.map(function (mh, index) {
					mh = config.alwaysFullHeight ? height : mh;
					if (selected === index) {
						selectedTop = top + config.gutterSize * index;
						selectedHeight = mh;
					}
					var position = {
						top: top + config.gutterSize * index,
						height: mh
					};
					top += mh;
					return position;
				});
				var dTop = (height - selectedHeight) / 2 - selectedTop;
				positions.map(function (position) {
					position.top += dTop;
				});

				positions.map(function (position, index) {
					var ctx = contexts[index];
					ctx.top.push(position.top);
					ctx.height.push(position.height);
				});
			});

			Stream.combine(is.map(function (i) {
				return i.minWidth;
			}), function () {
				var args = Array.prototype.slice.call(arguments);
				var width = args.reduce(mathMax, 0);

				return width;
			}).pushAll(instance.minWidth);

			return [contexts];
		}),
	]);
};

var sideBySide = function (config, cs) {
	config.gutterSize = config.gutterSize || 0;
	config.handleSurplusWidth = config.handleSurplusWidth || ignoreSurplusWidth;
	return div.all([
		componentName('sideBySide'),
		children(cs),
		wireChildren(function (instance, context, is) {
			var allMinWidths = Stream.combine(is.map(function (i) {
				return i.minWidth;
			}), function () {
				var args = Array.prototype.slice.call(arguments);
				return args;
			});
			
			allMinWidths.onValue(function (mws) {
				instance.minWidth.push(mws.reduce(add, config.gutterSize * (is.length - 1)));
			});
			
			var contexts = is.map(function () {
				return {
					top: Stream.once(0),
					left: Stream.never(),
					width: Stream.never(),
					height: context.height,
				};
			});

			Stream.all([context.width, allMinWidths], function (width, mws) {
				var left = 0;
				var positions = mws.map(function (mw, index) {
					var position = {
						left: left + config.gutterSize * index,
						width: mw,
					};
					left += mw;
					return position;
				});
				positions = config.handleSurplusWidth(width, positions);

				positions.map(function (position, index) {
					var ctx = contexts[index];
					ctx.left.push(position.left);
					ctx.width.push(position.width);
				});
			});

			Stream.combine(is.map(function (i) {
				return i.minHeight;
			}), function () {
				var args = Array.prototype.slice.call(arguments);
				return args.reduce(mathMax, 0);
			}).pushAll(instance.minHeight);
			
			return [contexts];
		}),
	]);
};

var slider = function (config, cs) {
	config = config || {};
	config.leftTransition = config.leftTransition || '0s';
	var grabbedS = Stream.once(false);
	var edge = {
		left: 'left',
		right: 'right',
	};
	var stateS = Stream.once({
		index: 0,
		edge: 'left',
	});
	var xCoord = 0;
	return div.all([
		componentName('slider'),
		$css('overflow-x', 'hidden'),
		$css('cursor', 'move'),
		children(cs),
		wireChildren(function (instance, context, is) {
			var allMinWidths = Stream.combine(is.map(function (i) {
				return i.minWidth;
			}), function () {
				var args = Array.prototype.slice.call(arguments);
				return args;
			});
			var totalMinWidthS = allMinWidths.map(function (mws) {
				return mws.reduce(add, 0);
			});
			allMinWidths.onValue(function (mws) {
				instance.minWidth.push(mws.reduce(mathMax, 0));
			});

			Stream.combine(is.map(function (i) {
				return i.minHeight;
			}), function () {
				var args = Array.prototype.slice.call(arguments);
				return args.reduce(mathMax, 0);
			}).pushAll(instance.minHeight);

			var leftS = Stream.combine([
				context.width,
				allMinWidths,
				stateS,
				grabbedS
			], function (width, mws, state, grabbed) {
				// configure left to be the left parameter of the first article in the slider
				var left = state.edge === 'left' ? 0 : width; // would love to case split
				mws.map(function (mw, index) {
					if (index < state.index) {
						left -= mw;
					}
					if (state.edge === 'right' && index === state.index) {
						left -= mw;
					}
				});
				if (grabbed !== false) {
					left += grabbed;
				}
				return left;
			});

			var leftsS = Stream.combine([
				allMinWidths,
				leftS,
			], function (mws, left) {
				return mws.reduce(function (acc, v) {
					acc.arr.push(acc.lastValue);
					acc.lastValue += v;
					return acc;
				}, {
					arr: [],
					lastValue: left,
				}).arr;
			});

			instance.$el.css('user-select', 'none');
			instance.$el.on('mousedown', function (ev) {
				ev.preventDefault();
				grabbedS.push(0);
				is.map(function (i) {
					i.$el.css('transition', 'left 0s');
				});
			});
			var release = function (ev) {
				is.map(function (i) {
					i.$el.css('transition', 'left ' + config.leftTransition);
				});
				var mws = allMinWidths.lastValue();
				var width = context.width.lastValue();
				var grabbed = grabbedS.lastValue();
				if (!grabbed) {
					return;
				}
				var left = leftS.lastValue();
				// array of sums of min widths
				var edgeScrollPoints = mws.reduce(function (a, mw) {
					var last = a[a.length - 1];
					a.push(last - mw);
					return a;
				}, [0]);
				var closest = edgeScrollPoints.reduce(function (a, scrollPoint, index) {
					var leftDistanceHere = Math.abs(scrollPoint - left);
					var rightDistanceHere = Math.abs(scrollPoint - (left - width));
					return {
						left: leftDistanceHere < a.left.distance ? {
							distance: leftDistanceHere,
							index: index,
						} : a.left,
						right: rightDistanceHere < a.right.distance ? {
							distance: rightDistanceHere,
							index: index - 1,
						} : a.right,
					};
				}, {
					left: {
						distance: Number.MAX_VALUE,
						index: -1,
					},
					right: {
						distance: Number.MAX_VALUE,
						index: -1,
					},
				});
				if (closest.left.distance <= closest.right.distance) {
					stateS.push({
						index: closest.left.index,
						edge: 'left',
					});
				}
				else {
					stateS.push({
						index: closest.right.index,
						edge: 'right',
					});
				}
				grabbedS.push(false);
				ev.preventDefault();
			};
			instance.$el.on('mouseup', release);
			instance.$el.on('mouseout', release);
			instance.$el.on('mousemove', function (ev) {
				var grabbed = grabbedS.lastValue();
				var totalMinWidth = totalMinWidthS.lastValue();
				var width = context.width.lastValue();
				var left = leftS.lastValue();
				if (grabbed !== false) {
					var dx = ev.clientX - xCoord;
					var left2 = left + dx;
					left2 = Math.min(0, left2);
					if (totalMinWidth > width) {
						left2 = Math.max(width - totalMinWidth, left2);
					}
					dx = left2 - left;
					grabbed = grabbed + dx;
					grabbedS.push(grabbed);
				}
				xCoord = ev.clientX;
			});

			return [is.map(function (i, index) {
				return {
					top: Stream.once(0),
					left: leftsS.map(function (lefts) {
						return lefts[index];
					}),
					width: i.minWidth,
					height: context.height,
				};
			})];
		}),
	]).all([
	]);
};

var stack2 = function (config, cs) {
	config.gutterSize = config.gutterSize || 0;
	config.handleSurplusHeight = config.handleSurplusHeight || ignoreSurplusHeight;
	return div.all([
		componentName('stack2'),
		children(cs),
		wireChildren(function (instance, context, is) {
			var allMinHeights = Stream.combine(is.map(function (i) {
				return i.minHeight;
			}), function () {
				var args = Array.prototype.slice.call(arguments);
				return args;
			});
			
			allMinHeights.onValue(function (mhs) {
				instance.minHeight.push(mhs.reduce(add, config.gutterSize * (is.length - 1)));
			});
			
			var contexts = is.map(function () {
				return {
					top: Stream.create(),
					left: Stream.once(0),
					width: context.width,
					height: Stream.never(),
				};
			});

			Stream.all([context.height, allMinHeights], function (height, mhs) {
				var top = 0;
				var positions = mhs.map(function (mh, index) {
					var position = {
						top: top + config.gutterSize * index,
						height: mh,
					};
					top += mh;
					return position;
				});
				positions = config.handleSurplusHeight(height, positions);

				positions.map(function (position, index) {
					var ctx = contexts[index];
					ctx.top.push(position.top);
					ctx.height.push(position.height);
				});
			});

			Stream.combine(is.map(function (i) {
				return i.minWidth;
			}), function () {
				var args = Array.prototype.slice.call(arguments);
				return args.reduce(mathMax, 0);
			}).pushAll(instance.minWidth);
			
			return [contexts];
		}),
	]);
};

var intersperse = function (arr, v) {
	var result = [];
	arr.map(function (el) {
		result.push(el);
		result.push(v);
	});
	result.pop();
	return result;
};

var stackTwo = function (options, cs) {
	options.gutterSize = options.gutterSize || 0;
	options.align = options.align || 'left';
	return div.all([
		componentName('stack'),
		children(cs),
		wireChildren(function (instance, context, is) {
			var i1 = is[0];
			var i2 = is[1];
			
			var gutterSize = options.gutterSize;
			var contexts = [];

			instance.minHeight.push(0);
			instance.minWidth.push(0);

			Stream.combine([
				i1.minHeight,
				i2.minHeight,
			], function (mh1, mh2) {
				return mh1 + mh2 + gutterSize;
			}).pushAll(instance.minHeight);

			Stream.combine([
				i1.minWidth,
				i2.minWidth,
			], function (mw1, mw2) {
				return Math.max(mw1, mw2);
			}).pushAll(instance.minWidth);
			
			return [[{
				top: Stream.once(0),
				left: Stream.once(0),
				width: context.width,
				height: i1.minHeight,
			}, {
				top: i1.minHeight.map(function (mh) {
					return mh + gutterSize;
				}),
				left: Stream.once(0),
				width: context.width,
				height: i2.minHeight,
			}]];
		}),
	]);
};

var stack = function (options, cs) {
	options.gutterSize = options.gutterSize || 0;
	options.collapseGutters = options.collapseGutters || false;
	options.align = options.align || 'left';
	if (cs.length === 0) {
		cs = [nothing];
	}
	return div.all([
		componentName('stack'),
		children(cs),
		wireChildren(function (instance, context, is) {
			var gutterSize = (options && options.gutterSize) || 0;
			var totalMinHeightStream = function (is) {
				if (is.length === 0) {
					return Stream.once(0);
				}
				return Stream.combine(is.map(function (i, index) {
					var iMinHeight;
					
					if (options && options.mhs && options.mhs[index]) {
						var optionMinHeight = options.mhs[index](context);
						return Stream.combine([i.minHeight, optionMinHeight], mathMax);
					}
					else {
						return i.minHeight;
					}
					return i.minHeight;
				}), function () {
					var args = Array.prototype.slice.call(arguments);
					var mh = args.reduce(function (a, b) {
						return a + b + ((options.collapseGutters && b === 0) ? 0 : gutterSize);
					}, -gutterSize);
					return mh;
				});
			};
			
			var contexts = [];
			is.reduce(function (is, i, index) {
				var top = totalMinHeightStream(is).map(function (t) {
					return t + ((index === 0 || (options.collapseGutters && t === 0)) ? 0 : gutterSize);
				});
				var iMinHeight;
				
				if (options && options.mhs && options.mhs[is.length]) {
					var optionMinHeight = options.mhs[is.length](context);
					iMinHeight = Stream.combine([i.minHeight, optionMinHeight], mahtMax);
				}
				else {
					iMinHeight = i.minHeight;
				}

				contexts.push({
					top: top,
					left: Stream.once(0),
					width: context.width,
					height: iMinHeight,
				});

				is.push(i);
				return is;
			}, []);

			totalMinHeightStream(is).pushAll(instance.minHeight);
			Stream.combine(is.map(function (i) {
				return i.minWidth;
			}), function () {
				var args = Array.prototype.slice.call(arguments);
				return args.reduce(mathMax, 0);
			}).pushAll(instance.minWidth);
			
			return [contexts];
		}),
	]);
};

var adjustPosition = function (amount, c) {
	var top = amount.top || 0;
	var left = amount.left || 0;
	var expand = amount.expand;
	return div.all([
		componentName('adjustPosition'),
		child(c),
		wireChildren(function (instance, context, i) {
			i.minWidth.map(function (w) {
				return w + expand ? left : 0;
			}).pushAll(instance.minWidth);
			i.minHeight.map(function (h) {
				return h + expand ? top : 0;
			}).pushAll(instance.minHeight);
			return [{
				top: context.top.map(function (t) {
					return t + top;
				}),
				left: context.left.map(function (l) {
					return l + left;
				}),
				width: context.width,
				height: context.height,
			}];
		}),
	]);
};

var padding = function (amount, c) {
	var top = amount.all || 0,
		bottom = amount.all || 0,
		left = amount.all || 0,
		right = amount.all || 0;
	
	// amount may be a single number
	if ($.isNumeric(amount)) {
		top = bottom = left = right = amount;
	}
	// or an object with properties containing 'top', 'bottom', 'left', and 'right'
	else {
		for (var key in amount) {
			var lcKey = key.toLowerCase();
			if (amount[key] !== null) {
				if (lcKey.indexOf('top') !== -1) {
					top = amount[key];
				}
				if (lcKey.indexOf('bottom') !== -1) {
					bottom = amount[key];
				}
				if (lcKey.indexOf('left') !== -1) {
					left = amount[key];
				}
				if (lcKey.indexOf('right') !== -1) {
					right = amount[key];
				}
			}
		}
	}
	return div.all([
		componentName('padding'),
		child(c),
		wireChildren(function (instance, context, i) {
			i.minWidth.map(function (mw) {
				return mw + left + right;
			}).pushAll(instance.minWidth);
			
			i.minHeight.map(function (mh) {
				return mh + top + bottom;
			}).pushAll(instance.minHeight);
			
			return [{
				top: Stream.once(top, 'padding top'),
				left: Stream.once(left, 'padding left'),
				width: context.width.map(function (w) {
					return w - left - right;
				}),
				height: context.height.map(function (h) {
					return h - top - bottom;
				}),
			}];
		}),
	]);
};

var margin = function (amount) {
	return function (c) {
		return padding(amount, c);
	};
};

var alignLRM = function (lrm) {
	return div.all([
		componentName('alignLRM'),
		child(lrm.middle || nothing),
		child(lrm.left || nothing),
		child(lrm.right || nothing),
		wireChildren(function (instance, context, mI, lI, rI) {
			var headerHeight = Stream.combine([mI, lI, rI].map(function (i) {
				return i.minHeight;
			}), function () {
				var args = Array.prototype.slice.call(arguments);
				var height = args.reduce(function (h, mh) {
					return Math.max(h, mh);
				}, 0);
				return height;
			});
			headerHeight.pushAll(instance.minHeight);
			
			Stream.combine([mI, lI, rI].map(function (i) {
				return i.minWidth;
			}), function () {
				var args = Array.prototype.slice.call(arguments);
				var height = args.reduce(function (w, mw) {
					return w + mw;
				}, 0);
				return height;
			}).pushAll(instance.minWidth);

			var minAvailableRequested = function (available, requested) {
				return Stream.combine([available, requested], mathMin);
			};
			var mWidth = minAvailableRequested(context.width, mI.minWidth);
			var lWidth = minAvailableRequested(context.width, lI.minWidth);
			var rWidth = minAvailableRequested(context.width, rI.minWidth);

			return [{
				top: Stream.once(0),
				left: Stream.combine([context.width, mWidth], function (width, mw) {
					return (width - mw) / 2;
				}),
				width: mWidth,
				height: context.height,
			}, {
				top: Stream.once(0),
				left: Stream.once(0),
				width: lWidth,
				height: context.height,
			}, {
				top: Stream.once(0),
				left: Stream.combine([context.width, rWidth], function (width, rMW) {
					return width - rMW;
				}),
				width: rWidth,
				height: context.height,
			}];
		}),
	]);
};

var alignTBM = function (tbm) {
	tbm.middle = tbm.middle || nothing;
	tbm.bottom = tbm.bottom || nothing;
	tbm.top = tbm.top || nothing;
	
	return div.all([
		componentName('alignTBM'),
		child(tbm.middle),
		child(tbm.bottom),
		child(tbm.top),
		wireChildren(function (instance, context, mI, bI, tI) {
			var minWidth = Stream.combine([mI, bI, tI].map(function (i) {
				return i.minWidth;
			}), function () {
				var args = Array.prototype.slice.call(arguments);
				var width = args.reduce(function (w, mw) {
					return Math.max(w, mw);
				}, 0);
				return width;
			});
			minWidth.pushAll(instance.minWidth);
			
			Stream.combine([mI, bI, tI].map(function (i) {
				return i.minHeight;
			}), function () {
				var args = Array.prototype.slice.call(arguments);
				var height = args.reduce(function (h, mh) {
					return h + mh;
				}, 0);
				return height;
			}).pushAll(instance.minHeight);

			return [{
				top: Stream.combine([context.height, mI.minHeight], function (height, mh) {
					return (height - mh) / 2;
				}),
				left: Stream.once(0),
				width: context.width,
				height: mI.minHeight,
			}, {
				top: Stream.combine([context.height, bI.minHeight], function (height, mh) {
					return height - mh;
				}),
				left: Stream.once(0),
				width: context.width,
				height: bI.minHeight,
			}, {
				top: Stream.once(0),
				left: Stream.once(0),
				width: context.width,
				height: tI.minHeight,
			}];
		}),
	]);
};
var invertOnHover = function (c) {
	var invert = Stream.once(false, 'invert');
	
	var choose = function (stream1, stream2) {
		return Stream.combine([invert, stream1, stream2], function (i, v1, v2) {
			return i ? v2 : v1;
		}, 'choose stream');
	};
	
	
	return div.all([
		componentName('invert-on-hover'),
		child(c.and($css('transition', 'background-color 0.2s linear, color 0.1s linear'))),
		wireChildren(function (instance, context, i) {
			i.minHeight.pushAll(instance.minHeight);
			i.minWidth.pushAll(instance.minWidth);
			return [{
				backgroundColor: choose(context.backgroundColor, context.fontColor),
				fontColor: choose(context.fontColor, context.backgroundColor),
				top: Stream.once(0),
				left: Stream.once(0),
				width: context.width,
				height: context.height,
			}];
		}),
		mouseoverThis(function () {
			invert.push(true);
		}),
		mouseoutThis(function () {
			invert.push(false);
		}),
	]);
};

var border = function (colorS, amount, c) {
	var left = amount.left || amount.all || 0;
	var right = amount.right || amount.all || 0;
	var top = amount.top || amount.all || 0;
	var bottom = amount.bottom || amount.all || 0;
	var radius = amount.radius || 0;

	if (!colorS.onValue) {
		colorS = Stream.once(colorS);
	}

	var colorStringS = colorS.map(colorString);

	return div.all([
		componentName('border'),
		child(div.all([
			componentName('border-child'),
			$css('border-radius', px(radius)),
			child(c),
			wireChildren(passThroughToFirst),
		])),
		function (i) {
			colorStringS.map(function (colorstring) {
				i.$el.css('border-left', px(left) + ' solid ' + colorstring);
				i.$el.css('border-right', px(right) + ' solid ' + colorstring);
				i.$el.css('border-top', px(top) + ' solid ' + colorstring);
				i.$el.css('border-bottom', px(bottom) + ' solid ' + colorstring);
			});
		},
		wireChildren(function (instance, context, i) {
			i.minWidth.map(function (mw) {
				return mw + left + right;
			}).pushAll(instance.minWidth);

			i.minHeight.map(function (mh) {
				return mh + top + bottom;
			}).pushAll(instance.minHeight);

			return [{
				top: Stream.once(0),
				left: Stream.once(0),
				width: context.width.map(function (w) {
					return w - left - right;
				}),
				height: context.height.map(function (h) {
					return h - top - bottom;
				}),
			}];
		}),
	]);
};

var componentStream = function (cStream) {
	var i;
	return div.all([
		componentName('component-stream'),
		function (instance, context) {
			var ctx = instance.newCtx();
			ctx.top.push(0);
			ctx.left.push(0);
			context.width.pushAll(ctx.width);
			context.height.pushAll(ctx.height);

			var localCStream = Stream.create();
			cStream.pushAll(localCStream);
			localCStream.map(function (c) {
				var instanceC = function (c) {
					if (i) {
						i.destroy();
					}
					i = c.create(ctx);
					i.$el.css('transition', 'inherit');
					i.minWidth.pushAll(instance.minWidth);
					i.minHeight.pushAll(instance.minHeight);
				};
				if (c.then) {
					c.then(function (c) {
						instanceC(c);
					}, function (error) {
						console.error('child components failed to load');
						console.log(error);
					});
				}
				else {
					instanceC(c);
				}
			});
			return function () {
				localCStream.end();
				if (i) {
					i.destroy();
				}
			};
		},
	]);
};

var componentStreamWithExit = function (cStream, exit) {
	var i;
	return div.all([
		componentName('component-stream'),
		function (instance, context) {
			var ctx = instance.newCtx();
			ctx.top.push(0);
			ctx.left.push(0);
			context.width.pushAll(ctx.width);
			context.height.pushAll(ctx.height);

			var localCStream = Stream.create();
			cStream.pushAll(localCStream);
			localCStream.map(function (c) {
				var instanceC = function (c) {
					if (i) {
						(function (i) {
							exit(i).then(function () {
								i.destroy();
							});
						})(i);
					}
					i = c.create(ctx);
					i.$el.css('transition', 'inherit');
					i.minWidth.pushAll(instance.minWidth);
					i.minHeight.pushAll(instance.minHeight);
				};
				if (c.then) {
					c.then(function (c) {
						instanceC(c);
					}, function (error) {
						console.error('child components failed to load');
						console.log(error);
					});
				}
				else {
					instanceC(c);
				}
			});
			return function () {
				localCStream.end();
				if (i) {
					i.destroy();
				}
			};
		},
	]);
};

var promiseComponent = function (cP) {
	var stream = Stream.once(nothing);
	cP.then(function (c) {
		stream.push(c);
	}, function (error) {
		console.log(error);
	}).catch(function (err) {
		console.log(err);
	});
	return componentStream(stream);
};

var toggleComponent = function (cs, indexStream) {
	return componentStream(indexStream.map(function (i) {
		return cs[i];
	}));
};

var modalDialog = function (c) {
	return function (stream, transition) {
		var open = Stream.once(false);
		stream.pushAll(open);

		transition = transition || 0;

		return div.all([
			$css('z-index', 100),
			componentName('toggle-height'),
			child(c),
			wireChildren(function (instance, context, i) {
				instance.minWidth.push(0);
				instance.minHeight.push(0);

				var $el = i.$el;
				$el.css('position', 'fixed');
				$el.css('transition', $el.css('transition') + ', opacity ' + transition + 's');
				$el.css('display', 'none');
				$el.css('pointer-events', 'initial');

				open.onValue(function (on) {
					if (on) {
						$el.css('display', '');
						setTimeout(function () {
							$el.css('opacity', 1);
						}, 100);
					}
					else {
						$el.css('opacity', 0);
						setTimeout(function () {
							$el.css('display', 'none');
						}, transition * 1000);
					}
				});

				return [{
					width: windowWidth.map(function () {
						return window.innerWidth;
					}),
					height: windowHeight,
					left: Stream.once(0),
					top: Stream.once(0),
				}];
			}),
		]);
	};
};

var toggleHeight = function (stream) {
	var open = Stream.once(false);
	stream.pushAll(open);
	return function (c) {
		return div.all([
			$css('overflow', 'hidden'),
			componentName('toggle-height'),
			child(c),
			wireChildren(function (instance, context, i) {
				i.minWidth.pushAll(instance.minWidth);
				Stream.combine([i.minHeight, open], function (mh, onOff) {
					return onOff ? mh : 0;
				}).pushAll(instance.minHeight);
				return [{
					top: Stream.once(0),
					left: Stream.once(0),
					width: context.width,
					height: context.height,
				}];
			}),
		]);
	};
};

var dropdownPanel = function (source, panel, onOff, config) {
	config = config || {};
	config.transition = config.transition || "0.5s";
	return div.all([
		componentName('dropdown-panel'),
		child(div.all([
			child(panel),
			wireChildren(function (instance, context, i) {
				i.minWidth.pushAll(instance.minWidth);
				i.minHeight.pushAll(instance.minHeight);
				i.$el.css('transition', 'top ' + config.transition);
				instance.$el.css('pointer-events', 'none');
				i.$el.css('pointer-events', 'initial');
				i.$el.css('z-index', '1000');
				return [{
					width: context.width,
					height: i.minHeight,
					top: Stream.combine([onOff, i.minHeight], function (on, mh) {
						return on ? 0 : -mh;
					}),
					left: Stream.once(0),
				}];
			}),
			$css('overflow', 'hidden'),
		])),
		child(source),
		wireChildren(function (instance, context, iPanel, iSource) {
			Stream.combine([
				iPanel.minWidth,
				iSource.minWidth,
			], Math.max).pushAll(instance.minWidth);
			iSource.minHeight.pushAll(instance.minHeight);
			return [{
				width: context.width,
				height: iPanel.minHeight,
				top: context.height,
				left: Stream.once(0),
			}, {
				width: context.width,
				height: context.height,
				top: Stream.once(0),
				left: Stream.once(0),
			}];
		}),
	]);
};

var fixedHeaderBody = function (config, header, body) {
	config.transition = config.transition || "0.5s";
	return div.all([
		componentName('fixedHeaderBody'),
		child(body),
		child(header),
		wireChildren(function (instance, ctx, bodyI, headerI) {
			headerI.$el.css('position', 'fixed');

			setTimeout(function () {
				headerI.$el.css('transition', 'height ' + config.transition);
				bodyI.$el.css('transition', 'top ' + config.transition);
			});

			Stream.combine([bodyI, headerI].map(function (i) {
				return i.minHeight;
			}), function () {
				var args = Array.prototype.slice.call(arguments);
				return args.reduce(add, 0);
			}).pushAll(instance.minHeight);
			
			Stream.combine([bodyI, headerI].map(function (i) {
				return i.minWidth;
			}), function () {
				var args = Array.prototype.slice.call(arguments);
				return args.reduce(mathMax, 0);
			}).pushAll(instance.minWidth);

			return [{
				top: headerI.minHeight,
				left: Stream.once(0),
				width: ctx.width,
				height: bodyI.minHeight,
			}, {
				top: Stream.once(0),
				left: Stream.once(0),
				width: ctx.width,
				height: headerI.minHeight,
			}];
		}),
	]);
};

var makeSticky = function (c) {
	return div.all([
		componentName('stickyHeaderBody'),
		child(c),
		wireChildren(function (instance, context, i) {
			i.minWidth.pushAll(instance.minWidth);
			i.minHeight.pushAll(instance.minHeight);

			return [{
				top: Stream.once(0),
				left: Stream.combine([
					i.minHeight,
					context.scroll,
					context.top,
					context.left,
					context.leftAccum,
				], function (mh, scroll, top, left, leftAccum) {
					var $el = i.$el;
					if (top > scroll) {
						$el.css('position', 'absolute');
						$el.css('transition', '');
						return 0;
					}
					else if (top < scroll) {
						var leftPosition = left + leftAccum;
						$el.css('position', 'fixed');
						$el.css('left', px(leftPosition));
						setTimeout(function () {
							$el.css('transition', 'inherit');
						}, 20);
						return leftPosition;
					}
				}),
				width: context.width,
				height: context.height,
			}];
		}),
	]);
};

var stickyHeaderBody = function (body1, header, body2) {
	return div.all([
		componentName('stickyHeaderBody'),
		child(body1),
		child(body2),
		child(header),
		wireChildren(function (instance, context, body1I, body2I, headerI) {
			Stream.combine([body1I, body2I, headerI].map(function (i) {
				return i.minHeight;
			}), function () {
				var args = Array.prototype.slice.call(arguments);
				return args.reduce(add, 0);
			}).pushAll(instance.minHeight);
			
			var fixedNow = false;
			
			return [{
				top: Stream.once(0),
				left: Stream.once(0),
				width: context.width,
				height: body1I.minHeight,
			}, {
				top: Stream.combine([body1I.minHeight, headerI.minHeight], add),
				left: Stream.once(0),
				width: context.width,
				height: body2I.minHeight,
			}, {
				top: Stream.combine([body1I.minHeight, context.scroll, context.topAccum], function (mh, scroll, topAccum) {
					var $header = headerI.$el;
					mh = Math.floor(mh);
					if (mh > scroll + topAccum) {
						$header.css('position', 'absolute');
						$header.css('transition', '');
						if (fixedNow) {
							window.scrollTo(0, mh + topAccum);
						}
						fixedNow = false;
						return mh;
					}
					else if (mh < scroll + topAccum) {
						$header.css('position', 'fixed');
						setTimeout(function () {
							$header.css('transition', 'inherit');
						}, 20);
						if (!fixedNow) {
							window.scrollTo(0, mh + topAccum);
						}
						fixedNow = true;
						return topAccum;
					}
				}),
				left: Stream.once(0),
				width: context.width,
				height: headerI.minHeight,
			}];
		}),
	]);
};


var grid = function (config, cs) {
	config.gutterSize = config.gutterSize || 0;
	config.handleSurplusWidth = config.handleSurplusWidth || ignoreSurplusWidth;
	config.handleSurplusHeight = config.handleSurplusHeight || ignoreSurplusHeight;
	config.maxPerRow = config.maxPerRow || 0;

	return padding(config.outerGutter ? config.gutterSize : 0, div.all([
		componentName('grid'),
		children(cs),
		wireChildren(function (instance, context, is) {
			if (is.length === 0) {
				instance.minWidth.push(0);
				instance.minHeight.push(0);
			}
			var minWidths = Stream.combine(is.map(function (i) {
				return i.minWidth;
			}), function () {
				return Array.prototype.slice.call(arguments);
			});
			var minHeights = Stream.combine(is.map(function (i) {
				return i.minHeight;
			}), function () {
				return Array.prototype.slice.call(arguments);
			});

			minWidths.map(function (mws) {
				return mws.reduce(function (a, mw) {
					return config.useFullWidth ? a + mw + config.gutterSize : Math.max(a, mw) + config.gutterSize;
				}, -config.gutterSize);
			}).pushAll(instance.minWidth);

			var contexts = is.map(function (i) {
				return {
					top: Stream.never(),
					left: Stream.never(),
					width: Stream.never(),
					height: Stream.never(),
				};
			});

			var rowsStream = Stream.combine([
				context.width,
				minWidths], function (gridWidth, mws) {
					var blankRow = function () {
						return {
							cells: [],
							contexts: [],
							height: 0,
						};
					};

					var rowsAndCurrentRow = is.reduce(function (a, i, index) {
						var rows = a.rows;
						var currentRow = a.currentRow;
						
						var mw = mws[index];
						var widthUsedThisRow = currentRow.cells.reduce(function (a, b) {
							return a + b + config.gutterSize;
						}, 0);
						var widthNeeded = Math.min(mw, gridWidth);
						
						if ((config.maxPerRow > 0 &&
							currentRow.cells.length === config.maxPerRow) ||
							(widthNeeded > 0 &&
							 widthNeeded + widthUsedThisRow > gridWidth)) {
							rows.push(currentRow);
							currentRow = blankRow();
						}

						currentRow.cells.push(widthNeeded);
						currentRow.contexts.push(contexts[index]);
						
						return {
							rows: rows,
							currentRow: currentRow,
						};
					}, {
						rows: [],
						currentRow: blankRow(),
					});
					var rows = rowsAndCurrentRow.rows;
					rows.push(rowsAndCurrentRow.currentRow);

					rows.map(function (row, i) {
						var widthUsed = 0;
						var positions = row.cells.map(function (widthNeeded) {
							var position = {
								left: widthUsed,
								width: widthNeeded,
							};
							widthUsed += widthNeeded + config.gutterSize;
							return position;
						});
						positions = config.handleSurplusWidth(gridWidth, positions, config, i);
						positions.map(function (position, index) {
							var ctx = row.contexts[index];
							ctx.width.push(position.width);
						});
					});
					
					return rows;
				});

			var rowsWithHeights = Stream.combine([
				minHeights,
				rowsStream,
			], function (mhs, rows) {
				var index = 0;
				rows.map(function (row) {
					row.height = 0;
					row.cells.map(function (cell, i) {
						row.height = Math.max(row.height, mhs[index + i]);
					});
					index += row.cells.length;
				});

				instance.minHeight.push(rows.map(function (r) {
					return r.height;
				}).reduce(function (a, b) { return a + b + config.gutterSize; }, -config.gutterSize));
				return rows;
			});

			
			Stream.all([
				context.width,
				context.height,
				rowsWithHeights], function (gridWidth, gridHeight, rows) {
					if (config.bottomToTop) {
						rows = rows.slice(0).reverse();
					}
					var top = 0;
					rows = config.handleSurplusHeight(gridHeight, rows, config);
					rows.map(function (row, i) {
						var widthUsed = 0;
						var positions = row.cells.map(function (widthNeeded) {
							var position = {
								top: top,
								left: widthUsed,
								width: widthNeeded,
								height: row.height,
							};
							widthUsed += widthNeeded + config.gutterSize;
							return position;
						});
						positions = config.handleSurplusWidth(gridWidth, positions, config, i);
						positions.map(function (position, index) {
							var ctx = row.contexts[index];
							ctx.top.push(position.top);
							ctx.left.push(position.left);
							ctx.width.push(position.width);
							ctx.height.push(position.height);
						});
						top += row.height + config.gutterSize;
					});
				});

			return [contexts];
		}),
	]));
};

var backgroundImagePosition = {
	fit: 'fit',
	fill: 'fill',
};

var withMinWidthStream = function (getMinWidthStream, c) {
	return div.all([
		componentName('with-min-width-stream'),
		child(c),
		wireChildren(function (instance, context, i) {
			getMinWidthStream(i, context).pushAll(instance.minWidth);
			i.minHeight.pushAll(instance.minHeight);
			return [{
				top: Stream.once(0),
				left: Stream.once(0),
				width: context.width,
				height: context.height,
			}];
		}),
	]);
};
var withMinHeightStream = function (getMinHeightStream, c) {
	return div.all([
		componentName('with-min-height-stream'),
		child(c),
		wireChildren(function (instance, context, i) {
			getMinHeightStream(i, context).pushAll(instance.minHeight);
			i.minWidth.pushAll(instance.minWidth);
			return [{
				top: Stream.once(0),
				left: Stream.once(0),
				width: context.width,
				height: context.height,
			}];
		}),
	]);
};

var extendToWindowBottom = function (c, distanceStream) {
	distanceStream = distanceStream || Stream.once(0);
	return withMinHeightStream(function (instance, context) {
		return Stream.combine([instance.minHeight,
							   context.top,
							   context.topAccum,
							   distanceStream,
							   windowResize], function (mh, t, ta, distance) {
								   return Math.max(mh, window.innerHeight - t - ta - distance);
							   });
	}, c);
};

var atMostWindowBottom = function (c, distanceStream) {
	distanceStream = distanceStream || Stream.once(0);
	return withMinHeightStream(function (instance, context) {
		return Stream.combine([instance.minHeight,
							   context.top,
							   context.topAccum,
							   distanceStream,
							   windowResize], function (mh, t, ta, distance) {
								   return Math.min(mh, window.innerHeight - t - ta - distance);
							   });
	}, c);
};

var overlays = function (cs) {
	return div.all([
		children(cs),
		wireChildren(function (instance, context, is) {
			var chooseLargest = function (streams) {
				return Stream.combine(streams, function () {
					var args = Array.prototype.slice.call(arguments);
					return args.reduce(mathMax, 0);
				});
			};

			chooseLargest(is.map(function (i) {
				return i.minHeight;
			})).pushAll(instance.minHeight);
			chooseLargest(is.map(function (i) {
				return i.minWidth;
			})).pushAll(instance.minWidth);
			return [
				is.map(function (i) {
					return {
						top: Stream.once(0),
						left: Stream.once(0),
						width: context.width,
						height: context.height,
					};
				}),
			];
		}),
	]);
};

var withBackground = function (background, c) {
	return div.all([
		componentName('with-background'),
		child(background),
		child(c),
		wireChildren(function (instance, context, bI, cI) {
			cI.minWidth.pushAll(instance.minWidth);
			cI.minHeight.pushAll(instance.minHeight);

			var ctx = {
				top: Stream.once(0),
				left: Stream.once(0),
				width: context.width,
				height: context.height,
			};
			return [
				ctx,
				ctx,
			];
		}),
	]);
};

var withBackgroundImage = function (config, c) {
	var imgAspectRatio = Stream.once(config.aspectRatio || 1);
	if (!config.src.map) {
		config.src = Stream.once(config.src);
	}
	return div.all([
		componentName('with-background-image'),
		$css('overflow', 'hidden'),
		child(img.all([
			$css('visibility', 'hidden'),
			function (i, context) {
				i.minWidth.push(0);
				i.minHeight.push(0);
				config.src.map(function (src) {
					i.$el.prop('src', src);
				});
				
				i.$el.on('load', function () {
					var nativeWidth = findMinWidth(i.$el);
					var nativeHeight = findMinHeight(i.$el);
					var aspectRatio = nativeWidth / nativeHeight;
					imgAspectRatio.push(aspectRatio);
				});
				Stream.all([context.width, context.height], function () {
					i.$el.css('visibility', '');
				});
			},
		])),
		child(c),
		wireChildren(function (instance, context, imgI, cI) {
			cI.minWidth.pushAll(instance.minWidth);
			cI.minHeight.pushAll(instance.minHeight);
			
			var ctx = instance.newCtx();
			context.top.push(0);
			context.left.push(0);
			context.width.pushAll(ctx.width);
			context.height.pushAll(ctx.height);
			
			var imgCtx = instance.newCtx();
			imgCtx.top.push(0);
			imgCtx.left.push(0);
			Stream.all([imgAspectRatio, context.width, context.height], function (aspectRatio, ctxWidth, ctxHeight) {
				var ctxAspectRatio = ctxWidth / ctxHeight;
				if (aspectRatio < ctxAspectRatio) {
					imgCtx.width.push(ctxWidth);
					imgCtx.height.push(ctxWidth / aspectRatio);
				}
				else {
					imgCtx.width.push(ctxHeight * aspectRatio);
					imgCtx.height.push(ctxHeight);
				}
			});
			
			return [
				imgCtx,
				ctx,
			];
		}),
	]);
};

var table = function (config, css) {
	config = config || {};
	var gutterSize = (config.paddingSize || 0) * 2;
	return div.all(css.map(function (cs) {
		return children(cs);
	})).all([
		componentName('table'),
		wireChildren(function () {
			var args = Array.prototype.slice.call(arguments);
			var instance = args[0];
			var context = args[1];
			var iss = args.slice(2);

			// we blindly assume all rows have the same number of columns
			
			// set table min width
			var maxMWs = Stream.combine(iss.reduce(function (a, is) {
				a.push(Stream.combine(is.map(function (i) {
					return i.minWidth;
				}), function () {
					return Array.prototype.slice.call(arguments);
				}));
				return a;
			}, []), function () {
				var rowMWs = Array.prototype.slice.call(arguments);
				return rowMWs.reduce(function (a, rowMWs) {
					return rowMWs.map(function (mw, i) {
						return Math.max(a[i] || 0, mw);
					});
				}, []);
			});
			maxMWs.map(function (maxMWs) {
				var mw = maxMWs.reduce(function (a, mw) {
					return a + mw + gutterSize;
				}, -gutterSize);
				instance.minWidth.push(mw);
			});

			// set table min height
			var rowMinHeights = iss.reduce(function (a, is) {
				a.push(Stream.combine(is.map(function (i) {
					return i.minHeight;
				}), function () {
					var args = Array.prototype.slice.call(arguments);
					return args.reduce(mathMax, 0);
				}));
				return a;
			}, []);
			Stream.combine(rowMinHeights, function () {
				var mhs = Array.prototype.slice.call(arguments);
				var mh = mhs.reduce(function (a, mh) {
					return a + mh + gutterSize;
				}, -gutterSize);
				instance.minHeight.push(mh);
			});

			return rowMinHeights.map(function (mh, i) {
				return iss[i].map(function (_, index) {
					return {
						width: maxMWs.map(function (maxMWs) {
							return maxMWs[index];
						}),
						height: rowMinHeights[i],
						top: Stream.combine(rowMinHeights.slice(0, i).concat([Stream.once(0)]), function () {
							var mhs = Array.prototype.slice.call(arguments);
							return mhs.reduce(function (a, mh) {
								return a + mh + gutterSize;
							}, -gutterSize);
						}),
						left: maxMWs.map(function (maxMWs) {
							return maxMWs.reduce(function (a, mw, mwI) {
								return a + (mwI < index ? mw + gutterSize : 0);
							}, 0);
						}),
					};
				});
			});
		}),
	]);
};

var tabs = function (list, stream) {
	var whichTab = stream || Stream.once(0);
	return stack({}, [
		sideBySide({
			handleSurplusWidth: centerSurplusWidth,
		}, list.map(function (item, index) {
			return alignTBM({
				bottom: toggleComponent([
					item.tab.left,
					item.tab.right,
					item.tab.selected,
				], whichTab.map(function (i) {
					if (index < i) {
						return 0;
					}
					if (index > i) {
						return 1;
					}
					return 2;
				})).all([
					link,
					clickThis(function () {
						whichTab.push(index);
					}),
				]),
			});
		})),
		componentStream(whichTab.map(function (i) {
			return list[i].content;
		})),
	]);
};

var matchStrings = function (stringsAndRouters) {
	return function (str) {
		for (var i = 0; i < stringsAndRouters.length; i++ ) {
			var stringAndRouter = stringsAndRouters[i];
			if (str.indexOf(stringAndRouter.string) === 0) {
				return stringAndRouter.router(str.substring(stringAndRouter.string.length));
			}
		}
	};
};

var routeToComponent = function (component) {
	return function () {
		return component;
	};
};

var routeToComponentF = function (componentF) {
	return function () {
		return componentF();
	};
};

var routeToFirst = function (routers) {
	return function (str) {
		for (var i = 0; i < routers.length; i++) {
			var result = routers[i](str);
			if (result) {
				return result;
			}
		}
	};
};

var routeMatchRest = function (f) {
	return function (str) {
		// wrapping in a promise catches any exceptions that f throws
		return Q(str).then(f);
	};
};

var ignoreHashChange = false;
var route = function (router) {
	var i;
	return div.all([
		child(div.all([
			componentName('route'),
			function (instance, context) {
				windowHash.onValue(function (hash) {
					if (ignoreHashChange) {
						ignoreHashChange = false;
						return;
					}
					if (i) {
						i.destroy();
					}

					Q.all([router(hash)]).then(function (cs) {
						var c = cs[0];
						i = c.create(context);
						i.$el.css('transition', 'inherit');
						i.minWidth.pushAll(instance.minWidth);
						i.minHeight.pushAll(instance.minHeight);
					}, function (error) {
						console.error('child components failed to load');
						console.log(error);
					});
				});
			},
		])),
		wireChildren(passThroughToFirst),
	]);
};
