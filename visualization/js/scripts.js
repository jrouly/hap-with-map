
/* get selected data set and graph type*/
$('#btnShowGraph').click(function(){
	var valSelectedDataSet = $('#selDataSet option:selected').val();
	var valSelectedGraphType = $('#selGraphType option:selected').val();
	console.log("Selected data set 1: ", valSelectedDataSet);
	console.log("Selected graph type 1: ", valSelectedGraphType);
});


function colorHsl(index) {
	return "hsl(" + 1*index + ",100%,50%)";
} // end of colorHsl

/* csv2object function generates an object from .csv row
* if a target (exemplar) is not in the list of targets, add it in the array. otherwise, skip it.
* object properties:
* radius: 10 if exemplar, 5 is document
* name: source (document) / target (exemplar) name
* color: color string that corresponds to clusterId value
*/
function csv2object(rows) {
	var i = 0;
	rows.forEach(function(row) {
			// if a target (exemplar) is not member of targets array, update data for source and target, update targets array
			if (targets.indexOf(row.target) == -1 ) {
				// add new source (document) in data
				data[i] = new Object();
				data[i].radius = sourceSize;
				data[i].color = colorHsl(row.clusterId); //color(row.clusterId);
				data[i].name = row.source;
				data[i].APlevel = row.APlevel;
				data[i].filename = row.sourcefilename;
				data[i].clustersize = row.clustersize;
				data[i].topwords = row.sourceTopWords;
				//data[i].targetTopWords = row.targetTopWords;
				// add new target (exemplar) in data
				data[i+1] = new Object();
				data[i+1].radius = targetSize;
				data[i+1].color = colorHsl(row.clusterId); //color(row.clusterId);
				data[i+1].name = row.target;
				data[i+1].APlevel = row.APlevel;
				data[i+1].filename = row.targetfilename;
				data[i+1].clustersize = row.clustersize;
				//data[i+1].sourceTopWords = row.sourceTopWords;
				data[i+1].topwords = row.targetTopWords;
				// keep track of targets (dont add it again)
				targets.push(row.target);
				i = i+2;
			} 
			// otherwise, add new source in data
			else {
				data[i] = new Object();
				data[i].radius = sourceSize;
				data[i].color = colorHsl(row.clusterId); //color(row.clusterId);
				data[i].name = row.source;
				data[i].APlevel = row.APlevel;
				data[i].filename = row.sourcefilename;
				data[i].clustersize = row.clustersize;
				data[i].topwords = row.sourceTopWords;
				i = i+1;
			} // end for if else
	} // end forEach
	) // end forEach
	console.log("Data length after reading the file: " + data.length);
	console.log("Data objects: ");
	console.log(data);
} // end for csv2object function

/* D3.js functions for tick, cluster and collide
 * 
 */
function tick(e) {
	circles
		.each(cluster(10 * e.alpha * e.alpha))
		.each(collide(.5))
		.attr("cx", function(d) { return d.x; })
		.attr("cy", function(d) { return d.y; });
}

// Move d to be adjacent to the cluster node.
function cluster(alpha) {
	var max = {};

	// Find the largest node for each cluster.
	nodes.forEach(function(d) {
		if (!(d.color in max) || (d.radius > max[d.color].radius)) {
			max[d.color] = d;
		}
	});

	return function(d) {
		var node = max[d.color],
			l,
			r,
			x,
			y,
			k = 1,
			i = -1;

		// For cluster nodes, apply custom gravity.
		if (node == d) {
			node = {x: width / 2, y: height / 2, radius: -d.radius};
			k = .1 * Math.sqrt(d.radius);
		}

		x = d.x - node.x;
		y = d.y - node.y;
		l = Math.sqrt(x * x + y * y);
		r = d.radius + node.radius;
		if (l != r) {
			l = (l - r) / l * alpha * k;
			d.x -= x *= l;
			d.y -= y *= l;
			node.x += x;
			node.y += y;
		}
	};
}

// calculate a circle charge based on its radius
function charge(d) {
    return Math.pow(d.radius, 1.1) / 16;
  }

// Resolves collisions between d and all other circles.
function collide(alpha) {
	var quadtree = d3.geom.quadtree(nodes);
	return function(d) {
		var r = d.radius + radius.domain()[1] + padding,
			nx1 = d.x - r,
			nx2 = d.x + r,
			ny1 = d.y - r,
			ny2 = d.y + r;
		quadtree.visit(function(quad, x1, y1, x2, y2) {
			if (quad.point && (quad.point !== d)) {
				var x = d.x - quad.point.x,
						y = d.y - quad.point.y,
						l = Math.sqrt(x * x + y * y),
						r = d.radius + quad.point.radius + (d.color !== quad.point.color) * padding;
			if (l < r) {
				l = (l - r) / l * alpha;
				d.x -= x *= l;
				d.y -= y *= l;
				quad.point.x += x;
				quad.point.y += y;
				}
			}
			return x1 > nx2
				|| x2 < nx1
				|| y1 > ny2
				|| y2 < ny1;
		});
	};
} // end for collide function