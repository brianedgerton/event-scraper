var util = require( "util" );
var _ = require( "lodash" );
var when = require( "when" );
var async = require( "async" );
var CONCURRENCY = 20;
var document = require( "./document" );

function fetch( riak, config, keys ) {
	return when.promise( function( resolve, reject ) {
		var bucketName = config.actorBucket;
		util.log( "Fetching keys from %s bucket", bucketName );
		util.log( "[%s]", keys.join( "," ) );

		var bucket = riak.bucket( bucketName );
		var tasks = keys.map( function( key ) {
			return function( callback ) {
				bucket.get( key )
					.then( function( docs ) {
						//callback( null, normalize( docs ) );
						callback( null, docs );
					}, function( err ) {
						callback( err );
					} );
			};
		} );

		async.parallelLimit( tasks, CONCURRENCY, function( err, results ) {
			if ( err ) {
				return reject( err );
			}
			resolve( {
				keys: keys,
				actors: results
			} );
		} );
	} );
}




function write( riak, config, fileResults ) {
	return when.promise( function( resolve, reject ) {
		var actors = fileResults.actors;

		var bucketName = config.actorBucket;
		var bucket = riak.bucket( bucketName );
		var tasks = actors.map( function( _docs ) {
			return function( callback ) {
				var docs = document.areSiblings( _docs ) ? _docs : [ _docs ];
				var puts = docs.map( function( d ) {
					var doc = document.prepare( d );
					return bucket.put( doc.data, doc.indexes );
				} );

				when.all( puts ).then( function() {
					callback();
				} );
			};
		} );

		async.parallelLimit( tasks, CONCURRENCY, function( err, results ) {
			if ( err ) {
				return reject( err );
			}

			resolve();
		} );

	} );
}

module.exports = {
	fetch: fetch,
	write: write
};