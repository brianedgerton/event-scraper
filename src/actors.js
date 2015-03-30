var util = require( "util" );
var path = require( "path" );
var _ = require( "lodash" );
var when = require( "when" );
var async = require( "async" );
var CONCURRENCY = 20;

function normalize( _docs ) {
	var docs = _.isArray( _docs ) ? _docs : [ _docs ];

	return docs.map( function( d ) {
		delete d.vclock;
		return d;
	} );
}

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
						callback( null, normalize( docs ) );
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

module.exports = {
	fetch: fetch
};