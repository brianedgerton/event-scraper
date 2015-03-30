var util = require( "util" );
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
		var bucketName = config.eventBucket;

		util.log( "Fetching records from %s bucket", bucketName );

		var bucket = riak.bucket( bucketName );

		var tasks = keys.map( function( key ) {
			return function( callback ) {
				var records = [];
				bucket.getByIndex( "actor_id", key )
					.progress( function( record ) {
						records.push( normalize( record ) );
					} )
					.then( function() {
						if ( records.length ) {
							return callback( null, { actor_id: key, events: records } );
						}
						callback( null, null );
					} );
			};
		} );

		async.parallelLimit( tasks, CONCURRENCY, function( err, results ) {
			if ( err ) {
				return reject( err );
			}
			resolve( {
				keys: keys,
				events: _.compact( results )
			} );
		} );

	} );
}

module.exports = {
	fetch: fetch
};