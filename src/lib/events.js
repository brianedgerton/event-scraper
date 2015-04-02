var util = require( "util" );
var _ = require( "lodash" );
var when = require( "when" );
var async = require( "async" );
var document = require( "./document" );
var CONCURRENCY = 20;

function fetch( riak, config, keys ) {
	return when.promise( function( resolve, reject ) {
		var bucketName = config.eventBucket;

		util.log( "Fetching records from %s bucket", bucketName );

		var bucket = riak.bucket( bucketName );

		var count = 0;
		var tasks = keys.map( function( key ) {
			return function( callback ) {
				var records = [];
				bucket.getByIndex( "actor_id", key )
					.progress( function( record ) {
						//records.push( normalize( record ) );
						records.push( record );
						count++;
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

			util.log( "Exported %s records.", count );

			resolve( {
				keys: keys,
				events: _.compact( results )
			} );
		} );

	} );
}

function write( riak, config, fileResults ) {
	return when.promise( function( resolve, reject ) {
		var events = fileResults.events;

		var count = 0;

		var bucketName = config.eventBucket;
		var bucket = riak.bucket( bucketName );
		var tasks = events.reduce( function( memo, actor ) {
			var events = actor.events;
			var subtasks = events.map( function( _docs ) {
				return function( callback ) {
					var docs = document.areSiblings( _docs ) ? _docs : [ _docs ];
					var puts = docs.map( function( d ) {
						var doc = document.prepare( d );
						return bucket.put( doc.data, doc.indexes );
					} );

					count++;

					when.all( puts ).then( function() {
						callback();
					} );
				};
			} );

			if ( subtasks.length ) {
				memo = memo.concat( subtasks );
			}

			return memo;
		}, [] );

		async.parallelLimit( tasks, CONCURRENCY, function( err, results ) {
			if ( err ) {
				return reject( err );
			}

			util.log( "Imported %s records.", count );

			resolve();
		} );

	} );
}

module.exports = {
	fetch: fetch,
	write: write
};