require( "../setup.js" );
var _ = require( "lodash" );
var when = require( "when" );
var path = require( "path" );
var riaktive = require( "riaktive" );
var actors = require( "../../src/actors" );
var events = require( "../../src/events" );
var file = require( "../../src/file" );
var document = require( "../../src/document" );
var riak;
var config;
var eventBucket;
var actorBucket;
var actorResults;
var eventResults;
var utils = require( "../utils" );
var fileDir = __dirname + "/../files";



describe( "Riak Writing behavior", function() {

	before( function( done ) {
		config = {
			dir: fileDir,
			actorBucket: "integrationWritingActors",
			eventBucket: "integrationWritingEvents"
		};

		riak = riaktive.connect( {
			host: process.env.RIAK_SERVER || "localhost"
		} );

		actorBucket = riak.bucket( config.actorBucket );
		eventBucket = riak.bucket( config.eventBucket );

		when.all( [
			file.readActors( fileDir ),
			file.readEvents( fileDir )
		] ).then( function( results ) {
			actorResults = results[ 0 ];
			eventResults = results[ 1 ];
			done();
		} );

	} );

	after( function( done ) {
		var actorDeletes = actorResults.keys.map( function( r ) {
			return actorBucket.del( r );
		} );

		var eventKeys = eventResults.events.reduce( function( memo, r ) {
			var events = r.events;

			events.forEach( function( e ) {
				var eventList = _.isArray( e ) ? e : [ e ];

				eventList.forEach( function( event ) {
					memo.push( event.id );
				} );

			} );

			return memo;
		}, [] );

		var eventDeletes = eventKeys.map( function( r ) {
			return eventBucket.del( r );
		} );

		when.all( [
			when.all( actorDeletes ),
			when.all( eventDeletes )
		] ).then( function() {
			done();
		} );
	} );

	describe( "when writing actors", function() {
		var fetchedResults;
		before( function( done ) {
			actors.write( riak, config, actorResults )
				.then( function() {

					actors.fetch( riak, config, actorResults.keys )
						.then( function( results ) {
							fetchedResults = results;
							done();
						} );

				} );
		} );

		it( "should write them correctly", function() {
			utils.normalize( fetchedResults.actors ).should.eql( utils.normalize( actorResults.actors ) );
		} );

	} );

	describe( "when writing events", function() {
		var fetchedResults;
		before( function( done ) {
			events.write( riak, config, eventResults )
				.then( function() {
					events.fetch( riak, config, eventResults.keys )
						.then( function( results ) {
							fetchedResults = results;
							done();
						} );

				} );
		} );

		it( "should write them correctly", function() {
			var normalizedFetched = utils.normalizeEvents( fetchedResults.events );
			var normalizedResults = utils.normalizeEvents( eventResults.events );

			var events1 = normalizedResults[ 0 ].events;
			var fetched1 = normalizedFetched[ 0 ].events;

			events1.length.should.equal( fetched1.length );

			// fetched1.forEach( function( f ) {
			// 	events1.should.contain( f );
			// } );

			// var events2 = normalizedResults[ 1 ].events;
			// var fetched2 = normalizedFetched[ 1 ].events;

			// events2.length.should.equal( fetched2.length );

			// fetched2.forEach( function( f ) {
			// 	events2.should.contain( f );
			// } );

		} );
	} );

} );