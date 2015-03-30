require( "../setup.js" );
var riaktive = require( "riaktive" );
var when = require( "when" );
var fs = require( "fs-extra" );
var scraper;
var tmpFolder = __dirname + "/../tmp";
var riak;
var riakConfig;
var config;
var eventBucket;
var actorBucket;
var actorRecords = [
	{ id: "321", name: "actor1" },
	{ id: "432", name: "actor2" },
	{ id: "543", name: "actor3" },
	{ id: "654", name: "actor4" },
	{ id: "765", name: "actor5" }
];

var eventRecords = [
	{ id: "123", actor_id: "321", name: "actor1 event1" },
	{ id: "234", actor_id: "321", name: "actor1 event2" },
	{ id: "345", actor_id: "321", name: "actor1 event3" },
	{ id: "456", actor_id: "321", name: "actor1 event4" },
	{ id: "567", actor_id: "321", name: "actor1 event5" },
	{ id: "678", actor_id: "432", name: "actor2 event1" },
	{ id: "789", actor_id: "432", name: "actor2 event2" },
	{ id: "987", actor_id: "432", name: "actor2 event3" },
	{ id: "876", actor_id: "432", name: "actor2 event4" },
	{ id: "765", actor_id: "432", name: "actor2 event5" }
];

describe( "End to end test", function() {

	before( function( done ) {
		fs.ensureDirSync( tmpFolder );

		process.chdir( tmpFolder );

		riakConfig = {
			host: process.env.RIAK_SERVER || "localhost"
		};

		config = {
			host: riakConfig.host,
			actorBucket: "integrationActorBucket",
			eventBucket: "integrationEventBucket",
			keyFile: "keys.txt",
			dir: process.cwd() + "/export"
		};

		var keysToRetrieve = [ "321", "432", "543" ];

		fs.writeFileSync( tmpFolder + "/keys.txt", keysToRetrieve.join( "\n" ) );

		riak = riaktive.connect( riakConfig );

		actorBucket = riak.bucket( config.actorBucket );
		eventBucket = riak.bucket( config.eventBucket );

		var actorInserts = actorRecords.map( function( r ) {
			if ( r.id % 2 ) {
				actorBucket.put( r.id, r );
			}
			return actorBucket.put( r.id, r );
		} );

		var eventInserts = eventRecords.map( function( r ) {
			if ( r.id % 2 ) {
				// Create some siblings
				eventBucket.put( r.id, r, { "actor_id": r.actor_id } );
			}
			return eventBucket.put( r.id, r, { "actor_id": r.actor_id } );
		} );

		when.all( [
			when.all( actorInserts ),
			when.all( eventInserts )
		] ).then( function() {
			done();
		} );

	} );

	after( function( done ) {
		var actorDeletes = actorRecords.map( function( r ) {
			return actorBucket.del( r.id );
		} );

		var eventDeletes = eventRecords.map( function( r ) {
			return eventBucket.del( r.id );
		} );

		when.all( [
			when.all( actorDeletes ),
			when.all( eventDeletes )
		] ).then( function() {

			fs.removeSync( tmpFolder );

			done();
		} );
	} );

	describe( "when running the export with a full configuration file", function() {
		before( function( done ) {
			scraper = require( "../../src/index.js" );
			scraper.export( config )
				.then( function( res ) {
					done();
				} );
		} );

		it( "should resolve correctly", function() {
			fs.existsSync( config.dir + "/actors.json" ).should.be.ok;
			fs.existsSync( config.dir + "/events.json" ).should.be.ok;
		} );
	} );

} );