require( "../setup.js" );
var fs = require( "fs-extra" );
var _ = require( "lodash" );
var when = require( "when" );
var path = require( "path" );
var riaktive = require( "riaktive" );
var actors = require( "../../src/lib/actors" );
var events = require( "../../src/lib/events" );
var file = require( "../../src/lib/file" );
var scraper = require( "../../src/index" );
var document = require( "../../src/lib/document" );
var utils = require( "../utils" );
var riak;

var fileDir = __dirname + "/../files";
var tmpDir = __dirname + "/../tmp/full";

var riakConfig;
var exportConfig;
var importConfig;

var eventBucketExport;
var actorBucketExport;

var eventBucketImport;
var actorBucketImport;

var actorRecords = [
	{ id: "321", name: "actor1" },
	{ id: "432", name: "actor2" },
	{ id: "543", name: "actor3" }
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

function createKeyFile() {
	var keysToRetrieve = [ "321", "432", "543" ];
	fs.writeFileSync( tmpDir + "/keys.txt", keysToRetrieve.join( "\n" ) );
}

function setupRecords() {
	var actorInserts = actorRecords.map( function( r ) {
		if ( r.id % 2 ) {
			actorBucketExport.put( r.id, r );
		}
		return actorBucketExport.put( r.id, r );
	} );

	var eventInserts = eventRecords.map( function( r ) {
		if ( r.id % 2 ) {
			// Create some siblings
			eventBucketExport.put( r.id, r, { "actor_id": r.actor_id } );
		}
		return eventBucketExport.put( r.id, r, { "actor_id": r.actor_id } );
	} );

	return when.all( [
		when.all( actorInserts ),
		when.all( eventInserts )
	] );
}

function clearRecords() {
	var actorExportDeletes = actorRecords.map( function( r ) {
		return actorBucketExport.del( r.id );
	} );

	var eventExportDeletes = eventRecords.map( function( r ) {
		return eventBucketExport.del( r.id );
	} );

	var actorImportDeletes = actorRecords.map( function( r ) {
		return actorBucketImport.del( r.id );
	} );

	var eventImportDeletes = eventRecords.map( function( r ) {
		return eventBucketImport.del( r.id );
	} );

	return when.all( [
		when.all( actorExportDeletes ),
		when.all( eventExportDeletes ),
		when.all( actorImportDeletes ),
		when.all( eventImportDeletes )
	] );
}

describe( "End to end", function() {
	before( function( done ) {
		fs.ensureDirSync( tmpDir );
		process.chdir( tmpDir );

		riakConfig = {
			host: process.env.RIAK_SERVER || "localhost"
		};

		exportConfig = {
			host: riakConfig.host,
			actorBucket: "integrationActorBucketExport",
			eventBucket: "integrationEventBucketExport",
			keyFile: "keys.txt",
			dir: process.cwd() + "/archived/export"
		};

		importConfig = {
			host: riakConfig.host,
			actorBucket: "integrationActorBucketImport",
			eventBucket: "integrationEventBucketImport",
			file: "archived/export.tar.gz",
			dir: process.cwd()
		};

		createKeyFile();

		riak = riaktive.connect( riakConfig );

		actorBucketExport = riak.bucket( exportConfig.actorBucket );
		eventBucketExport = riak.bucket( exportConfig.eventBucket );

		actorBucketImport = riak.bucket( importConfig.actorBucket );
		eventBucketImport = riak.bucket( importConfig.eventBucket );

		clearRecords()
			.then( function() {
				setupRecords()
					.then( function() {
						done();
					} );
			} );

	} );

	after( function( done ) {
		clearRecords()
			.then( function() {
				done();
			} );
	} );

	describe( "when exporting records and then re-importing", function() {

		before( function( done ) {
			scraper.export( exportConfig )
				.then( function() {

					scraper.import( importConfig )
						.then( function() {
							done();
						} );

				} );
		} );

		it( "should put the same actors into the import bucket", function( done ) {
			var actorIds = _.pluck( actorRecords, "id" );

			var exportedActors = actorIds.map( function( id ) {
				return actorBucketExport.get( id );
			} );

			var importedActors = actorIds.map( function( id ) {
				return actorBucketImport.get( id );
			} );


			when.all( [
				when.all( exportedActors ),
				when.all( importedActors )
			] ).then( function( results ) {
				var exported = results[ 0 ];
				var imported = results[ 1 ];

				exported.length.should.equal( imported.length );

				for (var i = 0; i < exported.length; i++) {
					utils.normalize( [ imported[ i ] ] ).should.eql( utils.normalize( [ exported[ i ] ] ) );
				}

				done();
			} );
		} );

		it( "should put the same events into the import bucket", function( done ) {
			var eventIds = _.pluck( eventRecords, "id" );

			var exportedEvents = eventIds.map( function( id ) {
				return eventBucketExport.get( id );
			} );

			var importedEvents = eventIds.map( function( id ) {
				return eventBucketImport.get( id );
			} );


			when.all( [
				when.all( exportedEvents ),
				when.all( importedEvents )
			] ).then( function( results ) {
				var exported = results[ 0 ];
				var imported = results[ 1 ];

				exported.length.should.equal( imported.length );

				for (var i = 0; i < exported.length; i++) {
					var n1 = utils.normalize( [ exported[ i ] ] );
					var n2 = utils.normalize( [ imported[ i ] ] );

					n1.should.eql( n2 );
				}

				done();
			} );
		} );

	} );

} );