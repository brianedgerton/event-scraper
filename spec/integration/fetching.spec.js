require( "../setup.js" );
var when = require( "when" );
var riaktive = require( "riaktive" );
var actors = require( "../../src/actors" );
var events = require( "../../src/events" );
var riak;
var config;
var eventBucket;
var actorBucket;
var actorRecords = [
	{ id: 1, name: "actor1" },
	{ id: 2, name: "actor2" },
	{ id: 3, name: "actor3" },
	{ id: 4, name: "actor4" },
	{ id: 5, name: "actor5" }
];

var eventRecords = [
	{ id: 1, actor_id: 1, name: "actor1 event1" },
	{ id: 2, actor_id: 1, name: "actor1 event2" },
	{ id: 3, actor_id: 1, name: "actor1 event3" },
	{ id: 4, actor_id: 1, name: "actor1 event4" },
	{ id: 5, actor_id: 1, name: "actor1 event5" },
	{ id: 6, actor_id: 2, name: "actor2 event1" },
	{ id: 7, actor_id: 2, name: "actor2 event2" },
	{ id: 8, actor_id: 2, name: "actor2 event3" },
	{ id: 9, actor_id: 2, name: "actor2 event4" },
	{ id: 10, actor_id: 2, name: "actor2 event5" }
];

describe( "Riak fetching behavior", function() {

	before( function( done ) {
		config = {
			actorBucket: "integrationActorBucket",
			eventBucket: "integrationEventBucket"
		};

		var riakConfig = {
			host: process.env.RIAK_SERVER || "localhost"
		};

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
			done();
		} );
	} );

	describe( "when fetching actors", function() {
		var results;

		before( function( done ) {
			actors.fetch( riak, config, [ 1, 2, 3 ] )
				.then( function( r ) {
					results = r;
					done();
				} );
		} );

		it( "should retrieve the correct documents", function() {
			results.should.eql( {
				"keys": [
					1,
					2,
					3
				],
				"actors": [
					[
						{
							"id": 1,
							"name": "actor1"
						},
						{
							"id": 1,
							"name": "actor1"
						}
					],
					[
						{
							"id": 2,
							"name": "actor2"
						}
					],
					[
						{
							"id": 3,
							"name": "actor3"
						},
						{
							"id": 3,
							"name": "actor3"
						}
					]
				]
			} );
		} );
	} );

	describe( "when fetching events", function() {
		var results;

		before( function( done ) {
			events.fetch( riak, config, [ 1, 2, 3 ] )
				.then( function( r ) {
					results = r;
					done();
				} );
		} );

		it( "should fetch the correct records", function() {
			results.should.eql( {
				"keys": [
					1,
					2,
					3
				],
				"events": [
					{
						"actor_id": 1,
						"events": [
							[
								{
									"id": 4,
									"actor_id": 1,
									"name": "actor1 event4",
									"_indexes": {
										"actor_id": 1
									}
								}
							],
							[
								{
									"id": 2,
									"actor_id": 1,
									"name": "actor1 event2",
									"_indexes": {
										"actor_id": 1
									}
								}
							],
							[
								{
									"id": 3,
									"actor_id": 1,
									"name": "actor1 event3",
									"_indexes": {
										"actor_id": 1
									}
								},
								{
									"id": 3,
									"actor_id": 1,
									"name": "actor1 event3",
									"_indexes": {
										"actor_id": 1
									}
								}
							],
							[
								{
									"id": 5,
									"actor_id": 1,
									"name": "actor1 event5",
									"_indexes": {
										"actor_id": 1
									}
								},
								{
									"id": 5,
									"actor_id": 1,
									"name": "actor1 event5",
									"_indexes": {
										"actor_id": 1
									}
								}
							],
							[
								{
									"id": 1,
									"actor_id": 1,
									"name": "actor1 event1",
									"_indexes": {
										"actor_id": 1
									}
								},
								{
									"id": 1,
									"actor_id": 1,
									"name": "actor1 event1",
									"_indexes": {
										"actor_id": 1
									}
								}
							]
						]
					},
					{
						"actor_id": 2,
						"events": [
							[
								{
									"id": 7,
									"actor_id": 2,
									"name": "actor2 event2",
									"_indexes": {
										"actor_id": 2
									}
								},
								{
									"id": 7,
									"actor_id": 2,
									"name": "actor2 event2",
									"_indexes": {
										"actor_id": 2
									}
								}
							],
							[
								{
									"id": 8,
									"actor_id": 2,
									"name": "actor2 event3",
									"_indexes": {
										"actor_id": 2
									}
								}
							],
							[
								{
									"id": 10,
									"actor_id": 2,
									"name": "actor2 event5",
									"_indexes": {
										"actor_id": 2
									}
								}
							],
							[
								{
									"id": 6,
									"actor_id": 2,
									"name": "actor2 event1",
									"_indexes": {
										"actor_id": 2
									}
								}
							],
							[
								{
									"id": 9,
									"actor_id": 2,
									"name": "actor2 event4",
									"_indexes": {
										"actor_id": 2
									}
								},
								{
									"id": 9,
									"actor_id": 2,
									"name": "actor2 event4",
									"_indexes": {
										"actor_id": 2
									}
								}
							]
						]
					}
				]
			} );
		} );
	} );
} );