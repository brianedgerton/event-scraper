var lift = require( "when/node" ).lift;
var path = require( "path" );
var fs = require( "fs-extra" );
var util = require( "util" );

var ACTOR_FILENAME = "actors.json";
var EVENT_FILENAME = "events.json";

var readJson = lift( fs.readJson );
var writeJson = lift( fs.outputJson );

function saveActors( config, data ) {
	var filename = path.resolve( config.dir, ACTOR_FILENAME );
	util.log( "Saving %s actor records to %s.", data.actors.length, filename );
	return writeJson( filename, data );
}

function saveEvents( config, data ) {
	var filename = path.resolve( config.dir, EVENT_FILENAME );

	var count = data.events.reduce( function( memo, e ) {
		memo += e.events.length;
		return memo;
	}, 0 );

	util.log( "Saving %s event records to %s.", count, filename );

	return writeJson( filename, data );
}

function readActors( dir ) {
	var filename = path.resolve( dir, ACTOR_FILENAME );
	return readJson( filename );
}

function readEvents( dir ) {
	var filename = path.resolve( dir, EVENT_FILENAME );
	return readJson( filename );
}

module.exports = {
	saveActors: saveActors,
	saveEvents: saveEvents,
	readActors: readActors,
	readEvents: readEvents
};