var lift = require( "when/node" ).lift;
var path = require( "path" );
var fs = require( "fs-extra" );
var util = require( "util" );

var writeFile = lift( fs.writeFile );

function saveToFile( filePath, data ) {
	var contents = JSON.stringify( data, null, 2 );
	return writeFile( filePath, contents );
}

function saveActors( config, data ) {
	var filename = path.resolve( config.dir, "actors.json" );
	util.log( "Saving %s actor records to %s.", data.actors.length, filename );
	return saveToFile( filename, data );
}

function saveEvents( config, data ) {
	var filename = path.resolve( config.dir, "events.json" );

	var count = data.events.reduce( function( memo, e ) {
		memo += e.events.length;
		return memo;
	}, 0 );

	util.log( "Saving %s event records to %s.", count, filename );

	return saveToFile( filename, data );
}

module.exports = {
	saveToFile: saveToFile,
	saveActors: saveActors,
	saveEvents: saveEvents
};