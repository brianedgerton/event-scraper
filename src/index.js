/*
	1. Read keys from source
	2. Retrieve actor records by key
		-> Save to compressed file in actors folder
	3. Retrieve event records by actorId
		-> Save to compressed file in events folder
*/
var _ = require( "lodash" );
var when = require( "when" );
var pipeline = require( "when/pipeline" );
var riaktive = require( "riaktive" );
var preflight = require( "./preflight" );
var keys = require( "./keys" );
var actors = require( "./actors" );
var events = require( "./events" );
var exportLib = require( "./exporter" );
var archiver = require( "./archiver" );

function exportRecords( riak, config, keys ) {

	var processActors = function( keys ) {
		var save = _.partial( exportLib.saveActors, config );
		return pipeline( [
			actors.fetch,
			save
		], riak, config, keys );
	};

	var processEvents = function( keys ) {
		var save = _.partial( exportLib.saveEvents, config );
		return pipeline( [
			events.fetch,
			save
		], riak, config, keys );
	};

	return when.all( [
		processActors( keys ),
		processEvents( keys )
	] );
}


function runExport( config ) {

	var riak = riaktive.connect( {
		host: config.host,
		port: config.port
	} );

	var checker = _.partial( preflight.check, config );
	var keyReader = _.partial( keys.read, config.keyFile );
	var exporter = _.partial( exportRecords, riak, config );
	var compressor = _.partial( archiver.compress, config.dir );

	return pipeline( [
		checker,
		keyReader,
		exporter,
		compressor
	] );
}

module.exports = {
	"export": runExport
};