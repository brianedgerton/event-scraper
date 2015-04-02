/*
	1. Read keys from source
	2. Retrieve actor records by key
		-> Save to compressed file in actors folder
	3. Retrieve event records by actorId
		-> Save to compressed file in events folder
*/
var _ = require( "lodash" );
var when = require( "when" );
var util = require( "util" );
var pipeline = require( "when/pipeline" );
var riaktive = require( "riaktive" );
var preflight = require( "./lib/preflight" );
var keys = require( "./lib/keys" );
var actors = require( "./lib/actors" );
var events = require( "./lib/events" );
var file = require( "./lib/file" );
var archiver = require( "./lib/archiver" );

function exportRecords( riak, config, keys ) {

	var processActors = function( keys ) {
		var save = _.partial( file.saveActors, config );
		return pipeline( [
			actors.fetch,
			save
		], riak, config, keys );
	};

	var processEvents = function( keys ) {
		var save = _.partial( file.saveEvents, config );
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

	util.log( "Starting export." );
	util.log( "Config options: " );

	_.forOwn( config, function( val, key ) {
		util.log( " - %s: %s", key, val );
	} );

	return pipeline( [
		checker,
		keyReader,
		exporter,
		compressor
	] );
}

module.exports = runExport;