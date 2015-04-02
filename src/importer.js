var _ = require( "lodash" );
var util = require( "util" );
var when = require( "when" );
var pipeline = require( "when/pipeline" );
var riaktive = require( "riaktive" );
var actors = require( "./lib/actors" );
var events = require( "./lib/events" );
var file = require( "./lib/file" );
var archiver = require( "./lib/archiver" );

function importRecords( riak, config, extractedPath ) {

	var importActors = function() {
		return file.readActors( extractedPath )
			.then( function( results ) {
				return actors.write( riak, config, results );
			} );
	};

	var importEvents = function() {
		return file.readEvents( extractedPath )
			.then( function( results ) {
				return events.write( riak, config, results );
			} );
	};

	return when.all( [
		importActors(),
		importEvents()
	] );

}

function run( config ) {
	var riak = riaktive.connect( {
		host: config.host,
		port: config.port
	} );

	var extractor = _.partial( archiver.extract, config.file, config.dir );
	var importer = _.partial( importRecords, riak, config );

	util.log( "Starting import." );
	util.log( "Config options: " );

	_.forOwn( config, function( val, key ) {
		util.log( " - %s: %s", key, val );
	} );

	return pipeline( [
		extractor,
		importer
	] );

}

module.exports = run;