var _ = require( "lodash" );
var when = require( "when" );
var pipeline = require( "when/pipeline" );
var riaktive = require( "riaktive" );
var actors = require( "./actors" );
var events = require( "./events" );
var file = require( "./file" );
var archiver = require( "./archiver" );

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

	return pipeline( [
		extractor,
		importer
	] );

}

module.exports = run;