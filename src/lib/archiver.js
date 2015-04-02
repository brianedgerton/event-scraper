var when = require( "when" );
var Targz = require( "tar.gz" );
var path = require( "path" );

function compress( dirPath ) {
	return when.promise( function( resolve, reject ) {
		var targetPath = dirPath + ".tar.gz";

		var tar = new Targz();
		tar.compress( dirPath, targetPath, function( err ) {
			if ( err ) {
				return reject( err );
			}
			resolve();
		} );
	} );
}

function extract( filePath, targetPath ) {
	return when.promise( function( resolve, reject ) {

		var folder = path.basename( filePath ).replace( ".tar.gz", "" );

		var extractedPath = path.resolve( targetPath, folder );

		var tar = new Targz();

		tar.extract( filePath, targetPath, function( err ) {
			if ( err ) {
				return reject( err );
			}
			resolve( extractedPath );
		} );

	} );
}

module.exports = {
	compress: compress,
	extract: extract
};