var fs = require( "fs-extra" );
var when = require( "when" );
var EOL = require( "os" ).EOL;

function read( fileName ) {
	return when.promise( function( resolve, reject ) {
		fs.readFile( fileName, "utf-8", function( err, results ) {
			if ( err ) {
				return reject( err );
			}

			var keys = results.split( EOL ).map( function( k ) {
				return k.trim();
			} );

			resolve( keys );
		} );
	} );
}

module.exports = {
	read: read
};