var when = require( "when" );
var Targz = require( "tar.gz" );

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

function extract() {

}

module.exports = {
	compress: compress,
	extract: extract
};