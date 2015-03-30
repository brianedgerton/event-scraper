var fs = require( "fs-extra" );
var lift = require( "when/node" ).lift;

var ensureDir = lift( fs.ensureDir );

function check( config ) {

	return ensureDir( config.dir );
		
}


module.exports = {
	check: check
};